"""Small USD-M REST transport for the hourly execution state machine.

No I/O occurs at construction. The caller persists client IDs before writes and
reconciles uncertain writes by querying those IDs; this module never resubmits
an order or cancellation. Raw exchange values (especially fills and fees) are
returned without wallet adjustments or guessed timestamps.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import socket
import ssl
import threading
import time
from contextlib import contextmanager
from decimal import Decimal
from http.client import HTTPException, IncompleteRead, RemoteDisconnected
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen, getproxies, proxy_bypass
from .request_budget import RequestBudget, RateLimited, cooldown_deadline
from .network_retry import NETWORK_RETRY_SECONDS, NetworkBackoff, NetworkRetryGate


def _pooled_failure(exc, errors):
    """Translate third-party network exceptions without signed URLs/credentials."""
    queue, seen = [exc], set()
    while queue and len(seen) < 12:
        current = queue.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (ssl.SSLError, socket.gaierror, TimeoutError, OSError, HTTPException)):
            if isinstance(current, ssl.SSLCertVerificationError):
                return ssl.SSLCertVerificationError('TLS证书验证失败')
            if isinstance(current, ssl.SSLEOFError):
                return ssl.SSLEOFError('TLS连接提前断开')
            if isinstance(current, ssl.SSLError):
                return ssl.SSLError('TLS连接失败')
            if isinstance(current, socket.gaierror):
                return socket.gaierror('DNS域名解析失败')
            if isinstance(current, (TimeoutError, socket.timeout)):
                return TimeoutError('连接或读取超时')
            if isinstance(current, IncompleteRead):
                return IncompleteRead(b'', current.expected)
            if isinstance(current, RemoteDisconnected):
                return RemoteDisconnected('远端关闭连接')
            if isinstance(current, HTTPException):
                return HTTPException('HTTP连接传输中断')
            marker = str(current).lower()
            if 'tunnel connection failed' in marker:
                return OSError('Tunnel connection failed 407' if '407' in marker else 'Tunnel connection failed')
            # Preserve only numeric OS evidence, never its arbitrary message.
            return OSError(getattr(current, 'errno', None) or 0, '网络连接失败')
        for attr in ('reason', 'original_error', '__cause__', '__context__'):
            child = getattr(current, attr, None)
            if isinstance(child, BaseException):
                queue.append(child)
        queue.extend(arg for arg in getattr(current, 'args', ()) if isinstance(arg, BaseException))
    if isinstance(exc, (errors.ReadTimeoutError, errors.ConnectTimeoutError)):
        return TimeoutError('连接或读取超时')
    if isinstance(exc, errors.NameResolutionError):
        return socket.gaierror('DNS域名解析失败')
    if isinstance(exc, errors.ProxyError):
        return OSError('Tunnel connection failed')
    if isinstance(exc, errors.SSLError):
        return ssl.SSLError('TLS连接失败')
    return HTTPException('HTTP连接传输中断')


class _PooledResponse:
    def __init__(self, owner, response, manager, deadline):
        self.owner, self.response, self.manager = owner, response, manager
        self.deadline = deadline
        self.status, self.headers = response.status, response.headers
        self.complete, self.closed = False, False
        connection = getattr(response, 'connection', None)
        socket_object = getattr(connection, 'sock', None)
        self.socket_object = socket_object
        # urllib3 captures this bound shutdown before http.client detaches a
        # Connection: close socket. The response body still owns that socket.
        self.socket_shutdown = (getattr(socket_object, 'shutdown', None)
                                or getattr(response, '_sock_shutdown', None))
        if socket_object is not None:
            owner._stats['socket_observations'] += 1
            if socket_object is owner._last_socket:
                owner._stats['socket_reuse_responses'] += 1
            owner._last_socket = socket_object
        if str(self.headers.get('Connection', '')).lower() == 'close':
            owner._stats['peer_close_responses'] += 1

    def read(self):
        started = self.owner.clock()
        expired = threading.Event()
        remaining = self.deadline - started
        timer = None
        def abort_body():
            expired.set()
            # Capture this response's socket, never a later pooled connection.
            # shutdown wakes buffered read before close can acquire its I/O lock.
            try:
                self.socket_shutdown(socket.SHUT_RDWR)
            except (OSError, AttributeError):
                pass
        try:
            if remaining <= 0:
                expired.set()
                raise TimeoutError('响应整体预算已用尽')
            if callable(self.socket_shutdown):
                timer = threading.Timer(remaining, abort_body)
                timer.daemon = True
                timer.start()
            data = self.response.read()
            if expired.is_set() or self.owner.clock() >= self.deadline:
                expired.set()
                raise TimeoutError('响应整体预算已用尽')
            self.complete = True
            self.owner._stats['responses_consumed'] += 1
            return data
        except Exception as exc:
            if not isinstance(exc, self.owner._network_errors) and not expired.is_set():
                raise
            self.owner._stats['last_failure_phase'] = 'body'
            if expired.is_set():
                exc = TimeoutError('响应整体预算已用尽')
            raise URLError(_pooled_failure(exc, self.owner._errors)) from None
        finally:
            if timer is not None:
                timer.cancel()
                timer.join()
            if expired.is_set():
                self.owner._stats['body_deadline_timeouts'] += 1
            self.owner._stats['last_body_ms'] = round((self.owner.clock() - started) * 1000)

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.complete:
                self.response.release_conn()
            else:
                try:
                    self.response.close()
                finally:
                    self.owner._discard(self.manager)
        except self.owner._network_errors as exc:
            if self.manager in self.owner._managers.values():
                self.owner._discard(self.manager)
            raise URLError(_pooled_failure(exc, self.owner._errors)) from None
        finally:
            self.owner._last_used = self.owner.clock()
            self.owner._lock.release()


def _local_admission_callback(callback, stage):
    """A local guard/registration failure never represents an HTTP submission."""
    try:
        result = callback()
        if result is False:
            raise EntryAdmissionError(stage)
        return result
    except OrderUncertain as exc:
        # Admission callbacks cannot perform exchange requests. Do not let an
        # arbitrary local callback relabel a request not yet sent as uncertain.
        raise EntryAdmissionError(stage, type(exc).__name__) from exc
    except ApiError as exc:
        exc.definite_not_submitted = True
        exc.definite_rejected = False
        raise
    except Exception as exc:
        raise EntryAdmissionError(stage, type(exc).__name__) from exc


class PooledHTTPTransport:
    """One client owns bounded keep-alive pools; no implicit request replay.

    Proxy discovery is unchanged urllib environment/Windows registry discovery.
    TLS uses the original system context and socket DNS resolution. Pool creation
    is lazy; importing or constructing a client performs no network request.
    """
    def __init__(self, *, pool_factory=None, proxy_factory=None, proxy_getter=getproxies,
                 bypass=proxy_bypass, context_factory=ssl.create_default_context,
                 clock=time.monotonic, max_idle_seconds=60):
        self.pool_factory, self.proxy_factory = pool_factory, proxy_factory
        self.proxy_getter, self.bypass, self.context_factory = proxy_getter, bypass, context_factory
        self.clock, self.max_idle_seconds = clock, max_idle_seconds
        self._lock, self._managers = threading.RLock(), {}
        self._proxies, self._context, self._last_used = None, None, None
        self._last_socket = None
        self._errors, self._network_errors, self._closed = None, (), False
        self._stats = {'version': 'bounded-keep-alive-v2', 'wire_attempts': 0,
                       'responses_consumed': 0, 'pool_reuse_requests': 0,
                       'pool_creations': 0, 'pool_discards': 0,
                       'socket_observations': 0, 'socket_reuse_responses': 0,
                       'peer_close_responses': 0, 'discard_errors': 0,
                       'slot_deferrals': 0, 'body_deadline_timeouts': 0,
                       'last_queue_ms': 0, 'last_headers_ms': 0, 'last_body_ms': 0,
                       'last_failure_phase': None}

    def _initialize(self):
        if self._errors is not None:
            return
        import urllib3
        self._errors = urllib3.exceptions
        errors = self._errors
        self._network_errors = (errors.ProtocolError, errors.ReadTimeoutError, errors.ConnectTimeoutError,
            errors.NewConnectionError, errors.NameResolutionError, errors.ProxyError, errors.SSLError,
            errors.MaxRetryError, errors.IncompleteRead, errors.DecodeError, errors.ClosedPoolError,
            OSError, HTTPException)
        self.pool_factory = self.pool_factory or urllib3.PoolManager
        self.proxy_factory = self.proxy_factory or urllib3.ProxyManager
        self._proxies = dict(self.proxy_getter() or {})
        self._context = self.context_factory()

    def _discard(self, manager):
        for key, stored in list(self._managers.items()):
            if stored is manager:
                del self._managers[key]
        self._stats['pool_discards'] += 1
        self._last_socket = None
        try:
            manager.clear()
        except Exception:
            # Discarded ownership must not be resurrected by a cleanup error.
            self._stats['discard_errors'] += 1

    def __call__(self, request, *, timeout, prepare_request=None, on_wire=None, refresh_request=None,
                 before_send=None):
        started = self.clock()
        deadline = started + float(timeout)
        if not self._lock.acquire(timeout=max(0, float(timeout))):
            self._stats['slot_deferrals'] += 1
            raise LocalTransportDeferred()
        transferred = False
        wire_entered = False
        try:
            self._stats['last_queue_ms'] = round((self.clock() - started) * 1000)
            if self._closed:
                raise RuntimeError('HTTP连接池已关闭')
            self._initialize()
            parsed = urlsplit(request.full_url)
            if parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username or parsed.password:
                raise ValueError('HTTP请求地址无效')
            if self._last_used is not None and self.clock() - self._last_used > self.max_idle_seconds:
                for manager in list(self._managers.values()):
                    self._discard(manager)
            proxy = None if self.bypass(parsed.netloc) else self._proxies.get(parsed.scheme)
            key = (parsed.scheme, proxy)
            manager = self._managers.get(key)
            if manager is None:
                options = dict(num_pools=2, maxsize=2, block=True, retries=False,
                               ssl_context=self._context)
                manager = (self.proxy_factory(proxy, proxy_ssl_context=self._context, **options)
                           if proxy else self.pool_factory(**options))
                self._managers[key] = manager
                self._stats['pool_creations'] += 1
            else:
                self._stats['pool_reuse_requests'] += 1
            remaining = deadline - self.clock()
            if remaining <= 0:
                self._stats['slot_deferrals'] += 1
                raise LocalTransportDeferred()
            if callable(prepare_request):
                request = prepare_request(request)
            remaining = deadline - self.clock()
            if remaining <= 0:
                self._stats['slot_deferrals'] += 1
                raise LocalTransportDeferred()
            import urllib3
            if callable(before_send):
                _local_admission_callback(before_send, 'registration')
            remaining = deadline - self.clock()
            if remaining <= 0:
                self._stats['slot_deferrals'] += 1
                raise LocalTransportDeferred()
            if callable(on_wire):
                try:
                    on_wire()
                except Exception:
                    pass  # Diagnostic failure cannot change submission semantics.
            if callable(refresh_request):
                request = refresh_request(request)
            remaining = deadline - self.clock()
            if remaining <= 0:
                self._stats['slot_deferrals'] += 1
                raise LocalTransportDeferred()
            self._stats['wire_attempts'] += 1
            headers_started = self.clock()
            wire_entered = True
            response = manager.request(request.get_method(), request.full_url,
                body=request.data, headers=dict(request.header_items()),
                timeout=urllib3.Timeout(total=remaining, connect=remaining, read=remaining),
                pool_timeout=remaining, retries=False, redirect=False, preload_content=False)
            self._stats['last_headers_ms'] = round((self.clock() - headers_started) * 1000)
            result = _PooledResponse(self, response, manager, deadline)
            transferred = True
            return result
        except self._network_errors as exc:
            self._stats['last_failure_phase'] = 'headers_or_connect'
            if 'manager' in locals():
                self._discard(manager)
            error = URLError(_pooled_failure(exc, self._errors))
            error.definite_not_submitted = not wire_entered
            raise error from None
        finally:
            if not transferred:
                self._lock.release()

    def snapshot(self):
        with self._lock:
            return dict(self._stats, closed=self._closed, keep_alive=True,
                        automatic_retries=False, active_pools=len(self._managers))

    def close(self):
        with self._lock:
            for manager in list(self._managers.values()):
                self._discard(manager)
            self._closed = True


class ApiError(RuntimeError):
    def __init__(self, code: int | None, message: str, *, status: int | None = None,
                 method: str | None = None, path: str | None = None,
                 recovery_kind: str | None = None, retry_at_ms: int | None = None,
                 retry_in_seconds: float = 0, definite_not_submitted: bool = False,
                 definite_rejected: bool = False):
        self.code, self.message, self.status = code, message, status
        self.method, self.path = method, path
        self.recovery_kind = recovery_kind or (
            'timestamp_rejected' if code == -1021 else
            'exchange_cooldown' if code == -1003 or status in {418, 429} else
            'credentials_error' if code in {-2014, -2015, -1022} else
            'exchange_rejected' if code is not None or status is not None else 'permanent')
        self.retry_at_ms = retry_at_ms
        self.retry_in_seconds = max(0, float(retry_in_seconds))
        # This means no wire submission occurred, not merely that the exchange
        # returned a rejection. definite_rejected records the latter separately.
        self.definite_not_submitted = bool(definite_not_submitted)
        self.definite_rejected = bool(definite_rejected)
        super().__init__(f"Binance {code if code is not None else 'transport'}: {message}")


class LocalTransportDeferred(ApiError):
    """No wire submission: local pool ownership did not become available in time."""
    def __init__(self):
        super().__init__(None, '本地连接正在完成前一请求，稍后核对',
                         recovery_kind='local_budget_wait', retry_in_seconds=2,
                         definite_not_submitted=True)


class EntryAdmissionError(ApiError):
    """Local entry registration/guard rejected before its HTTP write boundary."""
    def __init__(self, stage, error_type=None):
        self.admission_stage, self.local_error_type = stage, error_type
        super().__init__(None, '入场委托发送前登记或控制检查未通过，未提交',
                         recovery_kind='entry_preflight_rejected', definite_not_submitted=True)


class OrderUncertain(ApiError):
    """The write may have reached Binance. Query persisted IDs; do not resend."""
    def __init__(self, code: int | None, message: str, *, client_id: str | None = None,
                 **kwargs: Any):
        self.client_id = client_id
        kwargs['recovery_kind'] = 'order_uncertain'
        kwargs['definite_not_submitted'] = False
        kwargs['definite_rejected'] = False
        super().__init__(code, message, **kwargs)


class AccountModeError(ApiError):
    """Account mode must be changed by the human, never by this transport."""


class RequestExpired(ApiError):
    """Local deadline passed before submission; no exchange write was sent."""

    def __init__(self, code, message, **kwargs):
        kwargs.update(recovery_kind='signal_expired', definite_not_submitted=True)
        super().__init__(code, message, **kwargs)


class ReadBudgetDeferred(ApiError):
    """A local request reservation was deferred before any wire submission."""

    def __init__(self, pending, *, method, path):
        super().__init__(-1003, str(pending), method=method, path=path,
                         recovery_kind=pending.recovery_kind,
                         retry_at_ms=pending.retry_at_ms,
                         retry_in_seconds=pending.retry_in_seconds,
                         definite_not_submitted=True)
        # Internal compatibility code is not an exchange rejection. A local
        # reservation wait must never look like a fresh Binance IP-limit error.
        self.args = (str(pending),)


class NetworkRetryPending(ApiError):
    """Local recovery gate rejection, not a new wire failure."""
    network_backoff = True

    def __init__(self, pending, *, method, path):
        super().__init__(None, pending.message, method=method, path=path,
                         recovery_kind='network_read_wait',
                         retry_at_ms=int(pending.retry_at * 1000),
                         retry_in_seconds=pending.wait_seconds,
                         definite_not_submitted=True)


class ManualConnectionCheckExpired(ApiError):
    def __init__(self):
        super().__init__(None, '本次连接检查已超过截止期限',
                         recovery_kind='manual_check_timeout', definite_not_submitted=True)


class ManualConnectionCheckStale(ApiError):
    def __init__(self):
        super().__init__(None, '连接状态已发生更新，本次旧检查不能确认恢复',
                         recovery_kind='manual_check_superseded', definite_not_submitted=True)


class ManualConnectionCheckBusy(ApiError):
    def __init__(self):
        super().__init__(None, '已有连接检查正在执行，请等待该检查结果',
                         recovery_kind='manual_check_busy', definite_not_submitted=True)


class ManualConnectionWriteForbidden(ApiError):
    def __init__(self, method, path):
        super().__init__(None, '连接检查只允许GET读取，未提交写请求', method=method, path=path,
                         recovery_kind='manual_check_write_forbidden', definite_not_submitted=True)


class _ManualConnectionCheck:
    def __init__(self, client, ticket, deadline_monotonic):
        self.client, self.ticket = client, ticket
        self.generation, self.owner = ticket
        self.deadline_monotonic = deadline_monotonic
        self.thread_id = threading.get_ident()
        self.wire_attempts, self.completed = 0, False

    @property
    def remaining_seconds(self):
        return max(0.0, self.deadline_monotonic - self.client._monotonic())

    def require_current(self):
        if self.thread_id != threading.get_ident():
            raise ManualConnectionCheckBusy()
        if self.remaining_seconds <= 0:
            raise ManualConnectionCheckExpired()
        try:
            self.client._network_retry.check(self.ticket)
        except NetworkBackoff:
            raise ManualConnectionCheckStale() from None

    def snapshot(self):
        return {'generation': self.generation, 'owner': self.owner,
                'wire_attempts': self.wire_attempts, 'completed': self.completed,
                'remaining_seconds': self.remaining_seconds}


def transport_failure_reason(exc: BaseException) -> str:
    """Classify network errors without exposing signed URLs or proxy credentials."""
    reason = exc
    for _ in range(4):
        if not isinstance(reason, URLError) or reason.reason is reason:
            break
        reason = reason.reason
    if isinstance(reason, ssl.SSLCertVerificationError):
        return "TLS证书验证失败，请检查服务器时间及系统证书"
    if isinstance(reason, ssl.SSLEOFError):
        return "TLS连接被远端或代理提前断开"
    if isinstance(reason, ssl.SSLError):
        return "TLS握手或加密连接失败"
    if isinstance(reason, socket.gaierror):
        return "DNS域名解析失败"
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return "连接或读取超时"
    if isinstance(reason, RemoteDisconnected):
        return "远端在返回响应前关闭连接"
    if isinstance(reason, IncompleteRead):
        return "响应传输中断，收到的数据不完整"
    if isinstance(reason, HTTPException):
        return "HTTP连接响应无效或中断"
    error_numbers = {value for value in
                     (getattr(reason, 'errno', None), getattr(reason, 'winerror', None))
                     if isinstance(value, int)}
    if isinstance(reason, ConnectionRefusedError) or error_numbers & {61, 111, 10061}:
        return "连接被拒绝，请检查出站443端口及代理"
    if isinstance(reason, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)) or error_numbers & {54, 104, 10053, 10054, 10058}:
        return "连接被重置或中断"
    if error_numbers & {60, 110, 10060}:
        return "连接或读取超时"
    if error_numbers & {101, 113, 10050, 10051, 10065}:
        return "网络或目标地址不可达"
    # urllib can wrap proxy tunnel failures in OSError or a string. Inspect only
    # known markers; never echo the original string, URL, host or authentication.
    if isinstance(reason, (str, OSError)):
        marker = str(reason).lower()
        if 'tunnel connection failed' in marker:
            if '407' in marker:
                return "代理认证失败（HTTP 407）"
            return "代理隧道连接失败"
        if 'certificate_verify_failed' in marker:
            return "TLS证书验证失败，请检查服务器时间及系统证书"
        if 'timed out' in marker:
            return "连接或读取超时"
    return "网络连接异常，需在服务器运行网络诊断"


def _number(value: Any) -> str:
    amount = Decimal(str(value))
    if not amount.is_finite() or amount <= 0:
        raise ValueError("价格和数量必须是有限正数")
    return format(amount, "f")


def _client_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[.A-Za-z0-9_:/-]{1,36}", value):
        raise ValueError("client ID必须是1至36位合法字符，并在提交前持久化")
    return value


def _side(value: str) -> str:
    value = str(value).upper()
    if value not in ("BUY", "SELL"):
        raise ValueError("side必须是BUY或SELL")
    return value


def _is_false(value: Any) -> bool:
    return value is False or value == "false"


class BinanceClient:
    supports_entry_before_send = True

    def __init__(self, key: str, secret: str, transport: Callable | None = None,
                 base_url: str = "https://fapi.binance.com", *, timeout: float = 12,
                 get_retries: int = 2, recv_window: int = 30000,
                 clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep, budget=None, streams=None,
                 monotonic: Callable[[], float] = time.monotonic,
                  auto_sync_time: bool = True, time_sync_interval: float = 300,
                  diagnostics=None):
        self._key, self._secret = key, secret
        self.diagnostics = diagnostics
        self.transport = transport if transport is not None else PooledHTTPTransport(clock=monotonic)
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        if not 0 <= get_retries <= 3:
            raise ValueError("GET重试次数必须介于0至3")
        if not 1 <= recv_window <= 60000:
            raise ValueError("recvWindow必须介于1至60000毫秒")
        if time_sync_interval <= 0:
            raise ValueError("时间同步间隔必须为正数")
        self.get_retries, self.recv_window = int(get_retries), int(recv_window)
        self._clock, self._sleep, self._offset_ms = clock, sleep, 0
        self._monotonic = monotonic
        self.auto_sync_time, self.time_sync_interval = bool(auto_sync_time), float(time_sync_interval)
        self._time_anchor = None
        self._calibrated_time_snapshot = None
        self._time_rtt_ms = 0
        self._time_lock = threading.RLock()
        self._time_sync_failures = 0
        self._time_sync_retry_at = 0.0
        self._time_sync_error = None
        self._time_sync_failure_generation = None
        # Injected test transports remain entirely offline unless a budget is supplied.
        self.budget = budget if budget is not None else (RequestBudget() if transport is None else None)
        self._network_retry = (getattr(self.budget, 'network', None)
                               or NetworkRetryGate(clock=clock, monotonic=monotonic))
        self.streams = streams
        self._manual_check = None
        self._manual_lock = threading.Lock()

    @contextmanager
    def manual_connection_check(self, deadline_seconds=30):
        """Bounded GET-only inspection with shared recovery ownership.

        It bypasses an ordinary ten-minute network wait, never RequestBudget's
        exchange cooldown or local capacity. The caller owns a dedicated client;
        no trading state, clock anchor or user run intent is changed here.
        """
        seconds = float(deadline_seconds)
        if not 0 < seconds <= 120 or seconds != seconds:
            raise ValueError('连接检查期限必须大于0且不超过120秒')
        deadline = self._monotonic() + seconds
        if not self._manual_lock.acquire(blocking=False):
            raise ManualConnectionCheckBusy()
        ticket = None
        try:
            try:
                ticket = self._network_retry.manual_claim(probe_seconds=seconds)
            except NetworkBackoff as exc:
                raise NetworkRetryPending(exc, method='GET', path='/fapi/v1/time') from None
            check = _ManualConnectionCheck(self, ticket, deadline)
            self._manual_check = check
            check.require_current()
            yield check
            check.require_current()
            if check.wire_attempts:
                check.completed = self._network_retry.manual_complete(ticket)
                if not check.completed:
                    raise ManualConnectionCheckStale()
        finally:
            self._manual_check = None
            self._network_retry.release_probe(ticket)
            self._manual_lock.release()

    def now_ms(self) -> int:
        """Advance calibrated Binance time independently of Windows wall-clock jumps."""
        anchor = self._time_anchor
        if anchor is not None:
            server_at_receive, received_at = anchor
            return round(server_at_receive + (self._monotonic() - received_at) * 1000)
        return int(self._clock() * 1000) + self._offset_ms

    def acknowledge_manual_connection_check(self, evidence):
        """Permit fresh normal calibration after an acknowledged current probe.

        No manual clock anchor is copied and no execution fault is cleared.
        A new shared failure or non-network calibration error keeps its wait.
        """
        if (not isinstance(evidence, dict) or evidence.get('completed') is not True
                or type(evidence.get('generation')) is not int
                or not evidence.get('owner') or not evidence.get('wire_attempts')
                or self._manual_check is not None):
            return False
        with self._time_lock:
            state = self._network_retry.snapshot()
            if (state['generation'] != evidence['generation'] or state['failures']
                    or state['waiting'] or state['recovering'] or state['probing']):
                return False
            if self._time_sync_error is None:
                return True
            if (self._time_sync_error.recovery_kind not in {'network_read_failure', 'network_read_wait'}
                    or self._time_sync_failure_generation != evidence['generation']):
                return False
            self._time_sync_retry_at = 0.0
            self._time_sync_error = None
            self._time_sync_failure_generation = None
            return True

    def calibrated_time_bounds_ms(self) -> tuple[int, int] | None:
        """One atomic, memory-only calibrated sample; never waits for a sync lock."""
        snapshot = self._calibrated_time_snapshot
        if snapshot is None:
            return None
        server, received, rtt = snapshot
        elapsed = self._monotonic() - received
        if elapsed < 0:
            return None
        lower = round(server + elapsed * 1000)
        return lower, lower + rtt

    def sync_time(self) -> int:
        """Public GET only; refresh the server/monotonic anchor after a valid response."""
        with self._time_lock:
            if (self._manual_check is None and self._time_sync_error
                    and self._monotonic() < self._time_sync_retry_at):
                raise self._cached_time_sync_error() from None
            try:
                return self._sync_time_once()
            except ApiError as exc:
                if self._manual_check is not None:
                    # The shared gate owns the original automatic deadline;
                    # this independent manual clock must not replace its error
                    # metadata with a fresh ten-minute calibration wait.
                    raise
                if exc.recovery_kind in {'network_read_wait', 'local_budget_wait', 'exchange_cooldown'}:
                    # Neither a local reservation wait nor an exchange cooldown
                    # is a new failed time request. Preserve its exact boundary.
                    raise
                self._time_sync_failures += 1
                delay = NETWORK_RETRY_SECONDS
                self._time_sync_retry_at = self._monotonic() + delay
                exc.retry_at_ms = int((self._clock() + delay) * 1000)
                exc.retry_in_seconds = delay
                # Save only the redacted transport description. Repeated local
                # checks reuse a stable error, rather than creating countdown
                # events or fresh network requests every execution tick.
                self._time_sync_error = exc
                self._time_sync_failure_generation = self._network_retry.snapshot()['generation']
                raise

    def _cached_time_sync_error(self) -> ApiError:
        cached = self._time_sync_error
        remaining = max(0, self._time_sync_retry_at - self._monotonic())
        return ApiError(cached.code, cached.message, status=cached.status,
                        method='GET', path='/fapi/v1/time',
                        recovery_kind=('network_read_wait' if cached.recovery_kind == 'network_read_failure'
                                       else cached.recovery_kind),
                        retry_at_ms=int((self._clock() + remaining) * 1000),
                        retry_in_seconds=remaining, definite_not_submitted=True,
                        definite_rejected=cached.definite_rejected)

    def _sync_time_once(self) -> int:
        with self._time_lock:
            sample = {}
            result = self.request("GET", "/fapi/v1/time", _time_sample=sample)
            before, after = sample['local_before'], sample['local_after']
            sent_at, received_at = sample['sent_at'], sample['received_at']
            try:
                value = result["serverTime"]
                if isinstance(value, bool) or not isinstance(value, (int, str)):
                    raise ValueError("invalid server time type")
                server = int(value)
                if server <= 0 or received_at < sent_at:
                    raise ValueError("invalid server time")
            except (KeyError, TypeError, ValueError) as exc:
                raise ApiError(None, "服务器时间响应无效", method="GET", path="/fapi/v1/time",
                               recovery_kind='network_read_failure') from exc
            # serverTime was produced before receipt. A slow DNS/TLS/proxy
            # handshake can account for almost all RTT, so adding RTT/2 can
            # incorrectly put the signature more than one second in the future.
            # Keep a conservative server-time anchor; monotonic elapsed time
            # advances it without guessing which direction caused the delay.
            self._time_rtt_ms = round((received_at - sent_at) * 1000)
            self._offset_ms = server - after
            self._time_anchor = (server, received_at)
            self._calibrated_time_snapshot = (server, received_at, self._time_rtt_ms)
            self._time_sync_failures = 0
            self._time_sync_retry_at = 0.0
            self._time_sync_error = None
            self._time_sync_failure_generation = None
            return self.now_ms()

    def clock_status(self) -> dict:
        """Non-secret timing details for identifying the running fix in logs."""
        anchor = self._time_anchor
        return {'version': 'time-sync-v2', 'synced': anchor is not None,
                'sync_rtt_ms': self._time_rtt_ms,
                'sync_age_ms': max(0, round((self._monotonic() - anchor[1]) * 1000)) if anchor else None,
                'recv_window_ms': self.recv_window,
                'signature_margin_ms': 250 if anchor else 0,
                'sync_failures': self._time_sync_failures,
                'retry_in_seconds': max(0, round(self._time_sync_retry_at - self._monotonic(), 1)),
                'retry_interval_seconds': NETWORK_RETRY_SECONDS}

    def network_status(self) -> dict:
        return self._network_retry.snapshot()

    def connection_status(self) -> dict | None:
        getter = getattr(self.transport, 'snapshot', None)
        return getter() if callable(getter) else None

    def close(self):
        closer = getattr(self.transport, 'close', None)
        if callable(closer):
            closer()

    def now_upper_ms(self) -> int:
        """Conservative time bound for refusing an entry after its deadline."""
        return self.now_ms() + (self._time_rtt_ms if self._time_anchor else 0)

    def ensure_time_sync(self) -> int:
        """First use and every five minutes; no automatic trading permission changes."""
        with self._time_lock:
            anchor = self._time_anchor
            if self.auto_sync_time and (anchor is None or
                    self._monotonic() - anchor[1] >= self.time_sync_interval):
                return self.sync_time()
            return self.now_ms()

    def public(self, method: str, path: str, params: Mapping | None = None) -> Any:
        return self.request(method, path, params, signed=False)

    def signed(self, method: str, path: str, params: Mapping | None = None) -> Any:
        return self.request(method, path, params, signed=True)

    def request(self, method: str, path: str, params: Mapping | None = None,
                *, signed: bool = False, _time_sample: dict | None = None,
                _deadline_ms: int | None = None, _before_send=None, _wire_guard=None) -> Any:
        if self._manual_check is not None:
            if method.upper() != 'GET':
                raise ManualConnectionWriteForbidden(method.upper(), path)
            self._manual_check.require_current()
        if self.diagnostics is None:
            return self._request_impl(method, path, params, signed=signed,
                _time_sample=_time_sample, _deadline_ms=_deadline_ms,
                _before_send=_before_send, _wire_guard=_wire_guard)
        with self.diagnostics.request(method.upper(), path, signed):
            return self._request_impl(method, path, params, signed=signed,
                _time_sample=_time_sample, _deadline_ms=_deadline_ms,
                _before_send=_before_send, _wire_guard=_wire_guard)

    def _diagnostic_phase(self, name, *, new_attempt=False):
        if self.diagnostics is not None:
            self.diagnostics.request_phase(name, new_attempt=new_attempt)

    def _request_impl(self, method: str, path: str, params: Mapping | None = None,
                *, signed: bool = False, _time_sample: dict | None = None,
                _deadline_ms: int | None = None, _before_send=None, _wire_guard=None) -> Any:
        method = method.upper()
        manual = self._manual_check
        if method not in {"GET", "POST", "DELETE"}:
            raise ValueError("仅支持GET、POST和DELETE")
        if not path.startswith("/fapi/") or "?" in path:
            raise ValueError("使用不包含查询参数的fapi路径")
        base_params = {k: v for k, v in (params or {}).items() if v is not None}
        if "signature" in base_params:
            raise ValueError("签名由transport统一生成")
        entry_admission = _before_send is not None or _wire_guard is not None
        if entry_admission and (method != 'POST' or path != '/fapi/v1/order'
                or base_params.get('type') != 'MARKET'
                or base_params.get('reduceOnly') not in (None, False, 'false')
                or any(callback is not None and not callable(callback)
                       for callback in (_before_send, _wire_guard))):
            raise ValueError('发送前登记接口仅支持非只减仓市价入场')
        before_send_called = False

        def entry_ready():
            if not entry_admission:
                return
            try:
                self._network_retry.check()
            except NetworkBackoff as exc:
                raise NetworkRetryPending(exc, method=method, path=path) from None
            if _deadline_ms is not None and self.now_upper_ms() >= _deadline_ms:
                raise RequestExpired(None, '入场发送前五分钟窗口已结束，未提交', method=method, path=path)
            if self.budget:
                state = self.budget.snapshot()
                until = state.get('cooldown_until_ms', 0) / 1000
                budget_clock = getattr(self.budget, 'clock', self._clock)
                if until > budget_clock():
                    raise ReadBudgetDeferred(RateLimited(until, clock=budget_clock, source='exchange_cooldown'),
                                             method=method, path=path)
            if _wire_guard is not None:
                _local_admission_callback(_wire_guard, 'guard')
            if _deadline_ms is not None and self.now_upper_ms() >= _deadline_ms:
                raise RequestExpired(None, '入场控制核对后五分钟窗口已结束，未提交', method=method, path=path)

        def admit_once():
            nonlocal before_send_called
            entry_ready()
            if not before_send_called and _before_send is not None:
                before_send_called = True
                _local_admission_callback(_before_send, 'registration')
            entry_ready()
        client_id = base_params.get("newClientOrderId") or base_params.get("clientAlgoId")
        client_id = client_id or base_params.get("origClientOrderId")
        clock_recovered = False
        while True:
            if manual is not None:
                manual.require_current()
            self._diagnostic_phase('request_budget', new_attempt=True)
            reserved_minute = None
            if method == 'GET' or entry_admission:
                try:
                    self._network_retry.check(manual.ticket if manual is not None else None)
                except NetworkBackoff as exc:
                    raise NetworkRetryPending(exc, method=method, path=path) from None
            if self.budget:
                try:
                    reserved_minute = self.budget.reserve(method, path, base_params, critical=method != 'GET' or path.endswith(('/order', '/algoOrder')))
                except RateLimited as exc:
                    # Definite local rejection before wire: never mark a write uncertain.
                    raise ReadBudgetDeferred(exc, method=method, path=path) from None
            values = dict(base_params)
            headers: dict[str, str] = {"Accept": "application/json"}
            if signed:
                self._diagnostic_phase('time_sync')
                if not self._key or not self._secret:
                    raise ApiError(None, "缺少API凭据", method=method, path=path,
                                   recovery_kind='credentials_error', definite_not_submitted=True)
                # This is before the signed request is sent. A failed public
                # calibration cannot create an uncertain trading submission.
                try:
                    self.ensure_time_sync()
                except ApiError as exc:
                    if method != 'GET':
                        exc.definite_not_submitted = True
                        exc.submission_method, exc.submission_path = method, path
                    raise
                if _deadline_ms is not None and self.now_upper_ms() >= _deadline_ms:
                    raise RequestExpired(None, '校时或请求等待后开仓窗口已结束，未提交',
                                         method=method, path=path)
                # The business clock stays separate from this small signature
                # margin, so hourly signal boundaries are not moved by it.
                values["timestamp"] = self.now_ms() - (250 if self._time_anchor else 0)
                values["recvWindow"] = self.recv_window
                headers["X-MBX-APIKEY"] = self._key
            encoded = urlencode({k: (str(v).lower() if isinstance(v, bool) else str(v))
                                 for k, v in values.items()})
            if signed:
                signature = hmac.new(self._secret.encode(), encoded.encode(), hashlib.sha256).hexdigest()
                encoded += "&signature=" + signature
            body = None
            url = self.base_url + path
            if method == "POST":
                body = encoded.encode("utf-8")
                headers["Content-Type"] = "application/x-www-form-urlencoded"
            elif encoded:
                url += "?" + encoded
            req = Request(url, data=body, headers=headers, method=method)
            status: int | None = None
            response_headers = None
            ticket = None
            request_minute = None
            if method == 'GET' and not isinstance(self.transport, PooledHTTPTransport):
                try:
                    ticket = (manual.ticket if manual is not None else
                              self._network_retry.before_read(probe_seconds=self.timeout + 5))
                except NetworkBackoff as exc:
                    raise NetworkRetryPending(exc, method=method, path=path) from None
            def prepare_for_wire(_request, *, claim_ticket=True):
                self._diagnostic_phase('wire_preparation')
                nonlocal ticket, request_minute, local_before, sent_at
                if manual is not None:
                    manual.require_current()
                entry_ready()
                # Waiting for local ownership never spends the recovery lease
                # or leaves an old timestamp on a subsequently sent request.
                fresh_values = dict(base_params)
                if signed:
                    if _deadline_ms is not None and self.now_upper_ms() >= _deadline_ms:
                        raise RequestExpired(None, '连接等待后开仓窗口已结束，未提交',
                                             method=method, path=path)
                    fresh_values.update(timestamp=self.now_ms() - (250 if self._time_anchor else 0),
                                        recvWindow=self.recv_window)
                fresh_encoded = urlencode({k: (str(v).lower() if isinstance(v, bool) else str(v))
                                           for k, v in fresh_values.items()})
                if signed:
                    signature = hmac.new(self._secret.encode(), fresh_encoded.encode(), hashlib.sha256).hexdigest()
                    fresh_encoded += '&signature=' + signature
                if method == 'GET' and claim_ticket:
                    try:
                        ticket = (manual.ticket if manual is not None else
                                  self._network_retry.before_read(probe_seconds=self.timeout + 5))
                    except NetworkBackoff as exc:
                        raise NetworkRetryPending(exc, method=method, path=path) from None
                if _time_sample is not None:
                    local_before, sent_at = int(self._clock() * 1000), self._monotonic()
                if self.budget:
                    request_clock = getattr(self.budget, 'clock', self._clock)
                    request_minute = max(int(request_clock() // 60), reserved_minute or 0)
                if not claim_ticket:
                    self._diagnostic_phase('network_call')
                    if manual is not None:
                        manual.wire_attempts += 1
                return Request(self.base_url + path + ('?' + fresh_encoded if method != 'POST' and fresh_encoded else ''),
                    data=fresh_encoded.encode('utf-8') if method == 'POST' else None,
                    headers=headers, method=method)
            try:
                try:
                    if _time_sample is not None:
                        local_before, sent_at = int(self._clock() * 1000), self._monotonic()
                    if self.budget:
                        request_clock = getattr(self.budget, 'clock', self._clock)
                        request_minute = int(request_clock() // 60)
                        if type(reserved_minute) is int:
                            request_minute = max(request_minute, reserved_minute)
                    timeout = self.timeout
                    if manual is not None:
                        manual.require_current()
                        timeout = min(timeout, manual.remaining_seconds)
                    if isinstance(self.transport, PooledHTTPTransport):
                        options = ({'on_wire': self.diagnostics.wire} if self.diagnostics is not None else {})
                        if self.diagnostics is not None or manual is not None or entry_admission:
                            options['refresh_request'] = lambda request: prepare_for_wire(request, claim_ticket=False)
                        if entry_admission:
                            options['before_send'] = admit_once
                        response = self.transport(req, timeout=timeout, prepare_request=prepare_for_wire, **options)
                    else:
                        if entry_admission:
                            admit_once()
                        if self.diagnostics is not None:
                            self.diagnostics.wire()
                        if self.diagnostics is not None or manual is not None or entry_admission:
                            req = prepare_for_wire(req, claim_ticket=False)
                        response = self.transport(req, timeout=timeout)
                    try:
                        status = getattr(response, "status", None)
                        if status is None and hasattr(response, "getcode"):
                            status = response.getcode()
                        self._diagnostic_phase('response_body')
                        raw = response.read()
                        if _time_sample is not None:
                            received_at, local_after = self._monotonic(), int(self._clock() * 1000)
                        response_headers = getattr(response, 'headers', None)
                    finally:
                        response.close()
                except HTTPError as exc:
                    status = exc.code
                    response_headers = exc.headers
                    try:
                        raw = exc.read()
                    finally:
                        exc.close()
                if self.budget:
                    self.budget.observe(response_headers, status=status, path=path, request_minute=request_minute)
                try:
                    self._diagnostic_phase('response_decode')
                    result = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                except (ValueError, UnicodeDecodeError, TypeError) as exc:
                    http_status = f"；HTTP {status}" if status is not None else ""
                    message = (f"读取接口失败（GET {path}{http_status}；响应格式无效）"
                               if method == "GET" else
                               f"响应格式无效（{method} {path}{http_status}），请查询原请求，禁止重复提交")
                    err = self._error(method, path, client_id, None, message, status)
                    raise err from exc
                code = None
                if isinstance(result, dict) and "code" in result:
                    try:
                        code = int(result["code"])
                    except (ValueError, TypeError):
                        pass
                if (status is not None and status >= 400) or (code is not None and code < 0):
                    if self.diagnostics is not None:
                        self.diagnostics.response_status(status, code)
                    message = str(result.get("msg", "接口请求失败")) if isinstance(result, dict) else "接口请求失败"
                    if self.budget:
                        self.budget.observe(response_headers, status=status, code=code, message=message, path=path,
                                            request_minute=request_minute)
                    raise self._error(method, path, client_id, code, message, status)
                if self.budget:
                    self.budget.observe(response_headers, status=status, code=code, path=path, request_minute=request_minute)
                if manual is not None:
                    manual.require_current()
                if _time_sample is not None:
                    # Only the successful wire attempt contributes RTT. Earlier
                    # timeouts/backoff must not create a clock several seconds fast.
                    _time_sample.update(local_before=local_before, local_after=local_after,
                                        sent_at=sent_at, received_at=received_at)
                if method == 'GET' and manual is None:
                    self._network_retry.succeeded(ticket)
                if self.diagnostics is not None:
                    self.diagnostics.response_status(status, code)
                    if path == '/fapi/v1/accountConfig' and isinstance(result, dict):
                        self.diagnostics.observe_account(result)
                    elif path == '/fapi/v3/account' and isinstance(result, dict):
                        self.diagnostics.observe_wallet_fee(result)
                    elif path == '/fapi/v1/symbolConfig':
                        self.diagnostics.observe_symbols(result)
                    elif path == '/fapi/v1/commissionRate' and isinstance(result, dict):
                        self.diagnostics.observe_account_fee(result)
                return result
            except (URLError, OSError, TimeoutError, HTTPException) as exc:
                if manual is not None:
                    manual.require_current()
                # Do not include exception text: urlopen may embed the signed URL.
                reason = transport_failure_reason(exc)
                local_entry_failure = entry_admission and getattr(exc, 'definite_not_submitted', False) is True
                message = (f"入场连接准备失败（{method} {path}；{reason}），未提交"
                           if local_entry_failure else
                           f"网络读取失败（GET {path}；{reason}）；每10分钟自动重试"
                           if method == "GET" else
                           f"网络响应未确认（{method} {path}；{reason}），请查询原请求，禁止重复提交")
                err = (ApiError(None, message, method=method, path=path,
                                recovery_kind='network_read_failure', definite_not_submitted=True)
                       if local_entry_failure else self._error(method, path, client_id, None, message, status))
                self._network_retry.failed(message, ticket=manual.ticket if manual is not None else None,
                                           preserve_deadline=manual is not None)
                self._attach_network_deadline(err)
                raise err from exc
            except ApiError as exc:
                if exc.definite_not_submitted:
                    if manual is None:
                        self._network_retry.release_probe(ticket)
                    raise
                if exc.status in {418, 429} or exc.code == -1003:
                    retry_at = cooldown_deadline(response_headers, status=exc.status,
                                                 code=exc.code, message=exc.message, now=self._clock())
                    if self.budget:
                        retry_at = max(retry_at, self.budget.snapshot()['cooldown_until_ms'] / 1000)
                    if not isinstance(exc, OrderUncertain):
                        exc.recovery_kind = 'exchange_cooldown'
                    exc.retry_at_ms = int(retry_at * 1000)
                    exc.retry_in_seconds = max(0, retry_at - self._clock())
                    if manual is None:
                        self._network_retry.defer_probe(ticket)
                    raise  # Respect persistent IP cooldown; never rapid-retry a rate rejection.
                if manual is not None:
                    manual.require_current()
                if signed and exc.code == -1021:
                    timing = self.clock_status()
                    exc.message += (f"；{method} {path}；{timing['version']}"
                        f"；校时RTT={timing['sync_rtt_ms']}ms"
                        f"；校时年龄={timing['sync_age_ms']}ms"
                        f"；recvWindow={self.recv_window}ms")
                    exc.args = (f"Binance -1021: {exc.message}",)
                if (signed and exc.code == -1021 and not isinstance(exc, OrderUncertain)
                        and not clock_recovered):
                    if manual is None:
                        self._network_retry.succeeded(ticket)
                    clock_recovered = True
                    try:
                        self.sync_time()
                    except ApiError as sync_error:
                        # Preserve the definite rejection of the original write.
                        # Never relabel it uncertain because calibration failed.
                        exc.message += "; 币安校时未完成：" + str(sync_error)
                        exc.args = (f"Binance -1021: {exc.message}",)
                        exc.retry_at_ms = sync_error.retry_at_ms
                        exc.retry_in_seconds = sync_error.retry_in_seconds
                        exc.recovery_cause_kind = sync_error.recovery_kind
                        raise exc from sync_error
                    if method == "GET":
                        continue  # One additional read, freshly timestamped/signed.
                    # An explicit timestamp rejection is terminal for this
                    # persisted write. The engine decides the next action.
                    raise
                transient = (exc.code in {-1006, -1007} or
                             exc.status in {408} or
                             (exc.status is not None and exc.status >= 500) or
                             (exc.code is None and (exc.status is None or exc.status < 400)))
                if transient:
                    # Do not store arbitrary response text, which may contain
                    # request details; only a static endpoint/status description.
                    retry_reason = (f'接口连接读取失败（{method} {path}；HTTP {exc.status}；code {exc.code}）；'
                                    '每10分钟自动重试')
                    self._network_retry.failed(retry_reason,
                        ticket=manual.ticket if manual is not None else None,
                        preserve_deadline=manual is not None)
                    self._attach_network_deadline(exc)
                else:
                    if manual is None:
                        self._network_retry.defer_probe(ticket)
                raise

    def _attach_network_deadline(self, exc):
        state = self._network_retry.snapshot()
        if not isinstance(exc, OrderUncertain):
            exc.recovery_kind = 'network_read_failure'
        exc.retry_at_ms = state['retry_at_ms']
        exc.retry_in_seconds = state['retry_in_seconds']

    @staticmethod
    def _error(method: str, path: str, client_id: str | None, code: int | None,
               message: str, status: int | None) -> ApiError:
        uncertain = method != "GET" and (
            code in {-1006, -1007} or (code is None and status not in {418, 429}) or status == 408 or
            (status is not None and status >= 500))
        transient = (code in {-1006, -1007} or status == 408 or
                     (status is not None and status >= 500) or
                     (code is None and (status is None or status < 400)))
        args = dict(status=status, method=method, path=path,
                    recovery_kind='network_read_failure' if transient else None,
                    definite_rejected=not uncertain and method != 'GET')
        if uncertain:
            return OrderUncertain(code, message, client_id=client_id, **args)
        return ApiError(code, message, **args)

    def klines(self, symbol: str, limit: int = 1500, *, startTime: int | None = None,
               endTime: int | None = None) -> list:
        if not 1 <= limit <= 1500:
            raise ValueError("小时线limit必须介于1至1500")
        return self.public("GET", "/fapi/v1/klines", dict(
            symbol=symbol, interval="1h", limit=limit, startTime=startTime, endTime=endTime))

    def book_ticker(self, symbol: str) -> dict:
        cached = self.streams.latest('book', symbol, max_age=3) if self.streams else None
        if cached:
            return cached
        return self.public("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol})

    def entry_book_ticker(self, symbol: str):
        """One entry reference and its memory-only original-age proof."""
        observe = getattr(self.streams, 'entry_book_observation', None)
        cached = observe(symbol) if callable(observe) else None
        if cached is not None:
            return cached
        value = self.public('GET', '/fapi/v1/ticker/bookTicker', {'symbol': symbol})
        received = self._monotonic()
        bounds = self.calibrated_time_bounds_ms()
        stamp = value.get('time') if isinstance(value, dict) else None
        valid_stamp = (type(stamp) is int and stamp > 0) or (
            type(stamp) is str and stamp.isascii() and stamp.isdigit() and int(stamp) > 0)
        event = int(stamp) if valid_stamp else None
        initial_age = max(0, bounds[1] - event) if bounds is not None and event is not None else None

        def is_fresh():
            elapsed = self._monotonic() - received
            current = self.calibrated_time_bounds_ms()
            return bool(initial_age is not None and current is not None and 0 <= elapsed < 3 and
                max(initial_age + elapsed * 1000, current[1] - event) < 3000 and
                event - current[0] <= 3000)

        return value, is_fresh

    def contract_price(self, symbol: str) -> dict:
        cached = self.streams.latest('trade', symbol, max_age=3) if self.streams else None
        if cached:
            return cached
        return self.public("GET", "/fapi/v2/ticker/price", {"symbol": symbol})

    def mark_price(self, symbol: str) -> dict:
        cached = self.streams.latest('mark', symbol, max_age=3) if self.streams else None
        if cached:
            return cached
        return self.public("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})

    def exchange_info(self) -> dict:
        # The caller parses PRICE_FILTER/LOT_SIZE/MARKET_LOT_SIZE; precision is not a step.
        return self.public("GET", "/fapi/v1/exchangeInfo")

    def account(self) -> dict:
        # Fee/funding already included in wallet; do not subtract them here again.
        return self.signed("GET", "/fapi/v3/account")

    def positions(self, symbol: str | None = None) -> list:
        return self.signed("GET", "/fapi/v3/positionRisk", {"symbol": symbol})

    def account_config(self) -> dict:
        return self.signed("GET", "/fapi/v1/accountConfig")

    def symbol_config(self, symbol: str | None = None) -> list:
        return self.signed("GET", "/fapi/v1/symbolConfig", {"symbol": symbol})

    def commission_rate(self, symbol: str) -> dict:
        """Explicit diagnostic GET; never changes the fixed planning fee."""
        if symbol not in {'BTCUSDT', 'ETHUSDT'}:
            raise ValueError('仅支持BTCUSDT和ETHUSDT费率观察')
        return self.signed('GET', '/fapi/v1/commissionRate', {'symbol': symbol})

    def position_mode(self) -> dict:
        return self.signed("GET", "/fapi/v1/positionSide/dual")

    def multi_assets_mode(self) -> dict:
        return self.signed("GET", "/fapi/v1/multiAssetsMargin")

    def leverage_brackets(self, symbol: str | None = None) -> dict | list:
        return self.signed("GET", "/fapi/v1/leverageBracket", {"symbol": symbol})

    def open_orders(self, symbol: str | None = None) -> list:
        return self.signed("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    def open_algos(self, symbol: str | None = None) -> list:
        return self.signed("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})

    @staticmethod
    def _order_identity(client_id: str | None, order_id: int | None) -> dict:
        if order_id is not None:
            return {"orderId": int(order_id)}
        if client_id is not None:
            return {"origClientOrderId": _client_id(client_id)}
        raise ValueError("查询或取消必须提供原client_id或order_id")

    @staticmethod
    def _algo_identity(client_id: str | None, algo_id: int | None) -> dict:
        if algo_id is not None:
            return {"algoId": int(algo_id)}
        if client_id is not None:
            return {"clientAlgoId": _client_id(client_id)}
        raise ValueError("查询或取消必须提供原client_id或algo_id")

    def query_order(self, symbol: str, client_id: str | None = None,
                    order_id: int | None = None) -> dict:
        return self.signed("GET", "/fapi/v1/order",
                           {"symbol": symbol, **self._order_identity(client_id, order_id)})

    def market_order(self, symbol: str, side: str, quantity: Any, client_id: str,
                     reduce_only: bool = False, *, deadline_ms: int | None = None,
                     before_send=None, wire_guard=None) -> dict:
        return self.request("POST", "/fapi/v1/order", dict(
            symbol=symbol, side=_side(side), type="MARKET", quantity=_number(quantity),
            positionSide="BOTH", newClientOrderId=_client_id(client_id),
            reduceOnly=bool(reduce_only), newOrderRespType="ACK"),
            signed=True, _deadline_ms=deadline_ms, _before_send=before_send, _wire_guard=wire_guard)

    def cancel_order(self, symbol: str, client_id: str | None = None,
                     order_id: int | None = None) -> dict:
        return self.signed("DELETE", "/fapi/v1/order",
                           {"symbol": symbol, **self._order_identity(client_id, order_id)})

    def new_stop(self, symbol: str, side: str, trigger: Any, client_id: str) -> dict:
        return self.signed("POST", "/fapi/v1/algoOrder", dict(
            algoType="CONDITIONAL", symbol=symbol, side=_side(side), type="STOP_MARKET",
            positionSide="BOTH", triggerPrice=_number(trigger), clientAlgoId=_client_id(client_id),
            workingType="CONTRACT_PRICE", priceProtect=False, closePosition=True))

    def new_stop_quantity(self, symbol: str, side: str, trigger: Any, client_id: str,
                          quantity: Any) -> dict:
        """Protect confirmed quantity without a second close-all condition."""
        return self.signed("POST", "/fapi/v1/algoOrder", dict(
            algoType="CONDITIONAL", symbol=symbol, side=_side(side), type="STOP_MARKET",
            positionSide="BOTH", triggerPrice=_number(trigger), clientAlgoId=_client_id(client_id),
            quantity=_number(quantity), reduceOnly=True,
            workingType="CONTRACT_PRICE", priceProtect=False))

    def query_algo(self, client_id: str | None = None, algo_id: int | None = None) -> dict:
        return self.signed("GET", "/fapi/v1/algoOrder", self._algo_identity(client_id, algo_id))

    def cancel_algo(self, client_id: str | None = None, algo_id: int | None = None) -> dict:
        return self.signed("DELETE", "/fapi/v1/algoOrder", self._algo_identity(client_id, algo_id))

    def fills(self, symbol: str, order_id: int | None = None, *, fromId: int | None = None,
              startTime: int | None = None, endTime: int | None = None, limit: int = 1000) -> list:
        if not 1 <= limit <= 1000:
            raise ValueError("成交分页limit必须介于1至1000")
        if fromId is not None and (startTime is not None or endTime is not None):
            raise ValueError("fromId不能与startTime/endTime同时使用")
        if startTime is not None and endTime is not None:
            if endTime < startTime or endTime - startTime > 7 * 86400000:
                raise ValueError("成交时间查询每页不得超过7天")
        return self.signed("GET", "/fapi/v1/userTrades", dict(
            symbol=symbol, orderId=order_id, fromId=fromId, startTime=startTime,
            endTime=endTime, limit=limit))

    def fills_all(self, symbol: str, order_id: int | None = None, *, fromId: int | None = None,
                  startTime: int | None = None, endTime: int | None = None,
                  limit: int = 1000, max_pages: int = 100) -> list:
        """Fetch a bounded requested range, deduplicating by exchange trade ID.

        Time ranges are split into <=7-day requests. Full pages advance by
        actual trade ID (not time+1, which would lose same-millisecond fills).
        Raises if the page bound is reached; never labels truncated data complete.
        Without a cursor/time bound Binance returns recent fills only. The caller
        must also compare recovered fill quantity with order.executedQty and
        cannot infer a zero fill merely from an empty historical query.
        """
        if fromId is not None and (startTime is not None or endTime is not None):
            raise ValueError("fromId不能与时间区间同时使用")
        if max_pages < 1:
            raise ValueError("max_pages必须为正数")
        lower = startTime
        upper = endTime if endTime is not None else self.now_ms()
        if lower is not None and upper < lower:
            raise ValueError("endTime早于startTime")
        windows = [(None, None)]
        if lower is not None:
            windows = []
            cursor = lower
            while cursor <= upper:
                stop = min(cursor + 7 * 86400000 - 1, upper)
                windows.append((cursor, stop))
                cursor = stop + 1
        elif endTime is not None:
            windows = [(max(0, upper - 7 * 86400000 + 1), upper)]
        trades: dict[int, dict] = {}
        pages = 0
        for start, stop in windows:
            cursor_id = fromId
            while True:
                if pages >= max_pages:
                    raise ApiError(None, "成交查询达到分页上限，不能确认成交完整性")
                batch = self.fills(symbol, order_id, fromId=cursor_id,
                                   startTime=start if cursor_id is None else None,
                                   endTime=stop if cursor_id is None else None, limit=limit)
                pages += 1
                if not isinstance(batch, list):
                    raise ApiError(None, "成交列表响应无效")
                if not batch:
                    break
                try:
                    ids = [int(row["id"]) for row in batch]
                    times = [int(row["time"]) for row in batch]
                except (KeyError, TypeError, ValueError) as exc:
                    raise ApiError(None, "成交缺少交易所成交ID或实际成交时间") from exc
                for row, trade_id, fill_time in zip(batch, ids, times):
                    if start is not None and fill_time < start:
                        continue
                    if stop is not None and fill_time > stop:
                        continue
                    if order_id is not None and int(row.get("orderId", -1)) != int(order_id):
                        continue
                    trades[trade_id] = row
                if len(batch) < limit or (stop is not None and max(times) > stop):
                    break
                next_id = max(ids) + 1
                if cursor_id is not None and next_id <= cursor_id:
                    raise ApiError(None, "成交分页ID未前进，结果待核对")
                cursor_id = next_id
        return sorted(trades.values(), key=lambda row: (int(row["time"]), int(row["id"])))

    def income(self, symbol: str | None = None, *, income_type: str | None = "FUNDING_FEE",
               startTime: int | None = None, endTime: int | None = None,
               page: int = 1, limit: int = 1000) -> list:
        if not 1 <= limit <= 1000 or page < 1:
            raise ValueError("资金费分页参数无效")
        return self.signed("GET", "/fapi/v1/income", dict(
            symbol=symbol, incomeType=income_type, startTime=startTime,
            endTime=endTime, page=page, limit=limit))

    def set_cross50(self, symbol: str) -> dict:
        """Called only by the explicit human Start action, never construction.

        Reads account modes and only defines symbol CROSSED/50 mutations. Mode
        mismatch or uncertainty prevents progress; dual/multi modes are never set.
        """
        if not _is_false(self.position_mode().get("dualSidePosition")):
            raise AccountModeError(None, "请在币安手动切换为单向持仓后再启动")
        if not _is_false(self.multi_assets_mode().get("multiAssetsMargin")):
            raise AccountModeError(None, "请在币安手动切换为单资产保证金后再启动")
        config = self._find_symbol_config(symbol)
        if str(config.get("marginType", "")).upper() != "CROSSED":
            try:
                self.signed("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": "CROSSED"})
            except ApiError as exc:
                # Already-CROSSED is only accepted after a fresh read verifies it.
                if exc.code != -4046:
                    raise
            config = self._find_symbol_config(symbol)
            if str(config.get("marginType", "")).upper() != "CROSSED":
                raise ApiError(None, "尚未确认全仓保证金设置生效")
        if int(config.get("leverage", 0)) != 50:
            self.signed("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": 50})
            config = self._find_symbol_config(symbol)
        if str(config.get("marginType", "")).upper() != "CROSSED" or int(config.get("leverage", 0)) != 50:
            raise ApiError(None, "尚未确认全仓50倍设置生效")
        return config

    def _find_symbol_config(self, symbol: str) -> dict:
        rows = self.symbol_config(symbol)
        if not isinstance(rows, list):
            raise ApiError(None, "合约配置响应无效")
        for row in rows:
            if row.get("symbol") == symbol:
                return row
        raise ApiError(None, f"未找到{symbol}的杠杆和保证金配置")
