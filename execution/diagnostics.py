"""Bounded, credential-free execution evidence. No network or trading actions."""
import copy
from contextlib import contextmanager, closing
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import wraps
import json
import os
import sqlite3
import threading
import time
import uuid

VERSION = 'stage1-execution-evidence-v1'
PATHS = {'/fapi/v1/' + v for v in (
    'time', 'exchangeInfo', 'klines', 'ticker/bookTicker', 'premiumIndex',
    'accountConfig', 'symbolConfig', 'positionSide/dual', 'multiAssetsMargin',
    'leverageBracket', 'openOrders', 'openAlgoOrders', 'order', 'algoOrder',
    'userTrades', 'income', 'marginType', 'leverage', 'commissionRate')}
PATHS |= {'/fapi/v3/account', '/fapi/v3/positionRisk', '/fapi/v2/ticker/price'}
KINDS = {'execution_review', 'network_read_failure', 'network_read_wait',
    'local_budget_wait', 'exchange_cooldown', 'credentials_error', 'exchange_rejected',
    'protection_replacement', 'permanent', 'order_uncertain', 'request_expired',
    'timestamp_rejected', 'signal_expired'}
EXCEPTIONS = {'ApiError', 'OrderUncertain', 'RequestExpired', 'ReadBudgetDeferred',
    'LocalTransportDeferred', 'NetworkRetryPending', 'ExecutionFault', 'AccountModeError',
    'ValueError', 'TypeError', 'RuntimeError', 'OSError', 'TimeoutError', 'URLError'}


def error_fields(error, kind=None):
    name = type(error).__name__ if error is not None else None
    category = kind or getattr(error, 'recovery_kind', None)
    if name == 'OrderUncertain':
        category = 'order_uncertain'
    if name == 'RequestExpired':
        category = 'request_expired'
    def number(value):
        return value if type(value) is int else None
    return {'exception_type': name if name in EXCEPTIONS else ('OtherException' if name else None),
        'kind': category if category in KINDS else 'execution_review',
        'code': number(getattr(error, 'code', None)),
        'http_status': number(getattr(error, 'status', None)),
        'definite_not_submitted': getattr(error, 'definite_not_submitted', False) is True,
        'definite_rejected': getattr(error, 'definite_rejected', False) is True}


def trace_stage(name):
    def decorate(method):
        @wraps(method)
        def call(self, *args, **kwargs):
            recorder = getattr(self, 'diagnostics', None)
            if recorder is None:
                return method(self, *args, **kwargs)
            with recorder.operation(name):
                with recorder.stage(name):
                    return method(self, *args, **kwargs)
        return call
    return decorate


class ExecutionDiagnostics:
    def __init__(self, path, *, release, monotonic=time.monotonic, wall=time.time,
                 max_records=1000):
        self.path, self.release = str(path), release
        self.monotonic, self.wall = monotonic, wall
        self.max_records = max(1, int(max_records))
        self.process_id = uuid.uuid4().hex
        self.pid = os.getpid()
        self.started_utc = self._utc()
        self.local = threading.local()
        self.lock = threading.RLock()
        self.last, self.current, self.notes = None, None, {}
        self.active = {}
        self.persist_errors = self.dropped_details = 0
        self._last_idle_save = float('-inf')

    def _utc(self):
        return datetime.fromtimestamp(self.wall(), timezone.utc).isoformat()

    @contextmanager
    def operation(self, source):
        existing = getattr(self.local, 'operation', None)
        if existing is not None:
            yield existing
            return
        value = {'id': uuid.uuid4().hex, 'process_id': self.process_id,
            'source': source, 'started_utc': self._utc(), 'stages': [], 'requests': [],
            'timestamp_source': 'host_utc', 'duration_source': 'monotonic',
            'faults': [], 'request_count': 0, 'write_boundary_count': 0}
        started = self.monotonic()
        self.local.operation, self.local.stages, self.local.requests = value, [], []
        with self.lock:
            self.current = {'id': value['id'], 'source': source, 'phase': source}
            self.active[value['id']] = self.current
        try:
            yield value
        except BaseException as exc:
            self.record_fault(exc)
            raise
        finally:
            value['duration_ms'] = round(max(0, self.monotonic() - started) * 1000)
            value['ended_utc'] = self._utc()
            value['outcome'] = 'fault' if value['faults'] else 'complete'
            self.local.operation = None
            with self.lock:
                self.active.pop(value['id'], None)
                self.current = next(reversed(self.active.values()), None) if self.active else None
                self.last = copy.deepcopy(value)
            due = self.monotonic() - self._last_idle_save >= 30
            if value['faults'] or value['request_count'] or source != 'tick' or due:
                self._persist(value)
                self._last_idle_save = self.monotonic()

    @contextmanager
    def stage(self, name):
        value = getattr(self.local, 'operation', None)
        if value is None:
            yield
            return
        started = self.monotonic()
        started_utc = self._utc()
        self.local.stages.append(name)
        with self.lock:
            if value['id'] in self.active:
                self.active[value['id']]['phase'] = name
        try:
            yield
        except BaseException as exc:
            self.record_fault(exc)
            raise
        finally:
            if len(value['stages']) < 80:
                value['stages'].append({'name': name,
                    'started_utc': started_utc, 'ended_utc': self._utc(),
                    'duration_ms': round(max(0, self.monotonic() - started) * 1000)})
            else:
                self.dropped_details += 1
            self.local.stages.pop()
            with self.lock:
                if value['id'] in self.active:
                    self.active[value['id']]['phase'] = self.local.stages[-1] if self.local.stages else value['source']

    def record_fault(self, error=None, kind=None):
        value = getattr(self.local, 'operation', None)
        if value is not None and len(value['faults']) < 20:
            value['faults'].append(dict(error_fields(error, kind),
                phase=self.local.stages[-1] if self.local.stages else value['source']))

    @contextmanager
    def request(self, method, path, signed):
        with self.operation('exchange_read' if method == 'GET' else 'exchange_write') as operation:
            value = {'method': method if method in {'GET', 'POST', 'DELETE'} else 'unsupported',
                'path': path if path in PATHS else 'other_fapi_endpoint', 'signed': bool(signed),
                'attempt_count': 0, 'wire_boundary_count': 0, 'last_phase': 'preflight',
                'stage_path': list(self.local.stages),
                'started_utc': self._utc(), 'submission_may_have_occurred': False}
            started = self.monotonic()
            self.local.requests.append(value)
            try:
                yield value
            except BaseException as exc:
                value.update(error_fields(exc))
                if value['definite_not_submitted']:
                    value['submission_may_have_occurred'] = False
                raise
            finally:
                value['duration_ms'] = round(max(0, self.monotonic() - started) * 1000)
                value['ended_utc'] = self._utc()
                self.local.requests.pop()
                operation['request_count'] += 1
                if len(operation['requests']) < 80:
                    operation['requests'].append(value)
                else:
                    self.dropped_details += 1

    def request_phase(self, name, *, new_attempt=False):
        stack = getattr(self.local, 'requests', [])
        if stack:
            stack[-1]['last_phase'] = name
            stack[-1]['attempt_count'] += int(new_attempt)

    def wire(self):
        stack = getattr(self.local, 'requests', [])
        if stack:
            value = stack[-1]
            value['last_phase'] = 'network_call'
            value['wire_boundary_count'] += 1
            if value['method'] != 'GET':
                value['submission_may_have_occurred'] = True
                self.local.operation['write_boundary_count'] += 1
            # Evidence means the transport boundary was entered, never that
            # Binance received or accepted a request. Persist before the call
            # so a process exit leaves bounded, explicitly unfinished evidence.
            operation = self.local.operation
            pending = dict(operation, outcome='in_progress',
                active_phase=value['last_phase'], active_stages=list(self.local.stages),
                active_request=copy.deepcopy(value))
            self._persist(pending)

    def response_status(self, status, code=None):
        stack = getattr(self.local, 'requests', [])
        if stack:
            stack[-1]['http_status'] = status if type(status) is int else None
            responses = stack[-1].setdefault('responses', [])
            if len(responses) < 8:
                responses.append({'status': status if type(status) is int else None,
                                  'code': code if type(code) is int else None})

    def observe_account(self, raw):
        def boolean(name):
            value = raw.get(name)
            return True if value is True or value == 'true' else False if value is False or value == 'false' else None
        values = {name: boolean(name) for name in ('canTrade', 'dualSidePosition', 'multiAssetsMargin')}
        compatibility = ('unknown' if any(v is None for v in values.values()) else
                         'compatible' if values == {'canTrade': True, 'dualSidePosition': False, 'multiAssetsMargin': False}
                         else 'incompatible')
        with self.lock:
            self.notes['account_modes'] = dict(values, compatibility=compatibility, observed_utc=self._utc())

    def observe_wallet_fee(self, raw):
        """Reuse existing account GET; no added network operation or fee rule."""
        tier = raw.get('feeTier')
        tier = tier if type(tier) is int and 0 <= tier <= 100 else None
        burn = raw.get('feeBurn')
        burn = burn if type(burn) is bool else None
        with self.lock:
            self.notes['account_fee_profile'] = {'fee_tier': tier, 'fee_burn': burn,
                'source': 'existing_account_get', 'rate_source': 'explicit_commission_get_or_actual_fill',
                'observed_utc': self._utc()}

    def observe_symbols(self, rows):
        with self.lock:
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, dict) or row.get('symbol') not in {'BTCUSDT', 'ETHUSDT'}:
                    continue
                leverage = row.get('leverage')
                text = str(leverage)
                leverage = int(text) if len(text) <= 3 and text.isascii() and text.isdigit() and type(leverage) is not bool else None
                margin = str(row.get('marginType', '')).upper()
                margin = margin if margin in {'CROSSED', 'ISOLATED'} else None
                self.notes['mode_' + row['symbol']] = {'leverage': leverage, 'margin_type': margin,
                    'compatibility': ('unknown' if leverage is None or margin is None else
                                      'compatible' if leverage == 50 and margin == 'CROSSED' else 'incompatible'),
                    'observed_utc': self._utc()}

    def observe_fee(self, row):
        if row.get('symbol') not in {'BTCUSDT', 'ETHUSDT'}:
            return
        asset = row.get('commissionAsset')
        result = {'asset': asset if asset in {'USDT', 'BNB', 'BTC', 'ETH'} else 'other',
            'source': 'actual_fill', 'rate_is_account_tier': False, 'observed_utc': self._utc()}
        try:
            fee = Decimal(str(row['commission']))
            if not fee.is_finite():
                raise ValueError
            result['actual_commission'] = str(fee)
            quote = Decimal(str(row.get('quoteQty', '0')))
            result['denominator_source'] = 'quoteQty'
            if not quote.is_finite() or quote <= 0:
                quote = Decimal(str(row['qty'])) * Decimal(str(row['price']))
                result['denominator_source'] = 'qty_times_price'
            result['observed_rate'] = str(fee / quote) if asset == 'USDT' and quote.is_finite() and quote > 0 else None
            result['comparable'] = result['observed_rate'] is not None
        except (KeyError, ValueError, InvalidOperation, ArithmeticError):
            result.update(observed_rate=None, comparable=False)
        with self.lock:
            self.notes['last_fee_' + row['symbol']] = result

    def observe_account_fee(self, raw):
        if raw.get('symbol') not in {'BTCUSDT', 'ETHUSDT'}:
            return
        value = {'source': 'commission_rate_get', 'observed_utc': self._utc()}
        for field in ('makerCommissionRate', 'takerCommissionRate'):
            try:
                number = Decimal(str(raw[field]))
                value[field] = str(number) if number.is_finite() and 0 <= number < 1 else None
            except (KeyError, InvalidOperation):
                value[field] = None
        with self.lock:
            self.notes['account_fee_' + raw['symbol']] = value

    def _persist(self, value):
        try:
            with closing(sqlite3.connect(self.path, timeout=.01)) as db, db:
                db.executescript('CREATE TABLE IF NOT EXISTS processes(id TEXT PRIMARY KEY,value TEXT);'
                                 'CREATE TABLE IF NOT EXISTS operations(seq INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT);')
                process = {'version': VERSION, 'id': self.process_id, 'pid': self.pid,
                           'started_utc': self.started_utc, 'release': self.release}
                db.execute('INSERT OR IGNORE INTO processes VALUES(?,?)',
                           (self.process_id, json.dumps(process, allow_nan=False)))
                with self.lock:
                    row = dict(value, observations=copy.deepcopy(self.notes))
                db.execute('INSERT INTO operations(value) VALUES(?)',
                           (json.dumps(row, allow_nan=False, ensure_ascii=False),))
                db.execute('DELETE FROM operations WHERE seq NOT IN '
                           '(SELECT seq FROM operations ORDER BY seq DESC LIMIT ?)', (self.max_records,))
                db.execute('DELETE FROM processes WHERE rowid NOT IN '
                           '(SELECT rowid FROM processes ORDER BY rowid DESC LIMIT ?)', (self.max_records,))
        except Exception:
            self.persist_errors += 1

    def snapshot(self):
        with self.lock:
            return {'version': VERSION, 'process_id': self.process_id, 'pid': self.pid,
                'started_utc': self.started_utc, 'release': self.release,
                'persist_errors': self.persist_errors, 'dropped_details': self.dropped_details,
                'current': copy.deepcopy(self.current), 'last': copy.deepcopy(self.last),
                'active_operations': copy.deepcopy(list(self.active.values())),
                'observations': copy.deepcopy(self.notes)}
