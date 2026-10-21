"""Public USD-M WebSocket price and closed-hour cache; never sends orders.

Two combined sockets follow Binance's public/market stream routing. The
public quote source is a complete five-level snapshot at a 500ms cadence.
Cached quotes expire after three seconds; closed hours require the exchange x flag.
Each failed/disconnected channel retries every ten minutes until connected;
the stop event interrupts that wait without blocking the console thread.
Threads start only via explicit start(), construction/import are inert.
"""
import copy
import json
import ssl
import threading
import time
import uuid
from decimal import Decimal

SYMBOLS = ('BTCUSDT', 'ETHUSDT')
HOUR = 3600000
NETWORK_RETRY_SECONDS = 600
NETWORK_RETRY_VERSION = 'ten-minute-retry-v1'
STREAM_DIAGNOSTICS_VERSION = 'safe-stream-diagnostics-v2'
STREAM_OPTIMIZATION_VERSION = 'partial-depth5-500ms-v3'
STREAM_CLOCK_VERSION = 'calibrated-event-monotonic-ttl-v1'
PUBLIC_QUOTE_SUFFIX = 'depth5@500ms'


def _safe_exception(exc, source):
    def number(value):
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else None
    return {'source': source, 'exception_type': type(exc).__name__,
            'errno': number(getattr(exc, 'errno', None)),
            'winerror': number(getattr(exc, 'winerror', None))}


def connection_failure(exc, *, clock, socket=None, phase=None,
                       connection_duration_seconds=None):
    """Capture static evidence, never exception URLs, payloads or credentials."""
    received, sent = getattr(exc, 'rcvd', None), getattr(exc, 'sent', None)
    def code(value):
        return (int(value) if isinstance(value, int) and not isinstance(value, bool)
                and 1000 <= value <= 4999 else None)
    received_code, sent_code = code(getattr(received, 'code', None)), code(getattr(sent, 'code', None))
    close_code = received_code or sent_code
    # Sync recv() wraps transport errors; parser errors can be hidden by
    # "raise ... from None". Capture both explicitly, without formatting them.
    causes, errors, visited = [], [], set()
    current = exc
    for depth in range(4):
        if not isinstance(current, BaseException) or id(current) in visited:
            break
        visited.add(id(current))
        errors.append(current)
        causes.append(_safe_exception(current, 'exception' if depth == 0 else 'cause'))
        current = (current.__cause__ if current.__cause__ is not None else
                   (None if current.__suppress_context__ else current.__context__))
    protocol = getattr(socket, 'protocol', None)
    for source, error in (('recv_exc', getattr(socket, 'recv_exc', None)),
                          ('parser_exc', getattr(protocol, 'parser_exc', None))):
        if isinstance(error, BaseException):
            errors.append(error)
            causes.append(_safe_exception(error, source))
    # The library's exact static reason identifies its own keepalive failure.
    # Never stringify exceptions or retain any peer/local reason text.
    if getattr(sent, 'reason', None) == 'keepalive ping timeout':
        kind = 'ping_timeout'
    elif any(isinstance(error, ssl.SSLEOFError) for error in errors):
        kind = 'tls_eof'
    elif any(isinstance(error, ssl.SSLError) for error in errors):
        kind = 'tls_error'
    elif any(isinstance(error, ConnectionResetError) for error in errors):
        kind = 'connection_reset'
    elif any(isinstance(error, EOFError) for error in errors):
        kind = 'transport_eof'
    elif any(isinstance(error, TimeoutError) for error in errors):
        kind = 'connect_or_read_timeout'
    elif any(isinstance(error, (ValueError, UnicodeError)) for error in errors):
        kind = 'payload_decode_error'
    elif close_code is not None:
        kind = 'connection_closed'
    else:
        kind = 'connection_error'
    return {'exception_type': type(exc).__name__, 'kind': kind, 'close_code': close_code,
            'received_close_code': received_code, 'sent_close_code': sent_code,
            'received_close_frame': received is not None, 'sent_close_frame': sent is not None,
            'exception_chain': causes, 'phase': phase,
            'connection_duration_seconds': connection_duration_seconds,
            'occurred_at_ms': int(clock() * 1000)}


class MarketStreams:
    def __init__(self, *, clock=time.time, connect=None, monotonic=time.monotonic,
                 stop_event=None, exchange_clock_ms=None):
        self.clock, self.connect = clock, connect
        self.monotonic = monotonic
        self.exchange_clock_ms = exchange_clock_ms
        self._age_lower_floor = None
        self._event_watermarks = {}
        self.lock = threading.RLock()
        self.stop_event = stop_event if stop_event is not None else threading.Event()
        self.cache, self.closed, self.connections, self.threads = {}, {}, {}, []
        self.connected = {'public': False, 'market': False}
        self.retry = {channel: {'failures': 0, 'deadline': None}
                      for channel in ('public', 'market')}
        self.diagnostics = {channel: {'connection_attempts': 0, 'messages_received': 0,
            'invalid_messages': 0, 'ignored_unclosed_klines': 0,
            'last_message_at_ms': None, 'last_connected_at_ms': None,
            'last_failure': None, 'queue_peak_frames': 0, 'receive_paused_observed': False,
            'attempt_queue_peak_frames': 0, 'attempt_receive_paused_observed': False,
            'last_event_lag_ms': None, 'attempt_event_lag_peak_ms': None,
            'compression_negotiated': None, 'phase': 'idle',
            'last_connection_duration_seconds': None, 'last_disconnected_at_ms': None}
            for channel in ('public', 'market')}
        self.connected_since = {channel: None for channel in ('public', 'market')}
        self._retry_wake = {channel: threading.Event() for channel in ('public', 'market')}
        self._manual_reconnect = {channel: None for channel in ('public', 'market')}

    def _event_times(self, mono):
        if self.exchange_clock_ms is None:
            value = round(self.clock() * 1000)
            return value, value, value
        bounds = self.exchange_clock_ms()
        if (not isinstance(bounds, tuple) or len(bounds) != 2 or
                any(type(v) is not int for v in bounds) or bounds[1] < bounds[0]):
            return None
        lower, upper = bounds
        with self.lock:
            previous = self._age_lower_floor
            floor = lower
            reference_mono = mono
            if previous is not None:
                # A thread may sample before another thread and acquire the
                # lock after it. Never move the shared reference backwards.
                reference_mono = max(mono, previous[1])
                floor = max(lower, previous[0] + round(max(0, mono - previous[1]) * 1000))
            # Advance a lower bound, not RTT uncertainty: a slow old calibration
            # must not pin all new quotes in the future after RTT improves.
            self._age_lower_floor = (floor, reference_mono)
        return lower, upper, max(upper, floor)

    def start(self):
        with self.lock:
            self.threads = [task for task in self.threads if task.is_alive()]
            if self.threads:
                return  # A connecting thread may still be finishing stop().
            self.stop_event.clear()
            for wake in self._retry_wake.values():
                wake.clear()
            for channel in ('public', 'market'):
                task = threading.Thread(target=self._run, args=(channel,), daemon=True,
                    name='binance-' + channel + '-stream')
                self.threads.append(task)
                task.start()

    def stop(self):
        self.stop_event.set()
        for wake in self._retry_wake.values():
            wake.set()
        with self.lock:
            sockets = list(self.connections.values())
        for connection in sockets:
            try:
                connection.close()
            except Exception:
                pass
        for task in self.threads:
            task.join(timeout=2)
        self.threads = [task for task in self.threads if task.is_alive()]
        with self.lock:
            for retry in self.retry.values():
                retry['deadline'] = None
            for request in self._manual_reconnect.values():
                if request and request['state'] in {'pending', 'connecting'}:
                    request['state'] = 'stopped'

    def request_reconnect(self, channels=None, *, deadline_seconds=30):
        """Wake only disconnected retry loops; never creates or closes a socket.

        Repeat requests merge into the pending attempt. The existing thread is
        the sole connection owner, including the automatic/manual boundary.
        """
        selected = ('public', 'market') if channels is None else tuple(channels)
        if any(channel not in self.connected for channel in selected):
            raise ValueError('仅支持public和market行情通道')
        seconds = float(deadline_seconds)
        if not 0 < seconds <= 120:
            raise ValueError('行情检查期限必须大于0且不超过120秒')
        result = {}
        with self.lock:
            now = self.monotonic()
            for channel in dict.fromkeys(selected):
                request = self._manual_reconnect[channel]
                if self.stop_event.is_set():
                    result[channel] = {'state': 'stopped'}
                elif self.connected[channel]:
                    result[channel] = {'state': 'healthy'}
                elif request and request['state'] in {'pending', 'connecting'}:
                    result[channel] = dict(request, merged=True)
                elif self.diagnostics[channel]['phase'] == 'connect':
                    result[channel] = {'state': 'connecting', 'merged': True}
                elif self.diagnostics[channel]['phase'] != 'retry_wait':
                    result[channel] = {'state': 'not_running'}
                else:
                    request = {'id': uuid.uuid4().hex, 'state': 'pending',
                               'deadline': now + seconds, 'requested_at_ms': int(self.clock() * 1000),
                               'original_retry_deadline': self.retry[channel]['deadline']}
                    self._manual_reconnect[channel] = request
                    result[channel] = dict(request, merged=False)
                    self._retry_wake[channel].set()
        return result

    def _wait_for_retry(self, channel):
        """An interruptible monotonic wait also tolerates early/spurious wakeups."""
        while not self.stop_event.is_set():
            with self.lock:
                deadline = self.retry[channel]['deadline']
                request = self._manual_reconnect[channel]
                if request and request['state'] == 'pending':
                    if request['deadline'] > self.monotonic():
                        request['state'] = 'connecting'
                        self._retry_wake[channel].clear()
                        return True
                    request['state'] = 'expired'
            remaining = 0 if deadline is None else deadline - self.monotonic()
            if remaining <= 0:
                return True
            if isinstance(self.stop_event, threading.Event):
                # The wake is channel-specific; stopping one wait must not tell
                # the healthy other channel to stop. A bounded wait also handles
                # callers that set the supplied stop event directly.
                if self.stop_event.wait(0):
                    return False
                self._retry_wake[channel].wait(min(remaining, .5))
                self._retry_wake[channel].clear()
            elif self.stop_event.wait(remaining):
                return False  # Preserve injectable deterministic wait adapters.
        return False

    def _run(self, channel):
        if self.connect is None:
            from websockets.sync.client import connect
            connector = connect
        else:
            connector = self.connect
        suffixes = (PUBLIC_QUOTE_SUFFIX,) if channel == 'public' else ('aggTrade', 'markPrice@1s', 'kline_1h')
        streams = '/'.join(symbol.lower() + '@' + suffix for symbol in SYMBOLS for suffix in suffixes)
        uri = 'wss://fstream.binance.com/' + channel + '/stream?streams=' + streams
        while not self.stop_event.is_set():
            socket, connected_at, phase = None, None, 'connect'
            with self.lock:
                request = self._manual_reconnect[channel]
                manual = request if request and request['state'] == 'connecting' else None
            try:
                with self.lock:
                    diagnostic = self.diagnostics[channel]
                    diagnostic['connection_attempts'] += 1
                    diagnostic.update(attempt_queue_peak_frames=0,
                        attempt_receive_paused_observed=False, last_event_lag_ms=None,
                        attempt_event_lag_peak_ms=None, compression_negotiated=None, phase='connect')
                open_timeout = 12
                if manual is not None:
                    remaining = manual['deadline'] - self.monotonic()
                    if remaining <= 0:
                        raise TimeoutError('manual reconnect deadline expired')
                    open_timeout = min(open_timeout, remaining)
                with connector(uri, open_timeout=open_timeout, ping_interval=20, ping_timeout=20, max_queue=64) as socket:
                    if (self.stop_event.is_set() or
                            (manual is not None and self.monotonic() >= manual['deadline'])):
                        raise TimeoutError('manual reconnect no longer current')
                    connected_at = self.monotonic()
                    protocol = getattr(socket, 'protocol', None)
                    extensions = getattr(protocol, 'extensions', None)
                    compression = (any(getattr(extension, 'name', None) == 'permessage-deflate'
                                       for extension in extensions)
                                   if isinstance(extensions, (list, tuple)) else None)
                    with self.lock:
                        self.connections[channel], self.connected[channel] = socket, True
                        self.connected_since[channel] = connected_at
                        self.retry[channel] = {'failures': 0, 'deadline': None}
                        self.diagnostics[channel].update(last_connected_at_ms=int(self.clock() * 1000),
                                                         compression_negotiated=compression, phase='receive')
                        if manual is not None:
                            manual['state'] = 'connected'
                    while not self.stop_event.is_set():
                        phase = 'receive'
                        try:
                            payload = socket.recv(timeout=2)
                        except TimeoutError:
                            continue
                        phase = 'decode'
                        decoded = json.loads(payload)
                        phase = 'ingest'
                        accepted = self.ingest(decoded)
                        now_ms = int(self.clock() * 1000)
                        event_times = self._event_times(self.monotonic())
                        data = decoded.get('data', decoded) if isinstance(decoded, dict) else None
                        event_ms = data.get('E') if isinstance(data, dict) else None
                        lag_ms = (event_times[2] - event_ms if
                                  event_times is not None and type(event_ms) is int else None)
                        kline = data.get('k') if isinstance(data, dict) else None
                        unclosed_hour = (not accepted and isinstance(kline, dict)
                            and data.get('e') == 'kline' and data.get('s') in SYMBOLS
                            and data.get('st', 1) == 1 and kline.get('i') == '1h'
                            and kline.get('s', data['s']) == data['s'] and kline.get('x') is False
                            and lag_ms is not None and abs(lag_ms) <= 5000)
                        with self.lock:
                            diagnostic = self.diagnostics[channel]
                            diagnostic['messages_received'] += 1
                            diagnostic['invalid_messages'] += int(not accepted)
                            # Retain the historical counter contract; expose its
                            # expected unclosed-hour subset separately for readers.
                            diagnostic['ignored_unclosed_klines'] += int(unclosed_hour)
                            diagnostic['last_message_at_ms'] = now_ms
                            if lag_ms is not None:
                                diagnostic['last_event_lag_ms'] = lag_ms
                                peak = diagnostic['attempt_event_lag_peak_ms']
                                diagnostic['attempt_event_lag_peak_ms'] = lag_ms if peak is None else max(peak, lag_ms)
                            # Optional, version-tolerant observation of bounded
                            # receive pressure; never disable queue limits or replay old quotes.
                            assembler = getattr(socket, 'recv_messages', None)
                            frames = getattr(assembler, 'frames', None)
                            if callable(getattr(frames, 'qsize', None)):
                                depth = frames.qsize()
                                if type(depth) is int and depth >= 0:
                                    diagnostic['queue_peak_frames'] = max(diagnostic['queue_peak_frames'], depth)
                                    diagnostic['attempt_queue_peak_frames'] = max(diagnostic['attempt_queue_peak_frames'], depth)
                            paused = getattr(assembler, 'paused', False) is True
                            diagnostic['receive_paused_observed'] |= paused
                            diagnostic['attempt_receive_paused_observed'] |= paused
                    phase = 'close'
            except Exception as exc:
                if not self.stop_event.is_set():
                    duration = (round(max(0.0, self.monotonic() - connected_at), 3)
                                if connected_at is not None else None)
                    failure = connection_failure(exc, clock=self.clock, socket=socket, phase=phase,
                                                 connection_duration_seconds=duration)
                    with self.lock:
                        diagnostic = self.diagnostics[channel]
                        failure.update(queue_peak_frames=diagnostic['attempt_queue_peak_frames'],
                            receive_paused_observed=diagnostic['attempt_receive_paused_observed'],
                            event_lag_peak_ms=diagnostic['attempt_event_lag_peak_ms'],
                            compression_negotiated=diagnostic['compression_negotiated'])
                        diagnostic['last_failure'] = failure
                        if manual is not None and manual['state'] == 'connecting':
                            manual['state'] = ('expired' if self.monotonic() >= manual['deadline'] else 'failed')
            finally:
                with self.lock:
                    if connected_at is not None:
                        self.diagnostics[channel].update(
                            last_connection_duration_seconds=round(max(0.0, self.monotonic() - connected_at), 3),
                            last_disconnected_at_ms=int(self.clock() * 1000))
                    self.connected[channel] = False
                    self.connected_since[channel] = None
                    self.connections.pop(channel, None)
                    self.diagnostics[channel]['phase'] = 'stopped' if self.stop_event.is_set() else 'retry_wait'
            if self.stop_event.is_set():
                break
            with self.lock:
                retry = self.retry[channel]
                retry['failures'] += 1
                retry['deadline'] = (manual['original_retry_deadline']
                    if manual is not None and connected_at is None
                    and manual['original_retry_deadline'] is not None
                    else self.monotonic() + NETWORK_RETRY_SECONDS)
            if not self._wait_for_retry(channel):
                break

    def ingest(self, payload):
        if not isinstance(payload, dict):
            return False
        data = payload.get('data', payload)
        if not isinstance(data, dict) or data.get('s') not in SYMBOLS or data.get('st', 1) != 1:
            return False
        mono = self.monotonic()
        times = self._event_times(mono)
        if times is None:
            return False
        lower_ms, upper_ms, age_upper_ms = times
        now, symbol, kind = lower_ms / 1000, data['s'], data.get('e')
        try:
            event_ms = int(data['E'])
            if age_upper_ms - event_ms > 5000 or event_ms - lower_ms > 5000:
                return False
            def positive(value):
                d = Decimal(str(value))
                if not d.is_finite() or d <= 0:
                    raise ValueError('invalid price')
                return str(d)
            if kind == 'depthUpdate':
                # Only a complete subscribed partial-depth snapshot can provide
                # best prices. Diff-depth updates require a separate synced book.
                if (payload.get('stream') != symbol.lower() + '@' + PUBLIC_QUOTE_SUFFIX
                        or type(data.get('st')) is not int or data['st'] != 1
                        or type(data.get('E')) is not int
                        or age_upper_ms - event_ms > 3000 or event_ms - lower_ms > 3000):
                    return False
                def complete_side(levels, *, descending):
                    if not isinstance(levels, list) or len(levels) != 5:
                        raise ValueError('incomplete partial depth')
                    prices = []
                    for level in levels:
                        if not isinstance(level, list) or len(level) != 2:
                            raise ValueError('invalid partial depth level')
                        price = Decimal(positive(level[0]))
                        positive(level[1])
                        if prices and not (price < prices[-1] if descending else price > prices[-1]):
                            raise ValueError('unsorted or duplicate partial depth')
                        prices.append(price)
                    return prices[0]
                bid = complete_side(data.get('b'), descending=True)
                ask = complete_side(data.get('a'), descending=False)
                if bid > ask:
                    return False
                target, value = 'book', {'symbol': symbol, 'bidPrice': str(bid), 'askPrice': str(ask)}
            elif kind == 'bookTicker':
                bid, ask = positive(data['b']), positive(data['a'])
                if Decimal(bid) > Decimal(ask):
                    return False
                target, value = 'book', {'symbol': symbol, 'bidPrice': bid, 'askPrice': ask}
            elif kind == 'aggTrade':
                target, value = 'trade', {'symbol': symbol, 'price': positive(data['p'])}
            elif kind == 'markPriceUpdate':
                target, value = 'mark', {'symbol': symbol, 'markPrice': positive(data['p'])}
            elif kind == 'kline':
                k = data['k']
                if k.get('x') is not True or k.get('i') != '1h' or k.get('s', symbol) != symbol:
                    return False
                opened, closed = int(k['t']), int(k['T'])
                if opened % HOUR or closed != opened + HOUR - 1 or closed >= int(now * 1000):
                    return False
                o, h, low, c = [positive(k[name]) for name in ('o', 'h', 'l', 'c')]
                if Decimal(low) > min(Decimal(o), Decimal(c)) or Decimal(h) < max(Decimal(o), Decimal(c)):
                    return False
                row = [opened, o, h, low, c, k.get('v', '0'), closed,
                       k.get('q', '0'), k.get('n', 0), k.get('V', '0'), k.get('Q', '0'), '0']
                with self.lock:
                    history = self.closed.setdefault(symbol, {})
                    if opened in history and history[opened] != row:
                        return False
                    history[opened] = row
                    for old in sorted(history)[:-48]:
                        del history[old]
                return True
            else:
                return False
        except (KeyError, ValueError, TypeError, ArithmeticError):
            return False
        with self.lock:
            key = (target, symbol)
            old = self.cache.get(key)
            if old and old['event_ms'] > event_ms:
                return False
            if self.exchange_clock_ms is not None:
                watermark = self._event_watermarks.get(key)
                if watermark is not None and event_ms <= watermark and old is None:
                    return False
                if old and old['event_ms'] == event_ms:
                    # A duplicate event cannot reset its receive age. Keep a
                    # tombstone if it is already too old to provide a quote.
                    elapsed = mono - old['seen_monotonic']
                    if (elapsed < 0 or elapsed > 3 or
                            max(age_upper_ms - event_ms,
                                old['received_age_ms'] + elapsed * 1000) > 3000):
                        self.latest(target, symbol)  # Retain explicit expiry evidence.
                        self.cache.pop(key, None)
                        return False
                    old['value'] = value
                    return True
                self._event_watermarks[key] = event_ms
            self.cache[key] = {'event_ms': event_ms, 'seen': now,
                'seen_monotonic': mono, 'received_age_ms': age_upper_ms - event_ms, 'value': value}
        return True

    def latest(self, kind, symbol, *, max_age=3):
        mono = self.monotonic()
        times = self._event_times(mono)
        if times is None:
            with self.lock:
                if not hasattr(self, '_freshness'):
                    self._freshness = {}
                self._freshness[(kind, symbol)] = {'available': False, 'reason': 'clock_uncalibrated'}
            return None
        with self.lock:
            key = (kind, symbol)
            if not hasattr(self, '_freshness'):
                self._freshness = {}
            item = self.cache.get(key)
            if not item:
                previous = self._freshness.get(key) or {}
                if previous.get('available') or previous.get('reason') == 'clock_uncalibrated':
                    self._freshness[key] = {'available': False, 'reason': 'no_message'}
                else:
                    self._freshness.setdefault(key, {'available': False, 'reason': 'no_message'})
                return None
            elapsed = mono - item['seen_monotonic']
            age = max(times[2] - item['event_ms'], item['received_age_ms'] + elapsed * 1000)
            reasons = []
            if elapsed < 0:
                reasons.append('monotonic_epoch_changed')
            if elapsed > max_age:
                reasons.append('receive_ttl_expired')
            if age > max_age * 1000:
                reasons.append('event_age_expired')
            if item['event_ms'] - times[0] > max_age * 1000:
                reasons.append('event_in_future')
            self._freshness[key] = {'available': not reasons,
                'reason': reasons[0] if reasons else None, 'reasons': reasons,
                'receive_age_ms': round(elapsed * 1000), 'event_age_upper_ms': round(age),
                'event_ms': item['event_ms']}
            if reasons:
                if self.exchange_clock_ms is not None:
                    self.cache.pop(key, None)
                return None
            return copy.deepcopy(item['value'])

    def entry_book_observation(self, symbol):
        """Freeze one quote's age proof; a later quote cannot refresh this one."""
        with self.lock:
            value = self.latest('book', symbol, max_age=3)
            if value is None:
                return None
            item = dict(self.cache[('book', symbol)])
            observed = self.monotonic()
            times = self._event_times(observed)
            if times is None:
                return None
            receive_age = observed - item['seen_monotonic']
            event_age = max(times[2] - item['event_ms'],
                            item['received_age_ms'] + receive_age * 1000)

        def is_fresh():
            now = self.monotonic()
            elapsed = now - observed
            times = self._event_times(now)
            return bool(times is not None and elapsed >= 0 and receive_age + elapsed < 3 and
                max(event_age + elapsed * 1000, times[2] - item['event_ms']) < 3000 and
                item['event_ms'] - times[0] <= 3000)

        return value, is_fresh

    def closed_since(self, symbol, start_ms, boundary_ms):
        with self.lock:
            history = self.closed.get(symbol, {})
            keys = list(range(int(start_ms), int(boundary_ms), HOUR))
            if not keys or any(key not in history for key in keys):
                return []
            return [copy.deepcopy(history[key]) for key in keys]

    def snapshot(self):
        with self.lock:
            now = self.monotonic()
            retry = {}
            for channel, state in self.retry.items():
                remaining = (max(0.0, state['deadline'] - now)
                             if state['deadline'] is not None and not self.stop_event.is_set() else 0.0)
                retry[channel] = {'failures': state['failures'],
                                  'waiting': remaining > 0,
                                  'retry_in_seconds': remaining,
                                  'next_retry_at_ms': (int((self.clock() + remaining) * 1000)
                                                       if remaining > 0 else None)}
            diagnostics = copy.deepcopy(self.diagnostics)
            for channel, diagnostic in diagnostics.items():
                started = self.connected_since[channel]
                diagnostic['connection_duration_seconds'] = (
                    round(max(0.0, now - started), 3) if started is not None else
                    diagnostic['last_connection_duration_seconds'])
            return {'network_retry_version': NETWORK_RETRY_VERSION,
                'clock_version': STREAM_CLOCK_VERSION,
                'event_clock_calibrated': (self._event_times(now) is not None
                                           if self.exchange_clock_ms is not None else None),
                'diagnostics_version': STREAM_DIAGNOSTICS_VERSION,
                'stream_optimization_version': STREAM_OPTIMIZATION_VERSION,
                'public_quote_source': PUBLIC_QUOTE_SUFFIX,
                'diagnostics': diagnostics,
                'retry_interval_seconds': NETWORK_RETRY_SECONDS,
                'retry': retry,
                'manual_reconnect': {channel: copy.deepcopy(request)
                                     for channel, request in self._manual_reconnect.items()},
                'connections': dict(self.connected), 'symbols': {symbol: {
                'book_live': self.latest('book', symbol) is not None,
                'trade_live': self.latest('trade', symbol) is not None,
                'mark_live': self.latest('mark', symbol) is not None,
                'latest_trade': self.latest('trade', symbol),
                'freshness': {kind: copy.deepcopy(self._freshness.get((kind, symbol),
                    {'available': False, 'reason': 'no_message'})) for kind in ('book', 'trade', 'mark')},
                'closed_hours_cached': len(self.closed.get(symbol, {}))} for symbol in SYMBOLS}}
