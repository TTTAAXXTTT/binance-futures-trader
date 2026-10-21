"""Request deadlines, exception cleanup and local HTTP fixtures; no remote traffic."""
import hashlib
import hmac
import json
import ssl
import sys
import threading
import time
import unittest
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests'), str(ROOT / 'runtime/packages')]
import urllib3
from execution import binance
from execution.binance import ApiError, BinanceClient, OrderUncertain, PooledHTTPTransport, RequestExpired
from test_hourly_pool_transport import Pools, WireResponse


class Clock:
    def __init__(self):
        self.now = 6000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class AdvancingLock:
    """One deterministic local slot wait, without delaying the test process."""
    def __init__(self, clock, seconds, observe=None):
        self.lock, self.clock, self.seconds = threading.RLock(), clock, seconds
        self.observe = observe
        self.first = True

    def acquire(self, *args, **kwargs):
        acquired = self.lock.acquire(*args, **kwargs)
        if acquired and self.first:
            self.first = False
            if self.observe:
                self.observe()
            self.clock.advance(self.seconds)
        return acquired

    def release(self):
        self.lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_):
        self.release()


class DeadlineContractTests(unittest.TestCase):
    def test_expired_ssl_shutdown_valueerror_is_uncertain_write_not_programming_fault(self):
        finished = threading.Event()
        response = WireResponse({'ok': True})
        response._sock_shutdown = lambda _: finished.set()
        def interrupted_read():
            if not finished.wait(1):
                raise AssertionError('deadline watchdog failed to wake SSL read')
            raise ValueError('Read on closed or unwrapped SSL socket')
        response.read = interrupted_read
        pools = Pools(response)
        _, transport, client = self.make(pools, timeout=.05)
        with self.assertRaises(OrderUncertain) as caught:
            client.signed('POST', '/fapi/v1/order', {'newClientOrderId': 'original-offline'})
        self.assertFalse(caught.exception.definite_not_submitted)
        self.assertEqual(caught.exception.client_id, 'original-offline')
        self.assertEqual(len(pools.calls), 1)
        self.assertTrue(client.network_status()['waiting'])
        self.assertEqual(transport.snapshot()['active_pools'], 0)
        self.assert_lock_released(transport)

    def make(self, pools, *, timeout=12):
        clock = Clock()
        transport = PooledHTTPTransport(pool_factory=pools.factory, proxy_factory=pools.factory,
            proxy_getter=lambda: {}, bypass=lambda _: True, clock=clock)
        client = BinanceClient('offline-key', 'offline-secret', transport=transport,
            timeout=timeout, auto_sync_time=False, clock=clock, monotonic=clock)
        return clock, transport, client

    def assert_lock_released(self, transport):
        observed = []
        def check():
            acquired = transport._lock.acquire(timeout=.5)
            observed.append(acquired)
            if acquired:
                transport._lock.release()
        reader = threading.Thread(target=check, daemon=True)
        reader.start()
        reader.join(timeout=1)
        self.assertFalse(reader.is_alive(), 'cleanup must release the transport slot')
        self.assertEqual(observed, [True])

    def failing_clear_factory(self, pools):
        original = pools.factory
        def factory(*args, **kwargs):
            pool = original(*args, **kwargs)
            def clear():
                pool.clears += 1
                raise OSError(10054, 'private://proxy TOKEN=private')
            pool.clear = clear
            return pool
        pools.factory = factory

    def test_clear_failure_after_tls_error_removes_manager_releases_slot_and_never_replays_write(self):
        failure = urllib3.exceptions.SSLError(ssl.SSLEOFError('private signed URL'))
        for method in ('POST', 'DELETE'):
            with self.subTest(method=method):
                pools = Pools(failure, WireResponse({'ok': True}))
                self.failing_clear_factory(pools)
                clock, transport, client = self.make(pools)
                with self.assertRaises(OrderUncertain) as caught:
                    client.signed(method, '/fapi/v1/algoOrder', {'clientAlgoId': 'persisted-offline-id'})
                self.assertEqual(len(pools.calls), 1)
                self.assertEqual(caught.exception.client_id, 'persisted-offline-id')
                self.assertFalse(caught.exception.definite_not_submitted)
                self.assert_lock_released(transport)
                self.assertEqual(transport.snapshot()['active_pools'], 0)
                self.assertNotIn('private', str(caught.exception))
                self.assertNotIn('private', json.dumps(transport.snapshot()))
                self.assertEqual(client.network_status()['retry_in_seconds'], 600)
                clock.advance(600)
                self.assertEqual(client.public('GET', '/fapi/v1/time'), {'ok': True})
                self.assertEqual(len(pools.created), 2)

    def test_clear_failure_after_partial_body_keeps_original_error_and_releases_slot(self):
        response = WireResponse({}, failure=urllib3.exceptions.ProtocolError(
            'private URL', IncompleteRead(b'private-body', 500)))
        pools = Pools(response)
        self.failing_clear_factory(pools)
        clock, transport, client = self.make(pools)
        with self.assertRaises(ApiError) as caught:
            client.public('GET', '/fapi/v1/time')
        self.assertIn('不完整', str(caught.exception))
        self.assertNotIn('private', str(caught.exception))
        self.assert_lock_released(transport)
        self.assertEqual(transport.snapshot()['active_pools'], 0)
        self.assertEqual((response.releases, response.closes), (0, 1))
        self.assertEqual(len(pools.calls), 1)

    def test_busy_slot_is_local_deferral_without_network_wait_or_submission(self):
        pools = Pools(WireResponse({'ok': True}))
        clock, transport, client = self.make(pools, timeout=.06)
        entered, release = threading.Event(), threading.Event()
        def hold():
            with transport._lock:
                entered.set()
                release.wait(timeout=3)
        holder = threading.Thread(target=hold, daemon=True)
        holder.start()
        self.assertTrue(entered.wait(timeout=1))
        started = time.monotonic()
        try:
            with self.assertRaises(ApiError) as caught:
                client.signed('POST', '/fapi/v1/order', {'newClientOrderId': 'offline-busy'})
            elapsed = time.monotonic() - started
        finally:
            release.set()
            holder.join(timeout=1)
        self.assertIsInstance(caught.exception, binance.LocalTransportDeferred)
        self.assertEqual(caught.exception.recovery_kind, 'local_budget_wait')
        self.assertTrue(caught.exception.definite_not_submitted)
        self.assertLess(elapsed, 1)
        self.assertEqual(pools.calls, [])
        self.assertFalse(client.network_status()['waiting'])
        self.assertEqual(client.network_status()['failures'], 0)
        self.assertEqual(client.public('GET', '/fapi/v1/time'), {'ok': True})

    def test_signature_is_created_after_slot_wait_and_pool_preparation(self):
        pools = Pools(WireResponse({'ok': True}))
        clock, transport, client = self.make(pools)
        original = pools.factory
        def preparing(*args, **kwargs):
            clock.advance(2)
            return original(*args, **kwargs)
        pools.factory = preparing
        transport.pool_factory = transport.proxy_factory = preparing
        transport._lock = AdvancingLock(clock, 3)
        client.signed('POST', '/fapi/v1/order', {'symbol': 'BTCUSDT', 'newClientOrderId': 'offline-fresh-signature'})
        body = pools.calls[0][2]['body'].decode()
        values = parse_qs(body)
        self.assertEqual(values['timestamp'], ['6005000'])
        self.assertEqual(values['newClientOrderId'], ['offline-fresh-signature'])
        self.assertEqual(len(values['signature']), 1)
        unsigned, signed = body.rsplit('&signature=', 1)
        expected = hmac.new(b'offline-secret', unsigned.encode(), hashlib.sha256).hexdigest()
        self.assertEqual(signed, expected)

    def test_entry_expiring_while_waiting_for_slot_is_not_submitted_or_uncertain(self):
        pools = Pools(WireResponse({'must_not_send': True}))
        clock, transport, client = self.make(pools)
        transport._lock = AdvancingLock(clock, 2)
        with self.assertRaises(RequestExpired) as caught:
            client.request('POST', '/fapi/v1/order', {'newClientOrderId': 'offline-expired'},
                           signed=True, _deadline_ms=6001000)
        self.assertTrue(caught.exception.definite_not_submitted)
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(pools.calls, [])
        self.assertFalse(client.network_status()['waiting'])
        self.assertEqual(transport.snapshot()['wire_attempts'], 0)
        self.assert_lock_released(transport)

    def test_recovery_probe_lease_is_claimed_after_slot_wait(self):
        pools = Pools(WireResponse({'ok': True}))
        clock, transport, client = self.make(pools)
        client._network_retry.failed('offline prior failure')
        clock.advance(600)
        before = []
        transport._lock = AdvancingLock(clock, 5,
            observe=lambda: before.append(client.network_status()['probing']))
        observed = []
        original = pools.factory
        def factory(*args, **kwargs):
            pool = original(*args, **kwargs)
            request = pool.request
            def record(*args, **kwargs):
                observed.append(client.network_status())
                return request(*args, **kwargs)
            pool.request = record
            return pool
        transport.pool_factory = transport.proxy_factory = factory
        self.assertEqual(client.public('GET', '/fapi/v1/time'), {'ok': True})
        self.assertEqual(before, [False])
        self.assertTrue(observed[0]['probing'])
        self.assertEqual(observed[0]['retry_in_seconds'], 30)
        self.assertFalse(client.network_status()['waiting'])

    def test_local_preparation_exhausting_budget_does_not_add_a_new_600_second_wait(self):
        pools = Pools(WireResponse({'must_not_send': True}))
        clock, transport, client = self.make(pools)
        client._network_retry.failed('offline prior failure')
        clock.advance(600)
        original = pools.factory
        def prepare(*args, **kwargs):
            clock.advance(13)
            return original(*args, **kwargs)
        transport.pool_factory = transport.proxy_factory = prepare
        with self.assertRaises(ApiError) as caught:
            client.public('GET', '/fapi/v1/time')
        self.assertIsInstance(caught.exception, binance.LocalTransportDeferred)
        self.assertTrue(caught.exception.definite_not_submitted)
        self.assertEqual(pools.calls, [])
        state = client.network_status()
        self.assertFalse(state['waiting'], 'zero wire attempts must not reset the expired recovery deadline')
        self.assertFalse(state['probing'])
        self.assertEqual(state['failures'], 1)
        self.assert_lock_released(transport)

    def test_time_sync_rtt_excludes_local_slot_wait(self):
        pools = Pools(WireResponse({'serverTime': 6005000}))
        clock, transport, client = self.make(pools)
        transport._lock = AdvancingLock(clock, 5)
        response = pools.results[0]
        read = response.read
        def body():
            clock.advance(1)
            return read()
        response.read = body
        client.sync_time()
        self.assertEqual(client.clock_status()['sync_rtt_ms'], 1000)

    def test_connect_read_total_and_pool_wait_use_one_remaining_budget(self):
        pools = Pools(WireResponse({'ok': True}))
        clock, transport, client = self.make(pools)
        transport._lock = AdvancingLock(clock, 4)
        client.public('GET', '/fapi/v1/time')
        options = pools.calls[0][2]
        self.assertEqual(options['timeout'].total, 8)
        self.assertEqual(options['timeout'].connect_timeout, 8)
        self.assertEqual(options['pool_timeout'], 8)
        self.assertFalse(options['retries'])
        self.assertFalse(options['redirect'])


class LocalDeadlineTests(unittest.TestCase):
    def server(self, peer_close=False):
        requests = []
        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *args):
                pass
            def do_GET(self):
                self.respond()
            def do_POST(self):
                self.respond()
            def do_DELETE(self):
                self.respond()
            def respond(self):
                length = int(self.headers.get('Content-Length', '0'))
                if length:
                    self.rfile.read(length)
                query = parse_qs(urlsplit(self.path).query)
                scenario = query.get('scenario', ['trickle'])[0]
                requests.append((self.command, scenario))
                body = b' ' * 120 + b'{"ok": true}' if scenario == 'trickle' else b'{"ok": true}'
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                if peer_close:
                    self.send_header('Connection', 'close')
                    self.close_connection = True
                self.end_headers()
                try:
                    if scenario == 'delayed':
                        time.sleep(.5)
                    if scenario == 'trickle':
                        for byte in body:
                            self.wfile.write(bytes([byte]))
                            self.wfile.flush()
                            time.sleep(.025)
                    else:
                        self.wfile.write(body)
                        self.wfile.flush()
                except (OSError, ValueError):
                    self.close_connection = True
        class QuietServer(ThreadingHTTPServer):
            daemon_threads = True
            def handle_error(self, *args):
                pass
        server = QuietServer(('127.0.0.1', 0), Handler)
        runner = threading.Thread(target=lambda: server.serve_forever(poll_interval=.02), daemon=True)
        runner.start()
        def close():
            server.shutdown()
            server.server_close()
            runner.join(timeout=1)
        self.addCleanup(close)
        return server, requests

    def client(self, server, timeout):
        transport = PooledHTTPTransport(proxy_getter=lambda: {}, bypass=lambda _: True)
        client = BinanceClient('offline-key', 'offline-secret', transport=transport,
            base_url='http://127.0.0.1:' + str(server.server_port), timeout=timeout, auto_sync_time=False)
        self.addCleanup(client.close)
        return client

    def test_real_trickle_body_stops_at_absolute_deadline_without_read_or_write_replay(self):
        server, requests = self.server()
        for method in ('GET', 'POST', 'DELETE'):
            with self.subTest(method=method):
                client = self.client(server, .3)
                before = len(requests)
                started = time.monotonic()
                expected = ApiError if method == 'GET' else OrderUncertain
                with self.assertRaises(expected) as caught:
                    client.signed(method, '/fapi/v1/order',
                        {'scenario': 'trickle', 'newClientOrderId': 'offline-original'})
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 1.5, 'active trickle data must not renew the absolute body deadline')
                self.assertGreater(elapsed, .1)
                self.assertEqual(len(requests) - before, 1)
                self.assertEqual(client.connection_status()['active_pools'], 0)
                self.assertTrue(client.network_status()['waiting'])
                if method != 'GET':
                    self.assertFalse(caught.exception.definite_not_submitted)
                    self.assertEqual(caught.exception.client_id, 'offline-original')
                else:
                    with self.assertRaises(ApiError):
                        client.public('GET', '/fapi/v1/order')
                    self.assertEqual(len(requests) - before, 1)

    def test_completed_response_timer_cannot_close_next_request_on_reused_socket(self):
        server, requests = self.server()
        client = self.client(server, .3)
        self.assertEqual(client.public('GET', '/fapi/v1/time', {'scenario': 'fast'}), {'ok': True})
        client.timeout = 1.2
        self.assertEqual(client.public('GET', '/fapi/v1/time', {'scenario': 'delayed'}), {'ok': True})
        self.assertEqual(requests, [('GET', 'fast'), ('GET', 'delayed')])
        self.assertEqual(client.connection_status()['socket_reuse_responses'], 1)
        self.assertFalse(client.network_status()['waiting'])

    def test_peer_close_trickled_body_still_has_absolute_deadline(self):
        server, requests = self.server(peer_close=True)
        client = self.client(server, .3)
        started = time.monotonic()
        with self.assertRaises(ApiError):
            client.public('GET', '/fapi/v1/time', {'scenario': 'trickle'})
        self.assertLess(time.monotonic() - started, 1.5)
        self.assertEqual(requests, [('GET', 'trickle')])
        self.assertEqual(client.connection_status()['peer_close_responses'], 1)
        self.assertEqual(client.connection_status()['body_deadline_timeouts'], 1)
        self.assertEqual(client.connection_status()['active_pools'], 0)


if __name__ == '__main__':
    unittest.main()
