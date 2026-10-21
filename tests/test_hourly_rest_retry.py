"""Persistent REST recovery tests use fake wire, virtual time and disposable SQLite."""
import io
import json
import socket
import sqlite3
import ssl
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]

from execution.binance import ApiError, BinanceClient, OrderUncertain
from execution.network_retry import NetworkBackoff, NetworkRetryGate
from execution.request_budget import RequestBudget
from test_hourly_binance import Response, Transport


class SharedRestRetryTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / 'shared-budget.sqlite'
        self.wall = 6000.0
        self.monotonic = 100.0

    def advance(self, seconds):
        self.wall += seconds
        self.monotonic += seconds

    def client(self, transport, *, auto_sync_time=False):
        budget = RequestBudget(self.path, clock=lambda: self.wall)
        budget.network.monotonic = lambda: self.monotonic
        return BinanceClient('offline-key', 'offline-secret', transport=transport,
            budget=budget,
            clock=lambda: self.wall, monotonic=lambda: self.monotonic,
            auto_sync_time=auto_sync_time,
            sleep=lambda _: self.fail('No immediate sleep/retry is permitted'))

    def test_shared_gate_construction_is_inert(self):
        first = self.client(Transport())
        second = self.client(Transport())
        self.assertFalse(self.path.exists())
        self.assertEqual(first.transport.requests, [])
        self.assertEqual(second.transport.requests, [])

    def test_failed_read_blocks_multiple_clients_until_exact_boundary_then_success_clears(self):
        first_wire = Transport(TimeoutError('private detail'), {'price': '100'})
        second_wire = Transport({'serverTime': 6600000})
        first, second = self.client(first_wire), self.client(second_wire)
        with self.assertRaises(ApiError) as initial:
            first.contract_price('BTCUSDT')
        self.assertEqual(first.network_status()['retry_in_seconds'], 600)
        self.advance(599)
        for client in (first, second, first, second):
            with self.assertRaises(ApiError) as waiting:
                client.public('GET', '/fapi/v1/time')
            self.assertEqual(str(waiting.exception), str(initial.exception))
        self.assertEqual(len(first_wire.requests), 1)
        self.assertEqual(second_wire.requests, [])
        self.assertEqual(second.network_status()['failures'], 1)
        self.assertEqual(second.network_status()['retry_in_seconds'], 1)
        self.advance(1)
        self.assertEqual(second.public('GET', '/fapi/v1/time')['serverTime'], 6600000)
        self.assertFalse(first.network_status()['waiting'])
        self.assertEqual(first.network_status()['failures'], 0)
        self.assertEqual(first.contract_price('BTCUSDT')['price'], '100')

    def test_repeated_failures_each_schedule_600_seconds_and_do_not_limit_attempt_count(self):
        wire = Transport(*([TimeoutError('fixture')] * 4), {'price': '100'})
        client = self.client(wire)
        for expected_failures in range(1, 5):
            with self.assertRaises(ApiError):
                client.contract_price('ETHUSDT')
            self.assertEqual(client.network_status()['failures'], expected_failures)
            self.assertEqual(client.network_status()['retry_in_seconds'], 600)
            self.advance(600)
        self.assertEqual(client.contract_price('ETHUSDT')['price'], '100')
        self.assertEqual(len(wire.requests), 5)
        self.assertEqual(client.network_status()['failures'], 0)

    def test_http_5xx_retries_only_after_600_and_persisted_reason_is_sanitized(self):
        wire = Transport(Response({'code': -1007, 'msg': 'private-password'}, status=503), {'price': '100'})
        client = self.client(wire)
        with self.assertRaises(ApiError):
            client.contract_price('BTCUSDT')
        status = client.network_status()
        self.assertEqual(status['retry_in_seconds'], 600)
        self.assertNotIn('private-password', json.dumps(status))
        self.assertIn('HTTP 503', status['last_failure_reason'])
        self.advance(600)
        self.assertEqual(client.contract_price('BTCUSDT')['price'], '100')
        self.assertEqual(len(wire.requests), 2)

    def test_raw_transport_signed_url_and_proxy_secret_are_not_saved_or_reported(self):
        private = 'https://private-user:private-password@private-proxy.invalid?signature=private-signature'
        for failure in (URLError(ssl.SSLEOFError(8, private)),
                URLError(socket.gaierror(-2, private)), URLError(OSError('Tunnel connection failed: 407 ' + private))):
            with self.subTest(kind=type(failure).__name__):
                # Independent database per case, so no preceding cooldown shadows wire.
                self.path = Path(self.folder.name) / ('case-' + str(self.wall) + '.sqlite')
                client = self.client(Transport(failure))
                with self.assertRaises(ApiError) as caught:
                    client.account()
                output = str(caught.exception) + json.dumps(client.network_status())
                self.assertNotIn('private-', output)
                self.assertNotIn('signature=', output)
                self.advance(1)

    def test_nested_signed_time_calibration_after_cooldown_does_not_block_own_probe(self):
        wire = Transport(TimeoutError(), {'serverTime': 6600000}, {'availableBalance': '100'})
        client = self.client(wire, auto_sync_time=True)
        with self.assertRaises(ApiError):
            client.account()
        self.assertEqual(wire.paths(), [('GET', '/fapi/v1/time')])
        self.advance(600)
        self.assertEqual(client.account()['availableBalance'], '100')
        self.assertEqual(wire.paths(), [('GET', '/fapi/v1/time'), ('GET', '/fapi/v1/time'), ('GET', '/fapi/v3/account')])
        self.assertFalse(client.network_status()['waiting'])
        self.assertEqual(client.clock_status()['sync_failures'], 0)

    def test_timestamp_rejected_get_after_recovery_can_sync_once_without_nested_lease_deadlock(self):
        wire = Transport({'code': -1021, 'msg': 'clock rejected'}, {'serverTime': 6600000}, {'availableBalance': '100'})
        client = self.client(wire)
        client._network_retry.failed('fixture old connection error')
        self.advance(600)
        self.assertEqual(client.account()['availableBalance'], '100')
        self.assertEqual(wire.paths(), [('GET', '/fapi/v3/account'), ('GET', '/fapi/v1/time'), ('GET', '/fapi/v3/account')])
        self.assertFalse(client.network_status()['waiting'])

    def test_windows_wall_jump_does_not_bypass_monotonic_time_sync_wait(self):
        wire = Transport(TimeoutError(), {'serverTime': 10200000}, {'availableBalance': '100'})
        client = self.client(wire, auto_sync_time=True)
        with self.assertRaises(ApiError):
            client.account()
        self.wall += 3600
        self.monotonic += 2
        with self.assertRaises(ApiError):
            client.account()
        self.assertEqual(len(wire.requests), 1)
        self.monotonic += 598
        self.assertEqual(client.account()['availableBalance'], '100')
        self.assertEqual(len(wire.requests), 3)

    def test_windows_wall_forward_and_backward_jumps_do_not_change_public_read_wait(self):
        wire = Transport(TimeoutError(), {'serverTime': 1234000})
        client = self.client(wire)
        with self.assertRaises(ApiError):
            client.public('GET', '/fapi/v1/time')
        for wall_jump in (3600, -7200):
            self.wall += wall_jump
            self.monotonic += 1
            with self.assertRaises(ApiError):
                client.public('GET', '/fapi/v1/time')
            self.assertEqual(len(wire.requests), 1)
        self.monotonic = 700
        self.assertEqual(client.public('GET', '/fapi/v1/time')['serverTime'], 1234000)

    def test_late_client_local_clock_probe_does_not_delay_shared_recovery_boundary(self):
        first_wire = Transport(TimeoutError())
        second_wire = Transport({'serverTime': 6600000}, {'availableBalance': '100'})
        first, second = self.client(first_wire), self.client(second_wire, auto_sync_time=True)
        with self.assertRaises(ApiError):
            first.public('GET', '/fapi/v1/time')
        self.advance(500)
        with self.assertRaises(ApiError) as pending:
            second.sync_time()
        self.assertTrue(getattr(pending.exception, 'network_backoff', False))
        self.assertEqual(pending.exception.retry_in_seconds, 100)
        self.assertEqual(second.clock_status()['sync_failures'], 0)
        self.assertEqual(second.clock_status()['retry_in_seconds'], 0)
        self.assertEqual(second_wire.requests, [])
        self.advance(100)
        self.assertEqual(second.account()['availableBalance'], '100')
        self.assertEqual(second_wire.paths(), [('GET', '/fapi/v1/time'), ('GET', '/fapi/v3/account')])

    def test_longer_rate_limit_is_respected_even_when_network_retry_is_due(self):
        wire = Transport({'serverTime': 6900000})
        client = self.client(wire)
        client._network_retry.failed('fixture network fault')
        client.budget.observe({'Retry-After': '900'}, status=429, code=-1003)
        self.advance(600)
        with self.assertRaises(ApiError) as caught:
            client.public('GET', '/fapi/v1/time')
        self.assertEqual(caught.exception.code, -1003)
        self.assertEqual(wire.requests, [])
        self.advance(300)
        self.assertEqual(client.public('GET', '/fapi/v1/time')['serverTime'], 6900000)

    def test_rate_rejected_probe_is_not_success_and_keeps_later_ip_cooldown(self):
        error = HTTPError('https://offline.invalid', 429, 'limited', {'Retry-After': '1200'},
                          io.BytesIO(b'{"code":-1003,"msg":"limited"}'))
        wire = Transport(error)
        client = self.client(wire)
        client._network_retry.failed('fixture previous network fault')
        self.advance(600)
        with self.assertRaises(ApiError):
            client.public('GET', '/fapi/v1/time')
        self.assertTrue(client.network_status()['waiting'])
        self.assertEqual(client.network_status()['retry_in_seconds'], 600)
        self.advance(600)
        with self.assertRaises(ApiError) as caught:
            client.public('GET', '/fapi/v1/time')
        self.assertEqual(caught.exception.code, -1003)
        self.assertEqual(len(wire.requests), 1)

    def test_access_denied_probe_never_claims_recovery_or_persists_arbitrary_exchange_text(self):
        wire = Transport(Response({'code': -2015, 'msg': 'private-password'}, status=403))
        client = self.client(wire)
        client._network_retry.failed('fixture original network fault')
        self.advance(600)
        with self.assertRaises(ApiError):
            client.account()
        status = client.network_status()
        self.assertTrue(status['waiting'])
        self.assertEqual(status['failures'], 1)
        self.assertNotIn('private-password', json.dumps(status))

    def test_write_timeout_is_submitted_once_and_never_automatically_repeated(self):
        for method in ('POST', 'DELETE'):
            with self.subTest(method=method):
                wire = Transport(TimeoutError('private signed URL'))
                client = self.client(wire)
                with self.assertRaises(OrderUncertain) as caught:
                    client.signed(method, '/fapi/v1/order', {'symbol': 'BTCUSDT', 'origClientOrderId': 'offline-persisted'})
                self.assertEqual(caught.exception.client_id, 'offline-persisted')
                self.assertEqual(len(wire.requests), 1)
                self.assertNotIn('private signed URL', str(caught.exception))
                self.assertEqual(wire.paths()[0][0], method)
                self.assertTrue(client.network_status()['waiting'])
                self.assertEqual(client.network_status()['retry_in_seconds'], 600)


class SharedProbeLeaseTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / 'leases.sqlite'
        self.now = 6000.0
        self.first = NetworkRetryGate(self.path, clock=lambda: self.now)
        self.second = NetworkRetryGate(self.path, clock=lambda: self.now)

    def test_competing_instance_cannot_claim_live_probe_and_expired_old_success_is_ignored(self):
        self.first.failed('fixture network failure')
        self.now += 600
        older = self.first.before_read(probe_seconds=30)
        with self.assertRaises(NetworkBackoff):
            self.second.before_read()
        self.assertTrue(self.second.snapshot()['probing'])
        self.now += 30
        newer = self.second.before_read()
        self.assertNotEqual(older[1], newer[1])
        self.first.succeeded(older)
        self.assertTrue(self.first.snapshot()['probing'])
        self.second.succeeded(newer)
        self.assertFalse(self.first.snapshot()['waiting'])
        self.assertFalse(self.first.snapshot()['recovering'])

    def test_old_success_cannot_clear_newer_failure_generation(self):
        healthy_ticket = self.first.before_read()
        self.second.failed('fixture newer network failure')
        self.first.succeeded(healthy_ticket)
        self.assertEqual(self.first.snapshot()['failures'], 1)
        self.assertEqual(self.first.snapshot()['retry_in_seconds'], 600)
        self.now += 600
        probe_ticket = self.first.before_read()
        self.second.failed('fixture another newer failure')
        self.first.succeeded(probe_ticket)
        self.assertEqual(self.second.snapshot()['failures'], 2)
        self.assertEqual(self.second.snapshot()['retry_in_seconds'], 600)
        self.assertEqual(self.second.snapshot()['last_failure_reason'], 'fixture another newer failure')

    def test_restart_reads_persisted_deadline_and_block_checks_do_not_extend_it(self):
        self.first.failed('fixture stable error')
        deadline = self.first.snapshot()['retry_at_ms']
        self.now += 599
        restarted = NetworkRetryGate(self.path, clock=lambda: self.now)
        for _ in range(5):
            with self.assertRaises(NetworkBackoff):
                restarted.check()
        self.assertEqual(restarted.snapshot()['retry_at_ms'], deadline)
        self.assertEqual(restarted.snapshot()['failures'], 1)
        self.now += 1
        restarted.check()
        ticket = restarted.before_read()
        restarted.succeeded(ticket)
        self.assertFalse(self.first.snapshot()['waiting'])

    def test_old_wall_clock_table_migrates_remaining_wait_without_losing_failure_reason(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('CREATE TABLE network_retry (id INTEGER PRIMARY KEY, generation INTEGER, '
                'failures INTEGER,retry_at REAL,probe_until REAL,probe_id TEXT,reason TEXT,last_success REAL)')
            db.execute('INSERT INTO network_retry VALUES (1,2,2,6300,0,\'\',\'fixture old failure\',0)')
            db.commit()
        mono = {'value': 100.0}
        gate = NetworkRetryGate(self.path, clock=lambda: self.now, monotonic=lambda: mono['value'])
        state = gate.snapshot()
        self.assertEqual(state['retry_in_seconds'], 300)
        self.assertEqual(state['failures'], 2)
        self.assertEqual(state['last_failure_reason'], 'fixture old failure')
        self.now += 7200  # displayed wall deadline changes, remaining wait does not
        self.assertEqual(gate.snapshot()['retry_in_seconds'], 300)
        mono['value'] = 400
        ticket = gate.before_read()
        gate.succeeded(ticket)
        self.assertFalse(gate.snapshot()['waiting'])

    def test_os_monotonic_epoch_reset_invalidates_old_probe_without_adding_600_seconds(self):
        self.first.failed('fixture connection failure')
        self.now += 600
        old_ticket = self.first.before_read()
        self.assertTrue(self.first.snapshot()['probing'])
        self.now = 10.0  # simulate Windows restart resetting monotonic epoch
        restarted = NetworkRetryGate(self.path, clock=lambda: self.now)
        state = restarted.snapshot()
        self.assertFalse(state['probing'])
        self.assertEqual(state['retry_in_seconds'], 0)
        fresh_ticket = restarted.before_read()
        self.assertNotEqual(fresh_ticket[0], old_ticket[0])
        self.first.succeeded(old_ticket)
        self.assertTrue(restarted.snapshot()['probing'])
        restarted.succeeded(fresh_ticket)
        self.assertFalse(restarted.snapshot()['waiting'])


if __name__ == '__main__':
    unittest.main()
