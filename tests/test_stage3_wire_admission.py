"""Entry registration occurs once at the last local gate; all fixtures offline."""
import hashlib
import hmac
import json
import sys
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import parse_qs

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT), str(ROOT / 'tests')]
import urllib3
from execution.binance import (ApiError, BinanceClient, EntryAdmissionError, LocalTransportDeferred,
    NetworkRetryPending, OrderUncertain, PooledHTTPTransport, ReadBudgetDeferred, RequestExpired)
from execution.network_retry import NetworkRetryGate
from execution.request_budget import RequestBudget
from test_hourly_binance import Transport
from test_hourly_pool_transport import Pools, WireResponse
from test_hourly_transport_deadline import Clock, AdvancingLock


class Diagnostic:
    def __init__(self, wire):
        self.on_wire = wire
    @contextmanager
    def request(self, *_):
        yield
    def request_phase(self, *_args, **_kwargs):
        pass
    def wire(self):
        self.on_wire()
    def response_status(self, *_):
        pass


class EntryWireAdmission(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.budget = RequestBudget(Path(self.folder.name) / 'budget.sqlite', clock=self.clock)
        self.budget.network = NetworkRetryGate(self.budget.path, clock=self.clock, monotonic=self.clock)
        self.admissions, self.guards = [], []

    def make(self, *responses, pooled=True, diagnostics=None, auto_sync=False, timeout=12):
        if pooled:
            pools = Pools(*(responses or (WireResponse({'orderId': 123}),)))
            transport = PooledHTTPTransport(pool_factory=pools.factory, proxy_factory=pools.factory,
                proxy_getter=lambda: {}, bypass=lambda _: True, clock=self.clock)
        else:
            pools = None
            transport = Transport(*responses)
        client = BinanceClient('offline-key', 'offline-secret', transport=transport,
            auto_sync_time=auto_sync, timeout=timeout, clock=self.clock, monotonic=self.clock,
            diagnostics=diagnostics, budget=self.budget)
        return client, transport, pools

    def register(self):
        self.admissions.append(self.clock())

    def guard(self):
        self.guards.append(self.clock())

    def order(self, client, **options):
        options.setdefault('before_send', self.register)
        options.setdefault('wire_guard', self.guard)
        options.setdefault('deadline_ms', int((self.clock() + 300) * 1000))
        return client.market_order('ETHUSDT', 'BUY', '.1', 'original-candidate-order', **options)

    def assert_no_submission(self, error, pools):
        self.assertTrue(error.definite_not_submitted)
        self.assertNotIsInstance(error, OrderUncertain)
        self.assertEqual(pools.calls, [])

    def test_success_registers_once_and_guard_runs_again_before_http(self):
        client, _, pools = self.make()
        original = pools.factory
        order = []
        def factory(*args, **kwargs):
            pool = original(*args, **kwargs)
            request = pool.request
            def wire(*values, **options):
                order.append(('http', len(self.admissions), len(self.guards)))
                return request(*values, **options)
            pool.request = wire
            return pool
        client.transport.pool_factory = factory
        self.assertTrue(client.supports_entry_before_send)
        self.assertEqual(self.order(client)['orderId'], 123)
        self.assertEqual(len(self.admissions), 1)
        self.assertGreaterEqual(len(self.guards), 3)
        self.assertEqual(order[-1][0:2], ('http', 1))

    def test_local_budget_wait_does_not_register(self):
        client, _, pools = self.make()
        self.budget.critical_cap = 0
        with self.assertRaises(ReadBudgetDeferred) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(self.admissions, [])

    def test_network_recovery_wait_does_not_register(self):
        client, _, pools = self.make()
        self.budget.network.failed('offline waiting')
        with self.assertRaises(NetworkRetryPending) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(self.admissions, [])

    def test_real_cooldown_wait_does_not_register(self):
        for status in (429, 418):
            self.budget.observe({'Retry-After': '120'}, status=status, code=-1003)
            client, _, pools = self.make()
            with self.assertRaises(ReadBudgetDeferred) as caught:
                self.order(client)
            self.assert_no_submission(caught.exception, pools)
            self.assertEqual(self.admissions, [])

    def test_initial_time_sync_network_failure_precedes_registration(self):
        client, _, pools = self.make(urllib3.exceptions.ConnectTimeoutError(None, 'offline'), auto_sync=True)
        with self.assertRaises(ApiError) as caught:
            self.order(client)
        self.assertTrue(caught.exception.definite_not_submitted)
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(self.admissions, [])
        self.assertEqual([call[0] for call in pools.calls], ['GET'])

    def test_pool_slot_time_is_before_registration(self):
        client, transport, pools = self.make()
        transport._lock = AdvancingLock(self.clock, 4)
        self.order(client)
        self.assertEqual(self.admissions, [6004])
        self.assertEqual(pools.calls[0][2]['timeout'].total, 8)

    def test_expired_after_pool_wait_does_not_register(self):
        client, transport, pools = self.make()
        transport._lock = AdvancingLock(self.clock, 4)
        with self.assertRaises(RequestExpired) as caught:
            self.order(client, deadline_ms=6003000)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(self.admissions, [])

    def test_pool_budget_exhausted_before_admission_is_no_wire_no_registration(self):
        client, transport, pools = self.make(timeout=2)
        transport._lock = AdvancingLock(self.clock, 3)
        with self.assertRaises(LocalTransportDeferred) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(self.admissions, [])

    def test_network_fault_while_waiting_for_pool_prevents_registration(self):
        client, transport, pools = self.make()
        transport._lock = AdvancingLock(self.clock, 1,
            observe=lambda: self.budget.network.failed('new fault while queued'))
        with self.assertRaises(NetworkRetryPending) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(self.admissions, [])

    def test_cooldown_while_waiting_for_pool_prevents_registration(self):
        client, transport, pools = self.make()
        transport._lock = AdvancingLock(self.clock, 1,
            observe=lambda: self.budget.observe({}, status=418, code=-1003))
        with self.assertRaises(ReadBudgetDeferred) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(self.admissions, [])

    def test_guard_false_before_registration_is_local_rejection(self):
        client, _, pools = self.make()
        with self.assertRaises(EntryAdmissionError) as caught:
            self.order(client, wire_guard=lambda: False)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(caught.exception.admission_stage, 'guard')
        self.assertEqual(self.admissions, [])

    def test_registration_changes_controls_guard_prevents_http_without_undoing_registration(self):
        client, _, pools = self.make()
        allowed = True
        def register():
            nonlocal allowed
            self.register()
            allowed = False
        with self.assertRaises(EntryAdmissionError) as caught:
            self.order(client, before_send=register, wire_guard=lambda: allowed)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(len(self.admissions), 1)

    def test_diagnostic_checkpoint_control_change_is_seen_by_final_guard(self):
        allowed = True
        def checkpoint():
            nonlocal allowed
            allowed = False
        client, _, pools = self.make(diagnostics=Diagnostic(checkpoint))
        with self.assertRaises(EntryAdmissionError) as caught:
            self.order(client, wire_guard=lambda: allowed)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(len(self.admissions), 1)

    def test_new_network_fault_after_registration_blocks_http_and_retains_one_registration(self):
        client, _, pools = self.make(diagnostics=Diagnostic(
            lambda: self.budget.network.failed('new network fault')))
        with self.assertRaises(NetworkRetryPending) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(len(self.admissions), 1)

    def test_real_cooldown_after_registration_blocks_http_and_retains_one_registration(self):
        client, _, pools = self.make(diagnostics=Diagnostic(
            lambda: self.budget.observe({}, status=429, code=-1003)))
        with self.assertRaises(ReadBudgetDeferred) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(len(self.admissions), 1)

    def test_checkpoint_expiring_window_after_registration_has_no_write(self):
        client, _, pools = self.make(diagnostics=Diagnostic(lambda: self.clock.advance(4)))
        with self.assertRaises(RequestExpired) as caught:
            self.order(client, deadline_ms=6003000)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(len(self.admissions), 1)

    def test_callback_cannot_invent_an_uncertain_exchange_submission(self):
        client, _, pools = self.make()
        def broken():
            raise OrderUncertain(None, 'offline local callback', client_id='unrelated')
        with self.assertRaises(EntryAdmissionError) as caught:
            self.order(client, before_send=broken)
        self.assert_no_submission(caught.exception, pools)
        self.assertFalse(client.network_status()['waiting'])

    def test_local_callback_oserror_is_not_network_or_uncertain(self):
        client, transport, pools = self.make()
        def broken():
            raise OSError('private://local/path-and-secret')
        with self.assertRaises(EntryAdmissionError) as caught:
            self.order(client, before_send=broken)
        self.assert_no_submission(caught.exception, pools)
        self.assertFalse(client.network_status()['waiting'])
        self.assertNotIn('private', str(caught.exception))
        self.assertEqual(caught.exception.local_error_type, 'OSError')
        self.assertEqual(transport.snapshot()['wire_attempts'], 0)

    def test_tls_local_preparation_oserror_before_registration_is_definitely_unsent(self):
        client, transport, pools = self.make()
        def unavailable_context():
            raise OSError('private local TLS context path')
        transport.context_factory = unavailable_context
        with self.assertRaises(ApiError) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(self.admissions, [])
        self.assertEqual(caught.exception.recovery_kind, 'network_read_failure')
        self.assertTrue(client.network_status()['waiting'])
        self.assertNotIn('private', str(caught.exception))

    def test_manager_creation_error_before_registration_is_definitely_unsent(self):
        client, transport, pools = self.make()
        def unavailable_manager(*args, **kwargs):
            raise urllib3.exceptions.ProxyError('private proxy', OSError('offline'))
        transport.pool_factory = unavailable_manager
        with self.assertRaises(ApiError) as caught:
            self.order(client)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(self.admissions, [])

    def test_guard_oserror_after_registration_is_unsent_not_a_network_failure(self):
        client, _, pools = self.make()
        def guard():
            if self.admissions:
                raise OSError('offline local controls read')
        with self.assertRaises(EntryAdmissionError) as caught:
            self.order(client, wire_guard=guard)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(len(self.admissions), 1)
        self.assertFalse(client.network_status()['waiting'])

    def test_slow_registration_expiring_signal_stays_registered_without_http(self):
        client, _, pools = self.make()
        def slow():
            self.register()
            self.clock.advance(4)
        with self.assertRaises(RequestExpired) as caught:
            self.order(client, before_send=slow, deadline_ms=6003000)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(len(self.admissions), 1)

    def test_slow_registration_exhausting_transport_budget_stays_registered_without_http(self):
        client, _, pools = self.make(timeout=2)
        def slow():
            self.register()
            self.clock.advance(3)
        with self.assertRaises(LocalTransportDeferred) as caught:
            self.order(client, before_send=slow)
        self.assert_no_submission(caught.exception, pools)
        self.assertEqual(len(self.admissions), 1)

    def test_final_signature_includes_elapsed_registration_and_checkpoint(self):
        def checkpoint():
            self.clock.advance(2)
        client, _, pools = self.make(diagnostics=Diagnostic(checkpoint))
        def register():
            self.register()
            self.clock.advance(2)
        self.order(client, before_send=register)
        body = pools.calls[0][2]['body'].decode()
        fields = parse_qs(body)
        self.assertEqual(fields['timestamp'], ['6004000'])
        encoded, signature = body.rsplit('&signature=', 1)
        self.assertEqual(signature, hmac.new(b'offline-secret', encoded.encode(), hashlib.sha256).hexdigest())
        self.assertEqual(len(self.admissions), 1)

    def test_actual_http_timeout_remains_uncertain_and_never_replays(self):
        client, _, pools = self.make(urllib3.exceptions.ReadTimeoutError(None, 'offline', 'timeout'))
        with self.assertRaises(OrderUncertain) as caught:
            self.order(client)
        self.assertFalse(caught.exception.definite_not_submitted)
        self.assertEqual(caught.exception.client_id, 'original-candidate-order')
        self.assertEqual(len(self.admissions), 1)
        self.assertEqual(len(pools.calls), 1)

    def test_explicit_timestamp_rejection_never_reenters_registration_or_replays_write(self):
        client, _, pools = self.make(
            WireResponse({'code': -1021, 'msg': 'offline timestamp'}, status=400),
            WireResponse({'serverTime': 6001000}))
        with self.assertRaises(ApiError) as caught:
            self.order(client)
        self.assertEqual(caught.exception.code, -1021)
        self.assertEqual(len(self.admissions), 1)
        self.assertEqual([call[0] for call in pools.calls], ['POST', 'GET'])

    def test_reduce_only_and_protection_not_blocked_by_entry_network_gate(self):
        client, _, pools = self.make(WireResponse({'orderId': 1}), WireResponse({'algoId': 2}))
        self.budget.network.failed('read network recovery waiting')
        client.market_order('ETHUSDT', 'SELL', '.1', 'original-exit', reduce_only=True)
        client.new_stop('ETHUSDT', 'SELL', '2500', 'original-protection')
        self.assertEqual([call[0] for call in pools.calls], ['POST', 'POST'])
        self.assertEqual(self.admissions, [])

    def test_hooks_rejected_for_reduce_only_or_non_callable_before_any_http(self):
        client, _, pools = self.make()
        for options in ({'reduce_only': True}, {'before_send': 3}, {'wire_guard': 3}):
            with self.assertRaises(ValueError):
                self.order(client, **options)
        self.assertEqual(pools.calls, [])
        self.assertEqual(self.admissions, [])

    def test_injected_simple_transport_registers_once_and_refreshes_after_guard(self):
        client, transport, _ = self.make({'orderId': 2}, pooled=False)
        def register():
            self.register()
            self.clock.advance(2)
        self.order(client, before_send=register)
        self.assertEqual(len(self.admissions), 1)
        self.assertEqual(transport.fields()['timestamp'], '6002000')
        self.assertEqual(len(transport.requests), 1)

    def test_injected_transport_callback_failure_never_calls_transport(self):
        client, transport, _ = self.make(pooled=False)
        def broken():
            raise OSError('offline local storage')
        with self.assertRaises(EntryAdmissionError) as caught:
            self.order(client, before_send=broken)
        self.assertTrue(caught.exception.definite_not_submitted)
        self.assertEqual(transport.requests, [])
        self.assertFalse(client.network_status()['waiting'])

    def test_pool_callback_local_failure_releases_slot(self):
        client, transport, pools = self.make()
        with self.assertRaises(EntryAdmissionError):
            self.order(client, wire_guard=lambda: False)
        acquired = []
        def check():
            success = transport._lock.acquire(timeout=.5)
            acquired.append(success)
            if success:
                transport._lock.release()
        thread = threading.Thread(target=check)
        thread.start(); thread.join(1)
        self.assertEqual(acquired, [True])
        self.assertEqual(pools.calls, [])


if __name__ == '__main__':
    unittest.main()
