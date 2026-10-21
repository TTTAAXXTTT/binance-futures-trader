"""Offline recovery ownership, bounded manual reads, and separate stream wakes."""
import json
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from pathlib import Path
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT), str(ROOT / 'tests')]
from execution.binance import (ApiError, BinanceClient, ManualConnectionCheckExpired,
    ManualConnectionCheckStale, ManualConnectionWriteForbidden, ManualConnectionCheckBusy,
    NetworkRetryPending, ReadBudgetDeferred, PooledHTTPTransport)
from execution.network_retry import NetworkRetryGate, NetworkBackoff
from execution.request_budget import RequestBudget
from execution.streams import MarketStreams
from test_hourly_stream_retry import VirtualTime, VirtualStop, OfflineSocket, Connector


class Clock:
    def __init__(self, value=0):
        self.value = value

    def mono(self):
        return self.value

    def wall(self):
        return 1800000000 + self.value


class Response:
    def __init__(self, result, status=200, headers=None):
        self.result, self.status, self.headers = result, status, headers or {}
        self.closed = False

    def read(self):
        return json.dumps(self.result).encode()

    def close(self):
        self.closed = True


class Transport:
    def __init__(self, *actions):
        self.actions, self.calls = list(actions), []

    def __call__(self, request, timeout):
        self.calls.append((request, timeout))
        action = self.actions.pop(0)
        result = action() if callable(action) else action
        if isinstance(result, BaseException):
            raise result
        return result if isinstance(result, Response) else Response(result)


class ManualReads(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / 'budget.sqlite'
        self.clock = Clock()
        self.budget = RequestBudget(self.path, clock=self.clock.wall)
        self.budget.network = NetworkRetryGate(self.path, clock=self.clock.wall, monotonic=self.clock.mono)

    def client(self, transport, **options):
        return BinanceClient('offline-key', 'offline-secret', transport,
            budget=self.budget, clock=self.clock.wall, monotonic=self.clock.mono,
            auto_sync_time=False, **options)

    def waiting(self):
        self.budget.network.failed('original fault')
        self.clock.value = 120

    def test_manual_when_eight_minutes_remain_completes_all_gets_before_release(self):
        self.waiting()
        transport = Transport({'serverTime': 1800000120000}, {'canTrade': True}, [])
        client = self.client(transport)
        with client.manual_connection_check() as check:
            client.sync_time()
            client.account()
            client.positions('ETHUSDT')
            self.assertFalse(check.completed)
            self.assertTrue(client.network_status()['recovering'])
        self.assertTrue(check.completed)
        self.assertEqual(check.wire_attempts, 3)
        self.assertFalse(client.network_status()['waiting'])
        self.assertTrue(all(request.get_method() == 'GET' for request, _ in transport.calls))

    def test_manual_failure_retains_original_deadline(self):
        self.waiting()
        client = self.client(Transport(URLError(OSError('offline'))))
        with self.assertRaises(ApiError):
            with client.manual_connection_check():
                self.clock.value += 5
                client.account()
        self.assertEqual(client.network_status()['retry_in_seconds'], 475)
        self.assertEqual(client.network_status()['failures'], 2)

    def test_manual_failure_from_healthy_creates_one_normal_retry_deadline(self):
        client = self.client(Transport(TimeoutError('offline')))
        with self.assertRaises(ApiError):
            with client.manual_connection_check():
                client.account()
        self.assertEqual(client.network_status()['retry_in_seconds'], 600)

    def test_empty_context_does_not_clear_fault_or_claim_success(self):
        self.waiting()
        client = self.client(Transport())
        with client.manual_connection_check() as check:
            pass
        self.assertFalse(check.completed)
        self.assertTrue(client.network_status()['waiting'])
        self.assertFalse(client.network_status()['probing'])

    def test_healthy_concurrent_normal_get_continues_without_releasing_manual_owner(self):
        manual = self.client(Transport({'canTrade': True}))
        normal = self.client(Transport([]))
        with manual.manual_connection_check() as check:
            normal.positions()
            self.assertTrue(manual.network_status()['probing'])
            manual.account()
        self.assertTrue(check.completed)

    def test_auto_probe_at_due_boundary_cannot_race_manual_owner(self):
        self.waiting()
        client = self.client(Transport({'canTrade': True}))
        self.clock.value = 599
        with client.manual_connection_check():
            self.clock.value = 600
            with self.assertRaises(NetworkBackoff):
                self.budget.network.before_read()
            client.account()

    def test_new_fault_between_gets_invalidates_old_manual_success(self):
        self.waiting()
        client = self.client(Transport({'canTrade': True}))
        with self.assertRaises(ManualConnectionCheckStale):
            with client.manual_connection_check():
                client.account()
                self.budget.network.failed('new fault')
        self.assertEqual(client.network_status()['last_failure_reason'], 'new fault')
        self.assertEqual(client.network_status()['retry_in_seconds'], 600)

    def test_new_fault_while_response_in_flight_is_preserved(self):
        self.waiting()
        def late():
            self.budget.network.failed('newer wire fault')
            return {'canTrade': True}
        client = self.client(Transport(late))
        with self.assertRaises(ManualConnectionCheckStale):
            with client.manual_connection_check():
                client.account()
        self.assertEqual(client.network_status()['last_failure_reason'], 'newer wire fault')

    def test_new_fault_while_old_failure_in_flight_is_not_overwritten(self):
        self.waiting()
        def late():
            self.budget.network.failed('newer fault')
            return TimeoutError('old failure')
        client = self.client(Transport(late))
        with self.assertRaises(ManualConnectionCheckStale):
            with client.manual_connection_check():
                client.account()
        self.assertEqual(client.network_status()['last_failure_reason'], 'newer fault')

    def test_deadline_caps_every_request_and_rejects_late_success(self):
        self.waiting()
        def first():
            self.clock.value += 6
            return {'canTrade': True}
        def second():
            self.clock.value += 5
            return []
        transport = Transport(first, second)
        client = self.client(transport, timeout=12)
        with self.assertRaises(ManualConnectionCheckExpired):
            with client.manual_connection_check(deadline_seconds=10):
                client.account()
                client.positions()
        self.assertEqual([timeout for _, timeout in transport.calls], [10, 4])
        self.assertEqual(client.network_status()['retry_in_seconds'], 469)

    def test_expired_before_wire_never_submits(self):
        self.waiting()
        transport = Transport()
        client = self.client(transport)
        with self.assertRaises(ManualConnectionCheckExpired):
            with client.manual_connection_check(deadline_seconds=1):
                self.clock.value += 1
                client.account()
        self.assertEqual(transport.calls, [])
        self.assertEqual(client.network_status()['retry_in_seconds'], 479)

    def test_late_transport_timeout_does_not_renew_original_wait(self):
        self.waiting()
        def hung():
            self.clock.value += 31
            return TimeoutError('offline hung')
        client = self.client(Transport(hung))
        with self.assertRaises(ManualConnectionCheckExpired):
            with client.manual_connection_check():
                client.account()
        self.assertEqual(client.network_status()['retry_in_seconds'], 449)

    def test_writes_are_rejected_before_budget_signature_or_transport(self):
        for method in ('POST', 'DELETE'):
            transport = Transport()
            client = self.client(transport)
            with self.assertRaises(ManualConnectionWriteForbidden) as caught:
                with client.manual_connection_check():
                    client.signed(method, '/fapi/v1/order')
            self.assertTrue(caught.exception.definite_not_submitted)
            self.assertEqual(transport.calls, [])
        self.assertEqual(self.budget.snapshot()['used_weight'], 0)

    def test_nested_context_cannot_claim_duplicate(self):
        client = self.client(Transport({'canTrade': True}))
        with client.manual_connection_check():
            with self.assertRaises(ManualConnectionCheckBusy):
                with client.manual_connection_check():
                    self.fail('duplicate entered')
            client.account()

    def test_two_clients_merge_at_shared_gate_without_wire(self):
        self.waiting()
        transport = Transport()
        first, second = self.client(Transport({'canTrade': True})), self.client(transport)
        with first.manual_connection_check():
            with self.assertRaises(NetworkRetryPending):
                with second.manual_connection_check():
                    second.account()
            first.account()
        self.assertEqual(transport.calls, [])

    def test_cross_thread_get_on_manual_client_rejected(self):
        client = self.client(Transport({'canTrade': True}))
        errors = []
        with client.manual_connection_check():
            thread = threading.Thread(target=lambda: self.capture(client, errors))
            thread.start(); thread.join(2)
            self.assertFalse(thread.is_alive())
            client.account()
        self.assertIsInstance(errors[0], ManualConnectionCheckBusy)

    @staticmethod
    def capture(client, errors):
        try:
            client.account()
        except BaseException as error:
            errors.append(error)

    def test_local_budget_wait_sends_no_wire_preserves_network_deadline(self):
        self.waiting()
        self.budget.read_cap = 1
        self.budget.reserve('GET', '/fapi/v1/time')
        transport = Transport()
        client = self.client(transport)
        with self.assertRaises(ReadBudgetDeferred) as caught:
            with client.manual_connection_check():
                client.account()
        self.assertEqual(caught.exception.recovery_kind, 'local_budget_wait')
        self.assertEqual(transport.calls, [])
        self.assertEqual(client.network_status()['retry_in_seconds'], 480)

    def test_real_429_and_418_cooldown_cannot_be_bypassed(self):
        for status in (429, 418):
            with self.subTest(status=status):
                self.budget.observe({'Retry-After': '900'}, status=status, code=-1003)
                transport = Transport()
                client = self.client(transport)
                with self.assertRaises(ReadBudgetDeferred) as caught:
                    with client.manual_connection_check():
                        client.account()
                self.assertEqual(caught.exception.recovery_kind, 'exchange_cooldown')
                self.assertEqual(transport.calls, [])

    def test_rate_response_records_cooldown_without_postponing_network_retry(self):
        self.waiting()
        client = self.client(Transport(Response({'code': -1003, 'msg': 'Too many requests'},
                                               429, {'Retry-After': '120'})))
        with self.assertRaises(ApiError) as caught:
            with client.manual_connection_check():
                client.account()
        self.assertEqual(caught.exception.recovery_kind, 'exchange_cooldown')
        self.assertEqual(client.network_status()['retry_in_seconds'], 480)
        self.assertGreater(self.budget.snapshot()['cooldown_seconds'], 0)

    def test_cached_calibration_failure_can_be_manually_probed(self):
        self.waiting()
        client = self.client(Transport({'serverTime': 1800000120000}, {'canTrade': True}))
        client._time_sync_error = ApiError(None, 'old failure', recovery_kind='network_read_failure')
        client._time_sync_retry_at = 600
        with client.manual_connection_check() as check:
            client.sync_time()
            client.account()
        self.assertTrue(check.completed)
        self.assertTrue(client.clock_status()['synced'])

    def test_manual_calibration_failure_metadata_uses_original_deadline(self):
        self.waiting()
        client = self.client(Transport(TimeoutError('offline')))
        with self.assertRaises(ApiError) as caught:
            with client.manual_connection_check():
                client.sync_time()
        self.assertEqual(caught.exception.retry_in_seconds, 480)
        self.assertEqual(client.clock_status()['sync_failures'], 0)

    def test_invalid_deadlines_do_not_claim_lease(self):
        client = self.client(Transport())
        for seconds in (0, -1, 121, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                with client.manual_connection_check(seconds):
                    self.fail('invalid context entered')
        self.assertFalse(client.network_status()['probing'])

    def failed_clock_then_manual(self):
        normal_transport = Transport(TimeoutError('offline'), {'serverTime': 1800000120000})
        normal = self.client(normal_transport)
        with self.assertRaises(ApiError):
            normal.sync_time()
        self.clock.value = 120
        manual = self.client(Transport({'serverTime': 1800000120000}, {'canTrade': True}))
        with manual.manual_connection_check() as check:
            manual.sync_time()
            manual.account()
        return normal, normal_transport, check.snapshot()

    def test_acknowledgment_releases_only_old_local_clock_wait_and_requires_fresh_sync(self):
        normal, transport, evidence = self.failed_clock_then_manual()
        self.assertGreater(normal.clock_status()['retry_in_seconds'], 0)
        self.assertTrue(normal.acknowledge_manual_connection_check(evidence))
        self.assertFalse(normal.clock_status()['synced'])
        normal.sync_time()
        self.assertEqual(len(transport.calls), 2)
        self.assertTrue(normal.clock_status()['synced'])

    def test_acknowledgment_rejects_new_shared_fault(self):
        normal, transport, evidence = self.failed_clock_then_manual()
        self.budget.network.failed('new network fault')
        self.assertFalse(normal.acknowledge_manual_connection_check(evidence))
        self.assertGreater(normal.clock_status()['retry_in_seconds'], 0)
        self.assertEqual(len(transport.calls), 1)

    def test_acknowledgment_rejects_credentials_cooldown_and_unknown_clock_errors(self):
        for kind in ('credentials_error', 'exchange_cooldown', 'timestamp_rejected', 'permanent'):
            normal, _, evidence = self.failed_clock_then_manual()
            normal._time_sync_error = ApiError(-2015, 'offline unknown', recovery_kind=kind)
            self.assertFalse(normal.acknowledge_manual_connection_check(evidence))
            self.assertGreater(normal.clock_status()['retry_in_seconds'], 0)

    def test_acknowledgment_rejects_incomplete_or_forged_epoch_and_does_not_copy_clock(self):
        normal, _, evidence = self.failed_clock_then_manual()
        for changes in ({'completed': False}, {'generation': evidence['generation'] + 1},
                        {'wire_attempts': 0}, {'owner': ''}):
            self.assertFalse(normal.acknowledge_manual_connection_check(dict(evidence, **changes)))
        self.assertFalse(normal.clock_status()['synced'])

    def test_pooled_queue_wait_rechecks_before_wire(self):
        self.waiting()
        clock = self.clock
        class OfflinePool(PooledHTTPTransport):
            def __init__(self):
                self.wires = 0
            def __call__(self, request, *, timeout, prepare_request=None, refresh_request=None, **kwargs):
                clock.value += 31
                request = prepare_request(request)
                request = refresh_request(request)
                self.wires += 1
                return Response({'canTrade': True})
        transport = OfflinePool()
        client = self.client(transport)
        with self.assertRaises(ManualConnectionCheckExpired):
            with client.manual_connection_check():
                client.account()
        self.assertEqual(transport.wires, 0)

    def test_shared_lease_admission_time_is_inside_total_deadline(self):
        self.waiting()
        transport = Transport({'canTrade': True})
        client = self.client(transport)
        claim = self.budget.network.manual_claim
        def slow_claim(**options):
            self.clock.value += 8
            return claim(**options)
        with patch.object(self.budget.network, 'manual_claim', side_effect=slow_claim):
            with client.manual_connection_check(deadline_seconds=10):
                client.account()
        self.assertEqual(transport.calls[0][1], 2)

    def test_expired_shared_admission_does_not_enter_or_send(self):
        self.waiting()
        transport = Transport()
        client = self.client(transport)
        claim = self.budget.network.manual_claim
        def slow_claim(**options):
            self.clock.value += 11
            return claim(**options)
        with patch.object(self.budget.network, 'manual_claim', side_effect=slow_claim):
            with self.assertRaises(ManualConnectionCheckExpired):
                with client.manual_connection_check(deadline_seconds=10):
                    self.fail('expired admission entered')
        self.assertEqual(transport.calls, [])
        self.assertFalse(client.network_status()['probing'])


class PersistentLease(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / 'gate.sqlite'
        self.clock = Clock()

    def gate(self):
        return NetworkRetryGate(self.path, clock=self.clock.wall, monotonic=self.clock.mono)

    def test_restart_keeps_inflight_lease_then_expiry_allows_one_new_owner(self):
        gate = self.gate()
        gate.failed('offline')
        self.clock.value = 120
        old = gate.manual_claim(probe_seconds=30)
        restarted = self.gate()
        with self.assertRaises(NetworkBackoff):
            restarted.manual_claim(probe_seconds=30)
        self.clock.value = 151
        new = restarted.manual_claim(probe_seconds=30)
        self.assertFalse(gate.manual_complete(old))
        self.assertTrue(restarted.manual_complete(new))

    def test_two_threads_only_one_shared_owner(self):
        barrier = threading.Barrier(3)
        owners, rejected = [], []
        def claim():
            barrier.wait()
            try:
                owners.append(self.gate().manual_claim(probe_seconds=30))
            except NetworkBackoff:
                rejected.append(True)
        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(owners), 1)
        self.assertEqual(len(rejected), 1)

    def test_os_monotonic_reset_invalidates_old_owner_and_preserves_wait(self):
        gate = self.gate()
        self.clock.value = 100
        gate.failed('offline')
        self.clock.value = 120
        old = gate.manual_claim(probe_seconds=30)
        self.clock.value = 1
        restarted = self.gate()
        self.assertFalse(restarted.manual_complete(old))
        self.assertEqual(restarted.snapshot()['retry_in_seconds'], 580)
        self.assertFalse(restarted.snapshot()['probing'])


class StreamManualWakes(unittest.TestCase):
    def test_healthy_channel_untouched_selected_disconnected_queued_and_merged(self):
        streams = MarketStreams()
        streams.connected['public'] = True
        streams.diagnostics['market']['phase'] = 'retry_wait'
        streams.retry['market'] = {'failures': 1, 'deadline': streams.monotonic() + 480}
        first = streams.request_reconnect()
        second = streams.request_reconnect(('market',))
        self.assertEqual(first['public']['state'], 'healthy')
        self.assertEqual(first['market']['id'], second['market']['id'])
        self.assertTrue(second['market']['merged'])
        self.assertEqual(streams.threads, [])

    def test_existing_automatic_connect_is_merged_without_queuing_next_connect(self):
        streams = MarketStreams()
        streams.diagnostics['market']['phase'] = 'connect'
        result = streams.request_reconnect(('market',))
        self.assertEqual(result['market']['state'], 'connecting')
        self.assertTrue(result['market']['merged'])
        self.assertIsNone(streams.snapshot()['manual_reconnect']['market'])

    def test_request_only_selected_channel_does_not_wake_other_wait(self):
        streams = MarketStreams()
        for channel in ('public', 'market'):
            streams.diagnostics[channel]['phase'] = 'retry_wait'
            streams.retry[channel]['deadline'] = streams.monotonic() + 480
        result = streams.request_reconnect(('market',))
        self.assertEqual(set(result), {'market'})
        self.assertFalse(streams._retry_wake['public'].is_set())
        self.assertIsNone(streams.snapshot()['manual_reconnect']['public'])

    def test_expired_pending_request_uses_original_automatic_time(self):
        clock = VirtualTime()
        event = VirtualStop(clock, stop_on_wait=1)
        streams = MarketStreams(clock=clock.wall, monotonic=clock.monotonic, stop_event=event)
        streams.diagnostics['market']['phase'] = 'retry_wait'
        streams.retry['market'] = {'failures': 1, 'deadline': 600}
        streams.request_reconnect(('market',), deadline_seconds=30)
        clock.now = 31
        self.assertFalse(streams._wait_for_retry('market'))
        self.assertEqual(event.waits, [569])
        self.assertEqual(streams.snapshot()['manual_reconnect']['market']['state'], 'expired')

    def test_not_running_and_stopped_do_not_create_threads(self):
        streams = MarketStreams()
        self.assertEqual(streams.request_reconnect()['market']['state'], 'not_running')
        streams.stop()
        self.assertEqual(streams.request_reconnect()['market']['state'], 'stopped')
        self.assertEqual(streams.threads, [])

    def test_manual_failure_does_not_move_original_automatic_retry(self):
        clock = VirtualTime()
        waits = []
        def request_once():
            waits.append(clock.now)
            if len(waits) == 1:
                clock.now = 120
                streams.request_reconnect(('public',))
        event = VirtualStop(clock, advances=(0, 480), on_wait=request_once)
        connector = Connector(clock, OSError('first'), OSError('manual'), OfflineSocket(event))
        streams = MarketStreams(clock=clock.wall, monotonic=clock.monotonic, stop_event=event, connect=connector)
        streams._run('public')
        self.assertEqual([call[0] for call in connector.calls], [0, 120, 600])
        self.assertEqual(streams.snapshot()['manual_reconnect']['public']['state'], 'failed')

    def test_late_manual_success_is_closed_and_keeps_original_retry(self):
        clock = VirtualTime()
        waits = []
        def request_once():
            waits.append(clock.now)
            if len(waits) == 1:
                clock.now = 120
                streams.request_reconnect(('market',), deadline_seconds=10)
        event = VirtualStop(clock, advances=(0, 469), on_wait=request_once)
        class SlowSocket(OfflineSocket):
            def __enter__(self):
                clock.now += 11
                return self
        connector = Connector(clock, OSError('first'), SlowSocket(event), OfflineSocket(event))
        streams = MarketStreams(clock=clock.wall, monotonic=clock.monotonic, stop_event=event, connect=connector)
        streams._run('market')
        self.assertEqual([call[0] for call in connector.calls], [0, 120, 600])
        self.assertEqual(connector.calls[1][2]['open_timeout'], 10)
        self.assertEqual(streams.snapshot()['manual_reconnect']['market']['state'], 'expired')

    def test_real_wait_wakes_single_existing_thread_immediately(self):
        event = threading.Event()
        first_wait, connected = threading.Event(), threading.Event()
        calls = []
        class WaitingSocket:
            def __enter__(self):
                connected.set()
                return self
            def __exit__(self, *_):
                return False
            def recv(self, timeout):
                event.wait(.01)
                raise TimeoutError()
            def close(self):
                event.set()
        def connector(*args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                first_wait.set()
                raise OSError('offline')
            return WaitingSocket()
        streams = MarketStreams(connect=connector, stop_event=event)
        worker = threading.Thread(target=streams._run, args=('public',), daemon=True)
        streams.threads = [worker]
        worker.start()
        self.assertTrue(first_wait.wait(1))
        until = time.monotonic() + 1
        while streams.snapshot()['diagnostics']['public']['phase'] != 'retry_wait' and time.monotonic() < until:
            time.sleep(.001)
        for _ in range(10):
            streams.request_reconnect(('public',))
        self.assertTrue(connected.wait(1))
        self.assertEqual(len(calls), 2)
        streams.stop()
        self.assertFalse(worker.is_alive())


if __name__ == '__main__':
    unittest.main()
