"""Display-only account reuse and request cadence; no external network calls."""
import copy
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'runtime/packages'), str(ROOT / 'freqtrade')]
from control import hourly_app as console


class MonitorReuseChecks(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.now = 100.0
        self.client = object()
        self.configuration = {'can_trade': True, 'hedge_mode': False, 'multi_asset': False,
            'taker_fees': {'BTCUSDT': .0005, 'ETHUSDT': .0005},
            'leverage_limits': {'BTCUSDT': 125, 'ETHUSDT': 125},
            'actual_settings': {'BTCUSDT': {'leverage': 50, 'isolated': False}}}
        self.direct = dict(copy.deepcopy(self.configuration), connected=True, available_usdt=103,
            equity_usdt=103, wallet_usdt=103, unrealized_usdt=0, positions=[],
            open_orders=0, open_algo_orders=0, error='')
        self.worker = {'connected': True, 'reconciled': True, 'state': 'running',
            'last_successful_reconcile_ms': 1800000000000, 'fault_reason': None,
            'unknown_orders': [], 'execution_recovery': {'pending': False},
            'funding_read': {'pending_recovery': False}, 'network_retry': {'waiting': False},
            'account_snapshot': {'reconciled_at_ms': 1800000000000, 'age_seconds': 0,
                'account': {'availableBalance': '99', 'totalMarginBalance': '101',
                            'totalWalletBalance': '100.5', 'totalUnrealizedProfit': '.5'},
                'positions': [{'symbol': 'ETHUSDT', 'positionAmt': '0.1', 'entryPrice': '2500',
                               'unRealizedProfit': '.5', 'positionSide': 'BOTH'}],
                'open_orders': [{'symbol': 'ETHUSDT', 'orderId': 123}],
                'open_algo_orders': [{'symbol': 'ETHUSDT', 'algoId': 456}]}}
        self.stack.enter_context(patch.dict(console.STATE, {'account': {}, 'worker': {},
            'control_revision': 0, 'markets': {}, 'events': []}, clear=True))
        self.stack.enter_context(patch.dict(console.MONITOR_CACHE, {}, clear=True))
        self.stack.enter_context(patch.object(console.time, 'monotonic', side_effect=lambda: self.now))
        self.stack.enter_context(patch.object(console.time, 'time', return_value=1800000000))
        self.reader = self.stack.enter_context(patch.object(console.compat, 'read_account',
            side_effect=lambda _client: copy.deepcopy(self.direct)))
        self.snapshot = self.stack.enter_context(patch.object(console, 'worker_snapshot',
            side_effect=lambda: copy.deepcopy(self.worker)))
        self.stack.enter_context(patch.object(console, 'start_reasons', return_value=[]))
        self.stack.enter_context(patch.object(console.urllib.request, 'urlopen',
            side_effect=AssertionError('No actual network in monitor tests')))
        self.stack.enter_context(patch.object(console.subprocess, 'Popen',
            side_effect=AssertionError('No actual process in monitor tests')))

    def test_initial_monitor_reads_configuration_then_reuses_fresh_worker_balance_and_orders(self):
        console.refresh_monitor_private(self.client)
        self.assertEqual(self.reader.call_count, 1)
        self.assertEqual(console.STATE['account']['source'], 'console_direct')
        self.now += 5
        self.worker['account_snapshot']['age_seconds'] = 5
        console.refresh_monitor_private(self.client)
        self.assertEqual(self.reader.call_count, 1)
        account = console.STATE['account']
        self.assertEqual(account['source'], 'worker_reconcile')
        self.assertEqual(account['age_seconds'], 5)
        self.assertEqual(account['available_usdt'], 99)
        self.assertEqual(account['equity_usdt'], 101)
        self.assertEqual(account['positions'][0]['quantity'], .1)
        self.assertEqual(account['open_orders'], 1)
        self.assertEqual(account['open_algo_orders'], 1)
        self.assertEqual(account['order_ids'], [{'symbol': 'ETHUSDT', 'id': '123'},
                                               {'symbol': 'ETHUSDT', 'id': '456'}])
        self.assertTrue(account['can_trade'])
        self.assertFalse(account['hedge_mode'])

    def test_force_fresh_human_read_never_uses_monitor_reuse(self):
        console.refresh_monitor_private(self.client)
        self.now += 5
        console.refresh_monitor_private(self.client)
        self.assertEqual(self.reader.call_count, 1)
        console.refresh_private(self.client)
        self.assertEqual(self.reader.call_count, 2)
        self.assertEqual(console.STATE['account']['source'], 'console_direct')

    def test_busy_and_failed_publication_do_not_create_competing_rest_or_reset_age(self):
        console.refresh_monitor_private(self.client)
        observed = console.STATE['account_observed_monotonic']
        for metadata in ({'execution_busy': True}, {'publication_error': 'not yet published'}):
            self.now += 100
            self.worker['snapshot_metadata'] = metadata
            console.refresh_monitor_private(self.client)
            self.assertEqual(self.reader.call_count, 1)
            self.assertTrue(console.STATE['account']['stale'])
            self.assertEqual(console.STATE['account']['source'], 'worker_previous_reconcile')
            self.assertEqual(console.STATE['account_observed_monotonic'], observed)
            self.assertIsNone(console.worker_account_view(self.worker, self.configuration, 0))

    def test_old_configuration_requires_real_read_at_300_second_boundary(self):
        console.refresh_monitor_private(self.client)
        self.now += 300
        console.refresh_monitor_private(self.client)
        self.assertEqual(self.reader.call_count, 1)
        self.now += 1
        console.refresh_monitor_private(self.client)
        self.assertEqual(self.reader.call_count, 2)
        self.assertEqual(console.STATE['account']['source'], 'console_direct')

    def test_bad_snapshot_age_future_nan_infinite_and_old_are_rejected(self):
        for age in (-1, float('nan'), float('inf'), float('-inf'), 45.001, True, '1', None):
            with self.subTest(age=age):
                candidate = copy.deepcopy(self.worker)
                candidate['account_snapshot']['age_seconds'] = age
                self.assertIsNone(console.worker_account_view(candidate, self.configuration, 1))
        self.worker['account_snapshot']['age_seconds'] = 45
        self.assertIsNotNone(console.worker_account_view(self.worker, self.configuration, 1))

    def test_rpc_latency_counts_toward_45_second_freshness(self):
        self.worker['account_snapshot']['age_seconds'] = 43
        self.assertIsNone(console.worker_account_view(self.worker, self.configuration, 1, rpc_elapsed=3))
        account = console.worker_account_view(self.worker, self.configuration, 1, rpc_elapsed=2)
        self.assertEqual(account['age_seconds'], 45)

    def test_successful_snapshot_is_not_reused_during_fault_or_incomplete_reconciliation(self):
        changes = [{'reconciled': False}, {'connected': False}, {'state': 'waiting'},
                   {'state': 'recovering'}, {'state': 'fault'}, {'fault_reason': 'unresolved'},
                   {'unknown_orders': [{'id': 'unknown'}]}, {'loop_error': 'failure'},
                   {'execution_recovery': {'pending': True}},
                   {'funding_read': {'pending_recovery': True}},
                   {'network_retry': {'waiting': True}}, {'network_retry': {'probing': True}}]
        for change in changes:
            with self.subTest(change=change):
                candidate = copy.deepcopy(self.worker)
                candidate.update(change)
                self.assertIsNone(console.worker_account_view(candidate, self.configuration, 1))

    def test_wrong_snapshot_timestamp_or_nonfinite_balance_is_not_reused(self):
        for change in ({'reconciled_at_ms': 0}, {'reconciled_at_ms': 123},
                       {'reconciled_at_ms': True}, {'positions': None}, {'open_orders': {}},
                       {'account': {'availableBalance': 'nan'}}):
            with self.subTest(change=change):
                candidate = copy.deepcopy(self.worker)
                candidate['account_snapshot'].update(change)
                self.assertIsNone(console.worker_account_view(candidate, self.configuration, 1))

    def test_fallback_reads_are_serialized_and_limited_to_one_per_60_seconds(self):
        self.worker['reconciled'] = False
        console.refresh_monitor_private(self.client)
        self.now += 5
        console.refresh_monitor_private(self.client)
        self.assertEqual(self.reader.call_count, 1)
        self.assertFalse(console.STATE['account']['connected'])
        self.assertEqual(console.STATE['account']['source'], 'console_cached')
        self.assertEqual(console.STATE['account']['monitor_retry_in_seconds'], 55)
        self.now += 55
        console.refresh_monitor_private(self.client)
        self.assertEqual(self.reader.call_count, 2)
        self.assertEqual(console.STATE['account']['source'], 'console_direct')

    def test_cache_does_not_transfer_configuration_to_a_different_monitor_client(self):
        console.refresh_monitor_private(self.client)
        self.now += 5
        other_client = object()
        console.refresh_monitor_private(other_client)
        self.assertEqual(self.reader.call_count, 2)
        self.assertEqual(console.STATE['account']['source'], 'console_direct')

    def test_unknown_static_modes_or_expired_config_never_qualify_for_reuse(self):
        for age in (-1, 301, float('nan'), float('inf'), '1', True):
            with self.subTest(age=age):
                self.assertIsNone(console.worker_account_view(self.worker, self.configuration, age))
        config = copy.deepcopy(self.configuration)
        config.pop('can_trade')
        self.assertIsNone(console.worker_account_view(self.worker, config, 1))


if __name__ == '__main__':
    unittest.main()
