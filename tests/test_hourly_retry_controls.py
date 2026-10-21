"""Ten-minute read recovery preserves human controls; all exchange calls are fake."""
import asyncio
import copy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT / 'freqtrade'), str(ROOT), str(ROOT / 'tests')]

from control import hourly_app as console
from execution.binance import ApiError
from execution.engine import Engine
from execution.request_budget import NETWORK_RETRY_SECONDS
from execution.store import Store
from test_hourly_engine import FakeExchange


def network_status(*, waiting=True, probing=False, seconds=600):
    return {'version': 'ten-minute-retry-v1', 'interval_seconds': 600,
        'waiting': waiting, 'retry_in_seconds': seconds, 'retry_at_ms': 6600000,
        'failures': 1, 'last_failure_reason': '连接或读取超时', 'probing': probing}


def local_pending(seconds):
    error = ApiError(None, 'fixture stable shared recovery wait')
    error.network_backoff = True
    error.retry_in_seconds = seconds
    return error


class EngineReadRecoveryControlsTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.store = Store(Path(self.folder.name) / 'offline.sqlite')
        self.client = FakeExchange()
        # This group isolates retry behavior; first-install audit has dedicated replay tests.
        self.store.set_meta('funding_recheck_completed_ms', self.client.now)
        self.clock = 100.0
        self.engine = Engine(self.client, self.store, monotonic=lambda: self.clock)

    def test_network_fault_preserves_explicit_pause_and_coin_selection(self):
        self.engine.set_symbols({'BTCUSDT': False, 'ETHUSDT': True})
        self.engine.pause()
        self.engine.fault('fixture TLS read failed')
        self.assertEqual(self.engine.state, 'paused')
        self.assertEqual(self.store.meta('fault_reason'), 'fixture TLS read failed')
        self.assertEqual(self.engine.snapshot()['symbol_enabled'], {'BTCUSDT': False, 'ETHUSDT': True})
        self.assertEqual(self.client.writes(), [])

    def test_network_fault_never_changes_stopped_or_grants_permission(self):
        self.engine.fault('fixture connection failed')
        self.assertEqual(self.engine.state, 'stopped')
        self.engine.state = 'running'
        self.engine.fault('fixture connection failed')
        self.assertEqual(self.engine.state, 'fault')
        self.assertEqual(self.client.writes(), [])

    def test_network_snapshot_is_explicit_and_fake_client_without_api_compatible(self):
        self.assertIsNone(self.engine.snapshot()['network_retry'])
        self.client.network_status = lambda: network_status()
        self.assertEqual(self.engine.snapshot()['network_retry'], network_status())
        self.assertEqual(self.client.writes(), [])

    def test_funding_retries_exactly_every_600_seconds_until_success_without_resuming(self):
        self.assertEqual(NETWORK_RETRY_SECONDS, 600)
        self.engine.pause()
        self.engine.fault('fixture TLS funding error')
        cursor = self.client.now - 60000
        self.store.set_meta('funding_cursor', cursor)
        with patch.object(self.client, 'income', side_effect=[
                ApiError(None, 'fixture TLS error'), ApiError(None, 'fixture timeout'), []]) as income:
            with self.assertRaises(ApiError):
                self.engine._funding()
            self.clock = 699.0
            self.engine._funding()
            self.assertEqual(income.call_count, 1)
            self.assertEqual(self.store.meta('funding_cursor'), cursor)
            self.assertEqual(self.engine.snapshot()['funding_read']['interval_seconds'], 600)
            self.clock = 700.0
            with self.assertRaises(ApiError):
                self.engine._funding()
            self.assertEqual(income.call_count, 2)
            self.clock = 1299.0
            self.engine._funding()
            self.assertEqual(income.call_count, 2)
            self.clock = 1300.0
            self.engine._funding()
            self.assertEqual(income.call_count, 3)
        self.assertEqual(self.engine.income_failures, 0)
        self.assertFalse(self.engine.funding_pending_recovery)
        self.assertFalse(self.store.meta('funding_read_pending'))
        self.assertEqual(self.engine.state, 'paused')
        self.assertEqual(self.engine.fault_reason, 'fixture TLS funding error')
        self.assertEqual(self.client.writes(), [])

    def test_repeated_identical_connection_fault_is_not_logged_every_tick(self):
        self.engine.pause()
        before = len(self.store.events())
        for _ in range(15):
            self.engine.fault('fixture stable network wait reason')
        self.assertEqual(len(self.store.events()), before + 1)
        self.assertEqual(self.engine.state, 'paused')

    def test_late_funding_read_uses_remaining_shared_wait_without_counting_wire_failure(self):
        self.engine.pause()
        with patch.object(self.client, 'income', side_effect=[local_pending(73), []]) as income:
            with self.assertRaises(ApiError):
                self.engine._funding()
            self.assertEqual(self.engine.income_failures, 0)
            self.assertEqual(self.engine.income_retry_at, 173)
            self.assertTrue(self.engine.funding_pending_recovery)
            self.clock = 172
            self.engine._funding()
            self.assertEqual(income.call_count, 1)
            self.clock = 173
            self.engine._funding()
            self.assertEqual(income.call_count, 2)
        self.assertFalse(self.engine.funding_pending_recovery)
        self.assertEqual(self.engine.state, 'paused')
        self.assertEqual(self.client.writes(), [])


class ConsoleReadRecoveryControlsTests(unittest.TestCase):
    def test_network_wait_is_a_start_blocker_even_with_previous_connected_account(self):
        budget = {'cooldown_seconds': 0, 'network_retry': network_status(seconds=599)}
        with patch.object(console, 'RequestBudget') as constructor:
            constructor.return_value.snapshot.return_value = budget
            reasons = console.start_reasons({'connected': True, 'can_trade': True,
                'hedge_mode': False, 'multi_asset': False, 'equity_usdt': 100}, {}, {}, {})
        self.assertTrue(any('每10分钟自动重试' in reason and '599' in reason for reason in reasons))

    def test_probe_status_blocks_start_without_claiming_success(self):
        budget = {'cooldown_seconds': 0, 'network_retry': network_status(waiting=True, probing=True, seconds=30)}
        with patch.object(console, 'RequestBudget') as constructor:
            constructor.return_value.snapshot.return_value = budget
            reasons = console.start_reasons({'connected': False}, {}, {}, {})
        self.assertIn('正在重新读取币安接口，连接确认前暂停新开仓', reasons)
        self.assertIn('实盘账户尚未连接', reasons)

    def test_console_status_exposes_shared_network_wait_without_mutating_pause(self):
        network = network_status(seconds=431)
        budget = {'cooldown_seconds': 0, 'network_retry': network}
        worker = {'connected': True, 'state': 'paused', 'trades': [], 'unknown_orders': [],
                  'symbol_enabled': {'BTCUSDT': False, 'ETHUSDT': True}}
        with patch.dict(console.STATE, {'account': {'connected': False}, 'worker': worker,
                'markets': {}, 'events': []}, clear=True), \
                patch.object(console, 'RequestBudget') as constructor, \
                patch.object(console, 'trade_controls', return_value={'symbols': worker['symbol_enabled']}), \
                patch('control.installation.configured', return_value=True):
            constructor.return_value.snapshot.return_value = budget
            result = console.status()
        self.assertEqual(result['network_retry'], network)
        self.assertEqual(result['request_budget']['network_retry'], network)
        self.assertEqual(result['worker']['state'], 'paused')
        self.assertEqual(result['trade_controls']['symbols'], worker['symbol_enabled'])

    def test_preview_has_no_early_retry_then_retries_at_600_seconds(self):
        clock = {'value': 6000.0, 'loops': 0}
        async def advance_or_stop(interval):
            self.assertEqual(interval, 5)
            clock['loops'] += 1
            if clock['loops'] == 1:
                clock['value'] += 599
            elif clock['loops'] == 2:
                clock['value'] += 1
            else:
                raise asyncio.CancelledError()
        preview = Mock(side_effect=ApiError(None, 'fixture TLS read failed'))
        refresh = Mock(side_effect=lambda _: console.STATE.update(worker={'connected': False}))
        old = {symbol: {'symbol': symbol, 'price': 99} for symbol in console.SYMBOLS}
        with patch.dict(console.STATE, {'worker': {}, 'markets': copy.deepcopy(old)}, clear=True), \
                patch.object(console.compat, 'AccountClient', return_value=object()), \
                patch.object(console, 'refresh_monitor_private', refresh), \
                patch.object(console, 'market_snapshot', preview), \
                patch.object(console.time, 'time', side_effect=lambda: clock['value']), \
                patch.object(console.asyncio, 'sleep', side_effect=advance_or_stop):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(console.monitor())
            self.assertTrue(console.STATE['markets']['ETHUSDT']['stale'])
            self.assertEqual(console.STATE['markets']['ETHUSDT']['price'], 99)
        self.assertEqual(preview.call_count, 4)  # two symbols, only t=0 and t=600

    def test_late_preview_read_uses_remaining_shared_wait_instead_of_new_600_seconds(self):
        clock = {'value': 6000.0, 'loops': 0}
        async def advance_or_stop(interval):
            self.assertEqual(interval, 5)
            clock['loops'] += 1
            if clock['loops'] == 1:
                clock['value'] += 99
            elif clock['loops'] == 2:
                clock['value'] += 1
            else:
                raise asyncio.CancelledError()
        preview = Mock(side_effect=local_pending(100))
        refresh = Mock(side_effect=lambda _: console.STATE.update(worker={'connected': False}))
        with patch.dict(console.STATE, {'worker': {}, 'markets': {}}, clear=True), \
                patch.object(console.compat, 'AccountClient', return_value=object()), \
                patch.object(console, 'refresh_monitor_private', refresh), \
                patch.object(console, 'market_snapshot', preview), \
                patch.object(console.time, 'time', side_effect=lambda: clock['value']), \
                patch.object(console.asyncio, 'sleep', side_effect=advance_or_stop):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(console.monitor())
        self.assertEqual(preview.call_count, 4)  # only t=0 and original shared t=100 boundary

    def test_failed_account_read_preserves_snapshot_but_never_connected_or_running(self):
        budget = {'cooldown_seconds': 0, 'network_retry': network_status()}
        worker = {'connected': True, 'state': 'paused', 'trades': [], 'unknown_orders': [],
                  'symbol_enabled': {'BTCUSDT': False, 'ETHUSDT': True}}
        with patch.dict(console.STATE, {'account': {'connected': True, 'equity_usdt': 100},
                'worker': worker, 'markets': {}, 'events': []}, clear=True), \
                patch.object(console, 'RequestBudget') as constructor, \
                patch.object(console, 'worker_snapshot', return_value=copy.deepcopy(worker)), \
                patch.object(console.compat, 'read_account', side_effect=ApiError(None, 'fixture TLS error')):
            constructor.return_value.snapshot.return_value = budget
            console.refresh_private(object())
            self.assertFalse(console.STATE['account']['connected'])
            self.assertTrue(console.STATE['account']['stale'])
            self.assertEqual(console.STATE['account']['equity_usdt'], 100)
            self.assertEqual(console.STATE['worker']['state'], 'paused')
            self.assertEqual(console.STATE['worker']['symbol_enabled'], worker['symbol_enabled'])


if __name__ == '__main__':
    unittest.main()
