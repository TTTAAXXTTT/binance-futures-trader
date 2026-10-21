"""Per-symbol human entry choices, with a fake exchange and disposable SQLite."""
import sys
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]

from execution.engine import Engine, ExecutionFault, LEDGER_META, STRATEGY, STRATEGY_VERSION
from execution.store import Store
from execution.strategy import Analysis, HOUR_MS, Signal
from test_hourly_engine import FakeExchange, trade_row, fill_row


OFF = {'BTCUSDT': False, 'ETHUSDT': False}
BTC_ONLY = {'BTCUSDT': True, 'ETHUSDT': False}
ETH_ONLY = {'BTCUSDT': False, 'ETHUSDT': True}
BOTH = {'BTCUSDT': True, 'ETHUSDT': True}


class HourlyHumanControlsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / 'human-controls-offline.sqlite3')
        self.client = FakeExchange()
        # Current trades must belong to the explicit per-symbol allocation.
        # Historical strategy rows remain excluded from management by design.
        self.store.set_meta(LEDGER_META, {symbol: {
            'strategy': STRATEGY, 'version': STRATEGY_VERSION,
            'epoch': 'fixture-ledger-' + symbol, 'initialized_at_ms': 0,
            'activation_boundary_ms': 0, 'initial_wallet': '100', 'wallet': '100'}
            for symbol in OFF})
        self.engine = Engine(self.client, self.store, activation_check=lambda: True)
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                                for symbol in OFF}

    def enabled_previous_hour(self, selected):
        now = self.client.now
        self.client.now -= HOUR_MS
        self.engine.set_symbols(selected)
        self.client.now = now
        self.engine.state = 'running'
        self.engine.reconciled = True

    def signal(self, direction=1):
        # The controls suite consumes an already-confirmed signal. Indicator
        # and ADX branch selection are checked in the separate math suite.
        boundary = self.client.now // HOUR_MS * HOUR_MS
        return Signal(boundary - HOUR_MS, boundary, direction, D('1'), D('100'),
                      D('99'), D('99'), D('98'), branch='trend')

    def incorrect_config_until_confirmed_write(self):
        configured = set()
        original_write = self.client.set_cross50
        def config(symbol):
            self.client._call('symbol_config', symbol)
            return [dict(symbol=symbol, marginType='CROSSED' if symbol in configured else 'ISOLATED',
                         leverage=50 if symbol in configured else 20)]
        def configure(symbol):
            result = original_write(symbol)
            configured.add(symbol)
            return result
        self.client.symbol_config, self.client.set_cross50 = config, configure

    def test_new_engine_defaults_both_off_and_stopped(self):
        snapshot = self.engine.snapshot()
        self.assertEqual(snapshot['symbol_enabled'], OFF)
        self.assertEqual(snapshot['state'], 'stopped')
        self.assertFalse(snapshot['warmed_up'])
        self.assertEqual(self.client.calls, [])

    def test_local_selection_persists_but_restart_never_restores_permission(self):
        reply = self.engine.set_symbols(BTC_ONLY)
        self.assertEqual(reply['symbol_enabled'], BTC_ONLY)
        self.assertEqual(self.client.calls, [])
        self.engine.state = 'running'
        reboot = Engine(self.client, self.store, activation_check=lambda: True)
        self.assertEqual(reboot.snapshot()['symbol_enabled'], BTC_ONLY)
        self.assertEqual(reboot.state, 'stopped')
        reboot.tick()
        self.assertEqual(reboot.state, 'stopped')
        self.assertEqual(self.client.writes(), [])

    def test_invalid_or_incomplete_mapping_rejected_without_changing_selection(self):
        invalid = [None, [], {}, {'BTCUSDT': True},
                   {'BTCUSDT': True, 'ETHUSDT': False, 'SOLUSDT': True},
                   {'BTCUSDT': 1, 'ETHUSDT': False},
                   {'BTCUSDT': True, 'ETHUSDT': 'false'}]
        self.engine.set_symbols(BTC_ONLY)
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ExecutionFault):
                self.engine.set_symbols(value)
            self.assertEqual(self.store.meta('symbol_enabled'), BTC_ONLY)
        self.assertEqual(self.client.calls, [])

    def test_corrupt_persisted_mapping_falls_back_to_off(self):
        self.store.set_meta('symbol_enabled', {'BTCUSDT': 'true', 'ETHUSDT': False})
        reboot = Engine(self.client, self.store, activation_check=lambda: True)
        self.assertEqual(reboot.snapshot()['symbol_enabled'], OFF)
        self.assertEqual(reboot.state, 'stopped')

    def test_start_with_default_off_requires_explicit_selection_before_exchange_calls(self):
        with self.assertRaises(ExecutionFault):
            self.engine.start()
        self.assertEqual(self.engine.state, 'stopped')
        self.assertEqual(self.client.calls, [])

    def test_start_only_changes_cross_and_leverage_for_selected_symbol(self):
        self.incorrect_config_until_confirmed_write()
        self.engine.analyses['ETHUSDT'] = Analysis(False, 'warmup', 200)
        response = self.engine.start(BTC_ONLY)
        self.assertEqual(response['state'], 'running')
        self.assertEqual(response['symbol_enabled'], BTC_ONLY)
        cross = [row for row in self.client.calls if row[0] == 'set_cross50']
        self.assertEqual([row[1][0] for row in cross], ['BTCUSDT'])
        configs = [row[1][0] for row in self.client.calls if row[0] == 'symbol_config']
        self.assertEqual(configs, ['BTCUSDT', 'BTCUSDT'])  # Re-read after the actual setting write.
        self.assertTrue(self.engine.snapshot()['warmed_up'])
        self.assertEqual(self.engine.last_boundary, self.client.now // HOUR_MS * HOUR_MS)

    def test_start_eth_only_and_no_arg_start_use_persisted_explicit_choice(self):
        self.incorrect_config_until_confirmed_write()
        self.engine.set_symbols(ETH_ONLY)
        self.engine.start()
        cross = [row for row in self.client.calls if row[0] == 'set_cross50']
        self.assertEqual([row[1][0] for row in cross], ['ETHUSDT'])
        self.assertEqual(self.engine.symbol_enabled, ETH_ONLY)

    def test_pause_is_global_and_selections_never_implicitly_resume(self):
        self.enabled_previous_hour(BTC_ONLY)
        reply = self.engine.pause()
        self.assertEqual(reply['state'], 'paused')
        self.assertEqual(reply['symbol_enabled'], BTC_ONLY)
        self.assertEqual(self.engine.state, 'paused')
        self.assertEqual(self.engine.snapshot()['symbol_enabled'], BTC_ONLY)
        self.engine.set_symbols(BOTH)
        self.assertEqual(self.engine.state, 'paused')
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.engine._entry('ETHUSDT', self.signal(), True)
        self.assertEqual(self.client.writes(), [])

    def test_disabled_hour_is_consumed_and_enabling_does_not_reuse_it(self):
        self.enabled_previous_hour(BTC_ONLY)
        signal = self.signal()
        self.engine._entry('ETHUSDT', signal, True)
        consumed = self.store.signal('ETHUSDT', signal.boundary_ms)
        self.assertIn('未启用', consumed['skip_reason'])
        self.engine.set_symbols(BOTH)
        self.engine._entry('ETHUSDT', signal, True)
        self.assertEqual(self.client.writes(), [])
        self.assertEqual(self.engine.state, 'paused')
        self.engine.start(BOTH)
        self.client.now += HOUR_MS
        self.engine._entry('ETHUSDT', self.signal(), True)
        market = [row for row in self.client.calls if row[0] == 'market_order']
        self.assertEqual(len(market), 1)
        self.assertEqual(market[0][1][0], 'ETHUSDT')
        self.assertFalse(market[0][2]['reduce_only'])

    def test_enabling_before_any_tick_still_waits_next_hour(self):
        self.engine.set_symbols(BTC_ONLY)
        self.engine.state = 'running'
        self.engine.reconciled = True
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.client.writes(), [])
        current = self.store.signal('BTCUSDT', self.signal().boundary_ms)
        self.assertTrue(current['skip_reason'])
        self.client.now += HOUR_MS
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(len([row for row in self.client.calls if row[0] == 'market_order']), 1)

    def test_stopping_one_symbol_keeps_the_other_choice_and_does_not_write_exchange(self):
        self.enabled_previous_hour(BOTH)
        trade = trade_row(status='open', quantity='1', average='100', actual_cost='.5')
        self.store.save_trade(trade)
        self.engine.set_symbols(ETH_ONLY)
        self.assertEqual(self.engine.state, 'running')
        self.assertEqual(self.engine.snapshot()['symbol_enabled'], ETH_ONLY)
        self.assertEqual(self.store.trade(trade['id']), trade)
        self.assertEqual(self.client.calls, [])
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.client.writes(), [])

    def test_enabling_new_symbol_while_running_requires_human_start_before_any_entry(self):
        self.enabled_previous_hour(BTC_ONLY)
        reply = self.engine.set_symbols(BOTH)
        self.assertEqual(reply['state'], 'paused')
        self.assertIn('再次点击启动', reply['message'])
        self.assertEqual(self.client.calls, [])
        self.client.now += HOUR_MS
        self.engine._entry('ETHUSDT', self.signal(), True)
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.client.writes(), [])
        self.engine.start(BOTH)
        self.assertEqual(self.engine.state, 'running')
        self.assertEqual([row[1][0] for row in self.client.calls if row[0] == 'set_cross50'],
                         [])  # Both settings are already confirmed; no redundant POST.
        self.client.now += HOUR_MS
        self.engine._entry('ETHUSDT', self.signal(), True)
        market = [row for row in self.client.calls if row[0] == 'market_order']
        self.assertEqual(len(market), 1)
        self.assertEqual(market[0][1][0], 'ETHUSDT')

    def test_disabled_existing_position_still_gets_protection_and_reduce_only_exit(self):
        self.engine.refresh_rules()
        first = self.client.now - HOUR_MS
        trade = trade_row(status='open', quantity='1', average='100', fill_quantity='1', first_fill_ms=first)
        self.store.save_trade(trade)
        self.assertTrue(self.engine._current_trade(trade))
        self.store.claim_fill('BTCUSDT', fill_row(1, 101, '1', '100', first))
        position = dict(symbol='BTCUSDT', positionSide='BOTH', positionAmt='1', entryPrice='100')
        self.client.position_rows = [position]
        self.engine.positions = {'BTCUSDT': position}
        self.engine.algo_map = {}
        self.engine.state = 'paused'
        self.engine.set_symbols(OFF)
        self.client.calls.clear()
        self.engine._manage_protection()
        self.assertEqual(len([row for row in self.client.calls if row[0] == 'new_stop']), 1)
        self.engine._exit_intent(self.store.trade(trade['id']), 'offline exit test')
        self.engine._risk_and_exit()
        markets = [row for row in self.client.calls if row[0] == 'market_order']
        self.assertEqual(len(markets), 1)
        self.assertEqual(markets[0][1][0], 'BTCUSDT')
        self.assertTrue(markets[0][2]['reduce_only'])
        self.assertEqual(self.engine.snapshot()['symbol_enabled'], OFF)

    def test_metadata_switch_off_during_intent_persistence_prevents_entry_wire(self):
        self.enabled_previous_hour(BTC_ONLY)
        trade = trade_row()
        self.store.save_trade(trade)
        save = self.store.save_order
        def disable_before_wire(value):
            save(value)
            if value['kind'] == 'market' and value['purpose'] == 'entry' and value['state'] == 'prepared':
                self.store.set_meta('symbol_enabled', OFF)
        with patch.object(self.store, 'save_order', side_effect=disable_before_wire):
            intent = self.engine._submit_market(trade, 'entry', D('1'))
        self.assertTrue(intent['locally_not_submitted'])
        self.assertEqual(intent['state'], 'terminal')
        self.assertEqual(self.client.writes(), [])
        self.assertEqual(self.engine.snapshot()['symbol_enabled'], OFF)

    def test_global_pause_during_intent_persistence_prevents_entry_wire(self):
        self.enabled_previous_hour(BTC_ONLY)
        trade = trade_row()
        self.store.save_trade(trade)
        save = self.store.save_order
        def pause_before_wire(value):
            save(value)
            if value['kind'] == 'market' and value['purpose'] == 'entry' and value['state'] == 'prepared':
                self.engine.pause()
        with patch.object(self.store, 'save_order', side_effect=pause_before_wire):
            intent = self.engine._submit_market(trade, 'entry', D('1'))
        self.assertTrue(intent['locally_not_submitted'])
        self.assertEqual(self.engine.state, 'paused')
        self.assertEqual(self.client.writes(), [])


if __name__ == '__main__':
    unittest.main()
