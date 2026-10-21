"""Independent strategy allocations and branch exits; fake exchange/temp books only."""
import copy
import sys
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from execution.engine import Engine, STRATEGY, STRATEGY_VERSION, LEDGER_META
from execution.store import Store
from execution.strategy import HOUR_MS, Signal
from test_hourly_engine import FakeExchange, trade_row, fill_row, raw_candle


class StrategyLedgerChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / 'fake-ledger.sqlite')
        self.client = FakeExchange()
        self.engine = Engine(self.client, self.store, activation_check=lambda: True)
        present = self.client.now
        self.client.now -= HOUR_MS
        self.engine._initialize_strategy_ledgers(['BTCUSDT', 'ETHUSDT'])
        self.engine.set_symbols({'BTCUSDT': True, 'ETHUSDT': True})
        self.client.now = present
        self.engine.state = 'running'  # Local simulated human start; no real service.
        self.engine.refresh_rules()
        self.client.calls.clear()

    def trade(self, identifier, symbol='BTCUSDT', **updates):
        record = self.store.meta(LEDGER_META)[symbol]
        value = trade_row(identifier, symbol=symbol, ledger_epoch=record['epoch'], **updates)
        self.store.save_trade(value)
        return value

    def fill(self, trade, identifier, purpose='entry', quantity='1', price='100', fee='.05', pnl='0', offset=1000):
        initialized = self.store.meta(LEDGER_META)[trade['symbol']]['initialized_at_ms']
        row = fill_row(identifier, identifier + 100, quantity, price, initialized + offset,
                       trade=trade['id'], purpose=purpose, fee=fee, pnl=pnl)
        row['symbol'] = trade['symbol']
        self.assertTrue(self.store.claim_fill(trade['symbol'], row))
        self.assertFalse(self.store.claim_fill(trade['symbol'], row))
        return row

    def income(self, symbol, identifier=1, value='-.02', offset=2000, **updates):
        initialized = self.store.meta(LEDGER_META)[symbol]['initialized_at_ms']
        row = dict(tranId=identifier, symbol=symbol, asset='USDT', incomeType='FUNDING_FEE',
                   income=value, time=initialized + offset)
        row.update(updates)
        self.assertTrue(self.store.claim_income(row))
        self.assertFalse(self.store.claim_income(row))
        return row

    def test_allocations_start_once_at_100_and_reboot_never_arms_or_resets(self):
        before = self.store.meta(LEDGER_META)
        btc = self.trade('btc-closed', status='closed')
        self.fill(btc, 1, fee='.05')
        self.fill(btc, 2, purpose='exit', price='102', fee='.051', pnl='2', offset=3000)
        self.engine._rebuild_strategy_ledgers()
        reboot = Engine(self.client, self.store)
        reboot._initialize_strategy_ledgers(['BTCUSDT', 'ETHUSDT'])
        self.assertEqual(reboot.state, 'stopped')
        self.assertEqual(D(reboot.strategy_ledgers['BTCUSDT']['wallet']), D('101.899'))
        self.assertEqual(D(reboot.strategy_ledgers['ETHUSDT']['wallet']), D('100'))
        for symbol in before:
            self.assertEqual(self.store.meta(LEDGER_META)[symbol]['epoch'], before[symbol]['epoch'])
            self.assertEqual(self.store.meta(LEDGER_META)[symbol]['initialized_at_ms'], before[symbol]['initialized_at_ms'])
        self.assertEqual(self.client.writes(), [])

    def test_fees_realized_pnl_and_same_numeric_fill_ids_stay_with_each_symbol(self):
        btc = self.trade('btc-closed', status='closed')
        eth = self.trade('eth-closed', symbol='ETHUSDT', status='closed')
        self.fill(btc, 1, fee='.05')
        self.fill(btc, 2, purpose='exit', price='98', fee='.049', pnl='-2', offset=3000)
        self.fill(eth, 1, fee='.10')
        self.fill(eth, 2, purpose='exit', price='103', fee='.103', pnl='3', offset=3000)
        self.assertEqual(self.engine._rebuild_strategy_ledgers(), [])
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['wallet']), D('97.901'))
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['wallet']), D('102.797'))
        self.assertEqual(self.client.account_value['totalWalletBalance'], '1000')
        self.engine._rebuild_strategy_ledgers()
        self.assertEqual(D(self.store.meta(LEDGER_META)['BTCUSDT']['wallet']), D('97.901'))

    def test_mark_unrealized_profit_is_symbol_specific_and_shared_account_is_ignored(self):
        btc = self.trade('btc-open', status='open', quantity='1', average='100')
        self.fill(btc, 1)
        self.client.account_value.update(totalWalletBalance='999999', totalUnrealizedProfit='77777')
        self.engine.positions = {'BTCUSDT': dict(positionAmt='1', entryPrice='100', unRealizedProfit='3')}
        self.assertEqual(self.engine._rebuild_strategy_ledgers(), [])
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['equity']), D('102.95'))
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['equity']), D('100'))
        self.engine.positions['BTCUSDT'] = dict(positionAmt='-1', entryPrice='100', markPrice='99')
        self.engine._rebuild_strategy_ledgers()
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['unrealized']), D('1'))

    def test_funding_is_deduplicated_and_assigned_only_to_owned_settlement_positions(self):
        btc = self.trade('btc-open', status='open', quantity='1')
        eth = self.trade('eth-open', symbol='ETHUSDT', status='open', quantity='1')
        self.fill(btc, 1)
        self.fill(eth, 1)
        self.income('BTCUSDT', identifier=1, value='-.02')
        self.income('ETHUSDT', identifier=1, value='.01')
        self.assertEqual(self.engine._rebuild_strategy_ledgers(), [])
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['wallet']), D('99.93'))
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['wallet']), D('99.96'))
        reboot = Engine(self.client, self.store)
        reboot._rebuild_strategy_ledgers()
        self.assertEqual(D(reboot.strategy_ledgers['BTCUSDT']['funding']), D('-.02'))
        self.assertEqual(reboot.strategy_ledgers['BTCUSDT']['funding_count'], 1)

    def test_funding_before_activation_does_not_change_the_new_allocation(self):
        self.income('BTCUSDT', value='1000', offset=-1)
        self.assertEqual(self.engine._rebuild_strategy_ledgers(), [])
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['wallet']), D('100'))

    def test_unowned_or_same_millisecond_funding_is_not_guessed(self):
        self.income('BTCUSDT')
        self.assertTrue(self.engine._rebuild_strategy_ledgers())
        self.assertFalse(self.engine.strategy_ledgers['BTCUSDT']['verified'])
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['wallet']), D('100'))
        btc = self.trade('btc-open', status='open')
        self.fill(btc, 1, offset=2000)
        self.assertTrue(self.engine._rebuild_strategy_ledgers())
        self.assertFalse(self.engine.strategy_ledgers['BTCUSDT']['verified'])

    def test_unpriced_commission_cannot_be_folded_into_usdt_wallet(self):
        btc = self.trade('btc-open')
        row = fill_row(1, 101, '1', '100', self.client.now, trade=btc['id'])
        row['commissionAsset'] = 'BNB'
        self.store.claim_fill('BTCUSDT', row)
        self.assertTrue(self.engine._rebuild_strategy_ledgers())
        self.assertFalse(self.engine.strategy_ledgers['BTCUSDT']['verified'])
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['wallet']), D('100'))

    def test_legacy_trade_order_and_profit_remain_untouched_and_are_not_reconciled(self):
        legacy = trade_row('old-private-book', strategy_id='HourlyBreakoutEMA200',
                           ledger_epoch='old-epoch', branch=None, status='open', quantity='1')
        self.store.save_trade(legacy)
        self.store.save_order(dict(id='old-unknown', trade_id=legacy['id'], symbol='BTCUSDT',
            kind='market', purpose='entry', state='unknown', client_id='old-client'))
        self.store.claim_fill('BTCUSDT', fill_row(1, 101, '1', '100', self.client.now,
            trade=legacy['id'], fee='100', pnl='99999'))
        self.engine.state = 'stopped'
        self.engine.tick()
        self.assertEqual(self.store.trade(legacy['id']), legacy)
        self.assertEqual(self.store.order('old-unknown')['state'], 'unknown')
        self.assertFalse(any(call[0] in {'query_order', 'query_algo'} for call in self.client.calls))
        self.assertEqual(self.client.writes(), [])
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['wallet']), D('100'))
        snapshot = self.engine.snapshot()
        self.assertEqual(snapshot['strategy'], STRATEGY)
        self.assertEqual(snapshot['trades'], [])
        self.assertEqual(snapshot['history'], [])
        self.assertEqual(snapshot['legacy_history'][0]['id'], legacy['id'])

    def test_live_position_outside_current_epoch_blocks_entry_without_adopting_it(self):
        self.client.position_rows = [dict(symbol='BTCUSDT', positionSide='BOTH', positionAmt='1',
                                         entryPrice='100', unRealizedProfit='1')]
        self.engine.reconcile()
        self.assertFalse(self.engine.reconciled)
        self.assertEqual(self.engine._trades(), [])
        self.assertEqual(self.client.writes(), [])

    def test_unknown_replacement_does_not_block_durable_exit_of_confirmed_owned_position(self):
        trade = self.trade('exit-despite-unknown', status='open', quantity='1', average='100',
            confirmed_stop='95', tightest_stop='95', exit_intent={'reason': '此前明确的退出意图'})
        self.fill(trade, 1)
        self.client.position_rows = [dict(symbol='BTCUSDT', positionSide='BOTH', positionAmt='1',
                                         entryPrice='100', unRealizedProfit='0')]
        raw = self.client.stop_row(400, 'known-protection', '95')
        self.client.algo_rows[400] = raw
        self.store.save_order(dict(id='known-protection', client_id='known-protection', kind='stop',
            purpose='protection', trade_id=trade['id'], symbol='BTCUSDT', order_id=400,
            state='confirmed', raw=raw))
        self.store.save_order(dict(id='unknown-replacement', client_id='unknown-replacement', kind='stop',
            purpose='protection', trade_id=trade['id'], symbol='BTCUSDT', state='unknown'))
        self.engine.tick()
        self.assertFalse(self.engine.reconciled)
        exits = [call for call in self.client.calls if call[0] == 'market_order']
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0][1][:3], ('BTCUSDT', 'SELL', D('1')))
        self.assertTrue(exits[0][2]['reduce_only'])
        self.assertFalse(any(call[0] == 'new_stop' for call in self.client.calls))
        self.client.now += 2000
        self.engine.tick()
        exits = [call for call in self.client.calls if call[0] == 'market_order']
        self.assertEqual(len(exits), 1)  # The open exit is queried, never submitted twice.
        self.assertTrue(any(call[0] == 'query_order' for call in self.client.calls))

    def signal(self, branch='trend'):
        return Signal(1000 * HOUR_MS, 1001 * HOUR_MS, 1, D('1'), D('100'), D('99'), D('99'), D('98'), branch)

    def test_entry_freezes_branch_and_uses_100_allocation_50percent_and_15_multiple(self):
        self.engine.reconciled = True
        self.engine._entry('BTCUSDT', self.signal('range'), True)
        active = self.engine._trades(True)
        self.assertEqual(len(active), 1)
        trade = active[0]
        self.assertEqual(trade['branch'], 'range')
        self.assertEqual(trade['signal_snapshot']['branch'], 'range')
        self.assertEqual(D(trade['equity0']), D('100'))
        self.assertEqual(D(trade['budget0']), D('50'))
        self.assertEqual(D(trade['notional0']), D('1500'))
        self.assertEqual(D(trade['initial_atr_multiplier']), D('5'))
        self.assertEqual(D(trade['trailing_atr_multiplier']), D('6'))
        self.assertEqual(trade['ledger_epoch'], self.store.meta(LEDGER_META)['BTCUSDT']['epoch'])
        self.assertEqual(len([call for call in self.client.calls if call[0] == 'market_order']), 1)

    def test_actual_account_margin_gate_rejects_even_when_independent_allocation_is_100(self):
        self.client.account_value['availableBalance'] = '.1'
        self.engine.reconciled = True
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.engine._trades(), [])
        self.assertEqual(self.client.writes(), [])
        self.assertTrue(any(event['details'].get('reason') == 'insufficient_available_margin'
                            for event in self.store.events()))

    def test_corrupted_initialization_does_not_reset_100_or_clear_history(self):
        records = self.store.meta(LEDGER_META)
        records['BTCUSDT']['initial_wallet'] = '200'
        self.store.set_meta(LEDGER_META, records)
        with self.assertRaises(RuntimeError):
            self.engine._initialize_strategy_ledgers(['BTCUSDT'])
        self.assertEqual(self.store.meta(LEDGER_META)['BTCUSDT']['initial_wallet'], '200')
        self.assertEqual(self.client.writes(), [])

    def test_paused_and_rebooted_engine_keep_human_permission_off(self):
        self.engine.pause()
        self.assertEqual(self.engine.state, 'paused')
        before = copy.deepcopy(self.store.meta(LEDGER_META))
        reboot = Engine(self.client, self.store)
        reboot.tick()
        self.assertEqual(reboot.state, 'stopped')
        self.assertEqual(self.client.writes(), [])
        for symbol in before:
            self.assertEqual(self.store.meta(LEDGER_META)[symbol]['epoch'], before[symbol]['epoch'])


class FrozenBranchExitChecks(unittest.TestCase):
    setUp = StrategyLedgerChecks.setUp
    trade = StrategyLedgerChecks.trade

    def hour(self, branch, direction, close, ema12, sma30):
        trade = self.trade('exit-check', branch=branch, direction=direction, status='open', quantity='1')
        boundary = 1001 * HOUR_MS
        self.store.save_candles('BTCUSDT', [raw_candle(1000, high='110', low='90', close=str(close))])
        self.engine.analyses['BTCUSDT'] = SimpleNamespace(ready=True, ema12=D(ema12), boll_mid=D(sma30),
            signal=SimpleNamespace(branch='range' if branch == 'trend' else 'trend'))
        self.engine.algo_map = {}
        self.engine._hourly_exits(boundary)
        return self.store.trade(trade['id'])

    def test_range_exit_uses_frozen_branch_despite_latest_signal_being_trend(self):
        trade = self.hour('range', 1, '101', '100', '100')
        self.assertIn('SMA30', trade['exit_intent']['reason'])
        self.assertEqual(trade['branch'], 'range')

    def test_trend_exit_uses_ema12_even_when_latest_signal_is_range(self):
        trade = self.hour('trend', 1, '99', '100', '100')
        self.assertIn('EMA12', trade['exit_intent']['reason'])

    def test_equal_prices_never_trigger_branch_exit(self):
        for branch in ('trend', 'range'):
            for direction in (1, -1):
                with self.subTest(branch=branch, direction=direction):
                    trade = self.hour(branch, direction, '100', '100', '100')
                    self.assertIsNone(trade['exit_intent'])

    def test_missing_frozen_branch_does_not_silently_select_an_exit(self):
        trade = self.hour(None, 1, '99', '100', '100')
        self.assertIsNone(trade['exit_intent'])
        self.assertNotEqual(self.engine.state, 'running')
        self.assertIn('分支', self.engine.fault_reason)


if __name__ == '__main__':
    unittest.main()
