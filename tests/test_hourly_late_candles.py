"""Late completed-hour regressions: real Engine/Store and a local exchange.

No service is launched and no external transport is constructed.  Candle
availability, account positions and exchange time are controlled by the test.
"""
import copy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]

from execution.engine import Engine, LEDGER_META, STRATEGY, STRATEGY_VERSION
from execution.store import Store
from execution.strategy import HOUR_MS
from test_hourly_engine import FakeExchange, fill_row, raw_candle, trade_row

SYMBOLS = ('BTCUSDT', 'ETHUSDT')


class ClosedHourMailbox:
    """A completed-hour-only mailbox with the production contiguous contract."""
    def __init__(self):
        self.rows = {symbol: {} for symbol in SYMBOLS}

    def publish(self, symbol, row):
        self.rows[symbol][int(row[0])] = copy.deepcopy(row)

    def closed_since(self, symbol, start_ms, boundary_ms):
        keys = list(range(int(start_ms), int(boundary_ms), HOUR_MS))
        if not keys or any(key not in self.rows[symbol] for key in keys):
            return []
        return [copy.deepcopy(self.rows[symbol][key]) for key in keys]


class LateCompletedHourTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.store = Store(Path(self.folder.name) / 'offline-late-hours.sqlite')
        self.previous_boundary = 1001 * HOUR_MS
        self.boundary = self.previous_boundary + HOUR_MS
        self.client = FakeExchange(now=self.previous_boundary + 1000)
        self.client.next_order = 1000  # Keep simulated exits distinct from entry IDs.
        self.client.streams = ClosedHourMailbox()
        self.store.set_meta(LEDGER_META, {
            symbol: {'strategy': STRATEGY, 'version': STRATEGY_VERSION,
                     'epoch': 'fixture-ledger-' + symbol,
                     'initialized_at_ms': 0, 'activation_boundary_ms': 0,
                     'initial_wallet': '100', 'wallet': '100'}
            for symbol in SYMBOLS
        })
        for symbol in SYMBOLS:
            self.store.save_candles(symbol, [raw_candle(i) for i in range(1001)])
        self.engine = Engine(self.client, self.store, activation_check=lambda: True,
                             monotonic=lambda: self.client.now / 1000)
        self.engine.set_symbols({symbol: True for symbol in SYMBOLS})
        self.engine._persist_intent(True, 'human_start')
        self.engine.state = 'running'
        self.engine.refresh_rules()
        self.client.calls.clear()
        self.client.now = self.boundary + 1000
        self.hourly_calls = []
        hourly = self.engine._hourly_exits

        def record_hourly(boundary, symbols=None):
            self.hourly_calls.extend((boundary, symbol) for symbol in
                                     (SYMBOLS if symbols is None else symbols))
            return hourly(boundary, symbols=symbols)

        self.hourly_patch = patch.object(self.engine, '_hourly_exits',
                                          side_effect=record_hourly)
        self.hourly_patch.start()
        self.addCleanup(self.hourly_patch.stop)

    def calls(self, name, symbol=None):
        return [call for call in self.client.calls if call[0] == name and
                (symbol is None or (call[1] and call[1][0] == symbol))]

    def publish(self, symbol, *, signal=False):
        row = raw_candle(1001, high='102' if signal else '101', low='99',
                         close='101.1' if signal else '100')
        self.client.streams.publish(symbol, row)
        return row

    def assert_no_entry_orders(self):
        self.assertFalse([call for call in self.calls('market_order')
                          if not call[2].get('reduce_only', False)])

    def held_short(self, symbol):
        number = 101 if symbol == 'BTCUSDT' else 102
        identifier = 'held-' + symbol
        first_fill = self.previous_boundary - 2 * HOUR_MS
        trade = trade_row(identifier, symbol=symbol, direction=-1, status='open',
                          quantity='1', average='100', fill_quantity='1',
                          fill_average='100', entry_atr='2', budget0='50',
                          notional0='1500', first_fill_ms=first_fill,
                          confirmed_stop='110', tightest_stop='110', branch='trend')
        self.store.save_trade(trade)
        fill = fill_row(1, number, '1', '100', first_fill, trade=identifier)
        fill['symbol'] = symbol
        self.store.claim_fill(symbol, fill)
        entry = dict(symbol=symbol, orderId=number, clientOrderId='entry-' + symbol,
                     status='FILLED', executedQty='1', avgPrice='100', origQty='1')
        self.client.order_rows[number] = entry
        self.client.fill_rows[number] = [copy.deepcopy(fill)]
        self.store.save_order(dict(id='entry-' + symbol, client_id='entry-' + symbol,
                                  trade_id=identifier, symbol=symbol, kind='market',
                                  purpose='entry', state='terminal', order_id=number,
                                  raw=entry, fills_complete=True))
        stop_id = number + 300
        stop = self.client.stop_row(stop_id, 'stop-' + symbol, '110',
                                    symbol=symbol, side='BUY')
        self.client.algo_rows[stop_id] = stop
        self.store.save_order(dict(id='stop-' + symbol, client_id='stop-' + symbol,
                                  trade_id=identifier, symbol=symbol, kind='stop',
                                  purpose='protection', state='confirmed',
                                  order_id=stop_id, raw=stop))
        self.client.position_rows.append(dict(symbol=symbol, positionSide='BOTH',
                                               positionAmt='-1', entryPrice='100',
                                               markPrice='100', unRealizedProfit='0'))
        return identifier

    def settle_short(self, symbol, identifier):
        number = 201 if symbol == 'BTCUSDT' else 202
        fill = fill_row(2, number, '1', '100', self.client.now,
                        trade=identifier, purpose='exit', fee='.05', pnl='0')
        fill['symbol'] = symbol
        self.store.claim_fill(symbol, fill)
        exit_order = dict(symbol=symbol, orderId=number,
                          clientOrderId='settled-exit-' + symbol, status='FILLED',
                          executedQty='1', avgPrice='100', origQty='1')
        self.client.order_rows[number] = exit_order
        self.client.fill_rows[number] = [copy.deepcopy(fill)]
        self.store.save_order(dict(id=exit_order['clientOrderId'],
                                  client_id=exit_order['clientOrderId'], symbol=symbol,
                                  trade_id=identifier, kind='market', purpose='exit',
                                  state='terminal', order_id=number, raw=exit_order,
                                  fills_complete=True))
        self.client.position_rows = [position for position in self.client.position_rows
                                     if position['symbol'] != symbol]
        for order in self.store.orders():
            if order['trade_id'] == identifier and order['kind'] == 'stop':
                raw = self.client.algo_rows[order['order_id']]
                raw['algoStatus'] = 'CANCELED'
                order.update(state='terminal', raw=copy.deepcopy(raw))
                self.store.save_order(order)
        trade = self.store.trade(identifier)
        trade.update(status='closed', quantity='0', fill_quantity='0',
                     closed_ms=self.client.now, last_exit_fill_ms=self.client.now,
                     confirmed_stop=None)
        self.store.save_trade(trade)

    def test_both_late_then_ws_arrival_completes_each_symbol_once(self):
        self.engine.tick()
        self.assertIsNone(self.store.signal('BTCUSDT', self.boundary))
        self.assertIsNone(self.store.signal('ETHUSDT', self.boundary))
        self.assertEqual(self.hourly_calls, [])
        initial_rest = {symbol: len(self.calls('klines', symbol)) for symbol in SYMBOLS}

        self.client.now += 2000
        self.publish('ETHUSDT')
        self.engine.tick()
        self.assertIsNotNone(self.store.signal('ETHUSDT', self.boundary))
        self.assertIsNone(self.store.signal('BTCUSDT', self.boundary))
        self.assertEqual(self.hourly_calls.count((self.boundary, 'ETHUSDT')), 1)
        self.assertEqual(self.hourly_calls.count((self.boundary, 'BTCUSDT')), 0)
        self.client.now += 2000
        self.engine.tick()
        self.assertEqual(self.hourly_calls.count((self.boundary, 'ETHUSDT')), 1)

        self.client.now += 2000
        self.publish('BTCUSDT')
        self.engine.tick()
        self.assertIsNotNone(self.store.signal('BTCUSDT', self.boundary))
        for symbol in SYMBOLS:
            self.assertEqual(self.hourly_calls.count((self.boundary, symbol)), 1)
            self.assertEqual(len(self.calls('klines', symbol)), initial_rest[symbol])
        self.assertEqual(len(self.calls('exchange_info')), 1)
        self.assertEqual(len(self.calls('leverage_brackets')), 1)
        self.assert_no_entry_orders()

    def test_successful_old_rest_response_is_throttled_but_retried_after_10_seconds(self):
        self.engine.tick()
        for symbol in SYMBOLS:
            self.assertEqual(len(self.calls('klines', symbol)), 1)
        for _ in range(4):
            self.client.now += 2000
            self.engine.tick()
        for symbol in SYMBOLS:
            self.assertEqual(len(self.calls('klines', symbol)), 1)
            self.assertIsNone(self.store.signal(symbol, self.boundary))
            self.client.candle_rows[symbol] = [raw_candle(1001)]
        self.client.now += 2000
        self.engine.tick()
        for symbol in SYMBOLS:
            self.assertEqual(len(self.calls('klines', symbol)), 2)
            self.assertIsNotNone(self.store.signal(symbol, self.boundary))
            self.assertEqual(self.hourly_calls.count((self.boundary, symbol)), 1)
        self.assertEqual(len(self.calls('exchange_info')), 1)
        self.assertEqual(len(self.calls('account')), 1)
        self.assertIsNone(self.engine.fault_reason)
        self.assert_no_entry_orders()

    def test_expired_late_signal_is_consumed_without_opening(self):
        self.engine.tick()
        self.client.now = self.boundary + 300000
        self.publish('ETHUSDT', signal=True)
        self.engine.tick()
        analysis = self.engine.analyses['ETHUSDT']
        self.assertNotEqual(analysis.signal.direction, 0)
        consumed = self.store.signal('ETHUSDT', self.boundary)
        self.assertIsNotNone(consumed)
        self.assertIn('窗口', consumed['skip_reason'])
        self.assert_no_entry_orders()
        self.client.now += 2000
        self.engine.tick()
        self.assertEqual(self.store.signal('ETHUSDT', self.boundary), consumed)
        self.assertEqual(self.hourly_calls.count((self.boundary, 'ETHUSDT')), 1)

    def test_after_five_minutes_held_position_still_gets_indicator_exit(self):
        identifier = self.held_short('ETHUSDT')
        self.engine.tick()
        self.assertIsNone(self.store.trade(identifier).get('exit_intent'))
        self.client.now = self.boundary + 300000
        self.publish('ETHUSDT', signal=True)
        self.engine.tick()
        self.assertIsNotNone(self.store.trade(identifier).get('exit_intent'))
        exits = self.calls('market_order', 'ETHUSDT')
        self.assertEqual(len(exits), 1)
        self.assertTrue(exits[0][2]['reduce_only'])
        self.assert_no_entry_orders()
        self.client.now += 2000
        self.engine.tick()
        self.assertEqual(len(self.calls('market_order', 'ETHUSDT')), 1)

    def test_position_closed_while_hour_data_waits_does_not_reverse_same_signal(self):
        identifier = self.held_short('ETHUSDT')
        self.engine.tick()
        self.client.now += 2000
        self.settle_short('ETHUSDT', identifier)
        self.publish('ETHUSDT', signal=True)
        self.engine.tick()
        self.assertEqual(self.store.trade(identifier)['status'], 'closed')
        self.assertNotEqual(self.engine.analyses['ETHUSDT'].signal.direction, 0)
        consumed = self.store.signal('ETHUSDT', self.boundary)
        self.assertIsNotNone(consumed)
        self.assertTrue(consumed['skip_reason'])
        self.assert_no_entry_orders()

    def test_ready_held_symbol_exits_before_other_held_symbol_data_arrives(self):
        btc = self.held_short('BTCUSDT')
        eth = self.held_short('ETHUSDT')
        self.engine.tick()
        self.client.now += 2000
        self.publish('ETHUSDT', signal=True)
        self.engine.tick()
        self.assertIsNotNone(self.store.trade(eth).get('exit_intent'))
        self.assertIsNone(self.store.trade(btc).get('exit_intent'))
        self.assertEqual(len(self.calls('market_order', 'ETHUSDT')), 1)
        self.assertEqual(len(self.calls('market_order', 'BTCUSDT')), 0)
        self.client.now += 2000
        self.publish('BTCUSDT', signal=True)
        self.engine.tick()
        self.assertIsNotNone(self.store.trade(btc).get('exit_intent'))
        self.assertEqual(len(self.calls('market_order', 'BTCUSDT')), 1)
        self.assert_no_entry_orders()

    def test_restart_clock_calibration_backstep_does_not_skip_next_real_hour(self):
        # The process reads a host clock one hour fast before Binance calibration.
        # Reproduce the actual startup order, rather than assigning cursor fields.
        self.client.now = self.boundary + HOUR_MS + 1000
        reboot = Engine(self.client, self.store, activation_check=lambda: True,
                        restore_saved_intent=True,
                        monotonic=lambda: self.client.now / 1000)
        calibrated = [False]

        def ensure_time():
            if not calibrated[0]:
                self.client.now = self.boundary + 1000
                calibrated[0] = True
            return self.client.now

        self.client.ensure_time_sync = ensure_time
        for symbol in SYMBOLS:
            self.client.streams.publish(symbol, raw_candle(1001))
        reboot.tick()
        self.assertEqual(reboot.state, 'running')
        self.assertEqual(reboot.last_boundary, self.boundary)
        self.assertEqual(reboot.analyses['ETHUSDT'].signal.boundary_ms, self.boundary)

        # The formerly future boundary is now real and must be processed once.
        next_boundary = self.boundary + HOUR_MS
        self.client.now = next_boundary + 1000
        for symbol in SYMBOLS:
            self.client.streams.publish(symbol, raw_candle(1002))
        reboot.tick()
        for symbol in SYMBOLS:
            self.assertEqual(reboot.analyses[symbol].signal.boundary_ms, next_boundary)
            self.assertIsNotNone(self.store.signal(symbol, next_boundary))
        self.assertEqual(reboot.last_boundary, next_boundary)
        self.assertEqual(self.client.writes(), [])


if __name__ == '__main__':
    unittest.main()
