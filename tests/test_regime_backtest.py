"""Offline execution/accounting invariants; no historical result is fabricated."""
import sys
import unittest
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from execution.strategy import Analysis, Candle, HOUR_MS, MarginTier, MarketRules, Signal, risk_at_quantity
from research.regime_backtest import (
    FIVE_MS, Funding, Scenario, Simulator, acceptance, adverse, aggregate, assert_candles,
)

START = 1000 * HOUR_MS


def bar(t, price='100', high=None, low=None, duration=FIVE_MS):
    price = D(str(price))
    return Candle(t, price, D(str(high)) if high is not None else price + 1,
                  D(str(low)) if low is not None else price - 1, price, t + duration - 1)


def rules(symbol='BTCUSDT'):
    # Exchange price/market-quantity filters read during the 2026-10-12 audit.
    tick, minimum_notional, maximum_quantity = ('.10', '50', '120') if symbol == 'BTCUSDT' else ('.01', '20', '2000')
    return MarketRules(D(tick), D('.001'), D('.001'), D(minimum_notional), D(maximum_quantity),
                       (MarginTier(D('0'), D('1000000'), 50, D('.004'), D('0')),))


def analysis(boundary, direction=1, close='100', atr='1', ema12='99', mid='100', branch='trend'):
    signal = Signal(boundary - HOUR_MS, boundary, direction, D(atr), D(close), D('99'),
                    D('99'), D('98'), branch)
    return Analysis(True, '', 1001, D('99'), D(atr), signal, D('99'), D(ema12),
                    D('25') if branch == 'trend' else D('10'), D('40'), D(mid), D('102'), D('98'))


def fixture(hours_count=3):
    end = START + hours_count * HOUR_MS
    bars = [bar(t) for t in range(START, end, FIVE_MS)]
    hours = [bar(t, duration=HOUR_MS) for t in range(0, end, HOUR_MS)]
    return end, bars, list(bars), hours


class DataIntegrityTests(unittest.TestCase):
    def test_gap_and_duplicate_are_rejected_not_filled(self):
        rows = [bar(0), bar(FIVE_MS)]
        assert_candles(rows, 0, 2 * FIVE_MS, FIVE_MS, 'test')
        with self.assertRaises(ValueError):
            assert_candles([rows[0], rows[0]], 0, 2 * FIVE_MS, FIVE_MS, 'test')
        with self.assertRaises(ValueError):
            assert_candles([rows[0]], 0, 2 * FIVE_MS, FIVE_MS, 'test')

    def test_aggregation_preserves_first_open_extrema_last_close(self):
        rows = [bar(t, 100 + i, high=110 + i, low=90 - i)
                for i, t in enumerate(range(0, HOUR_MS, FIVE_MS))]
        result = aggregate(rows, FIVE_MS, HOUR_MS)
        self.assertEqual(len(result), 1)
        self.assertEqual((result[0].open, result[0].high, result[0].low, result[0].close),
                         (D('100'), D('121'), D('79'), D('111')))

    def test_missing_settlement_mark_is_rejected(self):
        with self.assertRaises(ValueError):
            Funding(1, D('.01'), D('0'))


class ExecutionAccountingTests(unittest.TestCase):
    def test_btc_and_eth_modeled_fills_follow_each_market_tick(self):
        prices = {
            ('BTCUSDT', 1): (D('60000.1'), D('500'), D('60012.2'), D('59988.0')),
            ('BTCUSDT', -1): (D('60000.1'), D('500'), D('59988.0'), D('60012.2')),
            ('ETHUSDT', 1): (D('3000.01'), D('29.88'), D('3000.62'), D('2999.40')),
            ('ETHUSDT', -1): (D('3000.01'), D('29.842'), D('2999.40'), D('3000.62')),
        }
        for (symbol, direction), (reference, atr, entry, exit_price) in prices.items():
            with self.subTest(symbol=symbol, direction=direction):
                end = START + HOUR_MS
                bars = [bar(t, reference) for t in range(START, end, FIVE_MS)]
                hours = [bar(t, reference, duration=HOUR_MS) for t in range(0, end, HOUR_MS)]
                market_rules = rules(symbol)
                model = Simulator(symbol, START, end, market_rules, Scenario('ticks'))
                result = model.run(bars, list(bars), hours, [],
                                   {START: analysis(START, direction, close=str(reference), atr=str(atr))})
                trade = result['trades'][0]
                q = trade['quantity']
                self.assertEqual(trade['entry_price'], entry)
                self.assertEqual(trade['exit_price'], exit_price)
                self.assertEqual(entry % market_rules.tick_size, 0)
                self.assertEqual(exit_price % market_rules.tick_size, 0)
                expected_net = direction * q * (exit_price - entry) - q * (entry + exit_price) * D('.0005')
                self.assertAlmostEqual(model.wallet - D('100'), expected_net, places=20)
                if symbol == 'ETHUSDT':
                    self.assertEqual(trade['opening_quantity'], D('.326'))

    def test_modeled_stop_exit_rounds_against_long_and_short(self):
        for direction, atr, trigger, expected_exit in (
            (1, '29.88', '2851.22', '2850.64'),
            (-1, '29.842', '3148.61', '3149.24'),
        ):
            with self.subTest(direction=direction):
                end = START + HOUR_MS
                bars = [bar(t, '3000.01') for t in range(START, end, FIVE_MS)]
                marks = list(bars)
                hours = [bar(t, '3000.01', duration=HOUR_MS) for t in range(0, end, HOUR_MS)]
                bars[1] = bar(START + FIVE_MS, '3000.01',
                              low=trigger if direction == 1 else '2999.01',
                              high=trigger if direction == -1 else '3001.01')
                model = Simulator('ETHUSDT', START, end, rules('ETHUSDT'), Scenario('stop_ticks'))
                result = model.run(bars, marks, hours, [],
                                   {START: analysis(START, direction, close='3000.01', atr=atr)})
                trade = result['trades'][0]
                self.assertEqual(trade['initial_stop'], D(trigger))
                self.assertEqual(trade['exit_reason'], 'stop_intrabar')
                self.assertEqual(trade['exit_price'], D(expected_exit))

    def execute(self, scenarios=None, signals=None, funding=None, change=None, hours_count=3):
        end, bars, marks, hours = fixture(hours_count)
        if change:
            change(bars, marks, hours)
        model = Simulator('BTCUSDT', START, end, rules(), scenarios or Scenario('test'))
        result = model.run(bars, marks, hours, funding or [], signals or {START: analysis(START)})
        return model, result

    def test_round_trip_slippage_is_in_prices_and_fees_debited_once(self):
        model, result = self.execute()
        trade = result['trades'][0]
        q = trade['opening_quantity']
        self.assertEqual(trade['entry_price'], D('100.1'))
        self.assertEqual(trade['exit_price'], D('99.9'))
        expected = q * (D('99.9') - D('100.1')) - q * (D('100.1') + D('99.9')) * D('.0005')
        self.assertAlmostEqual(model.wallet - D('100'), expected, places=20)
        self.assertEqual(trade['exit_reason'], 'period_end_forced_close')

    def test_funding_uses_actual_event_quantity_and_settlement_mark(self):
        events = [Funding(START, D('.01'), D('300')),
                  Funding(START + 200, D('.01'), D('150')),
                  Funding(START + FIVE_MS + 200, D('-.02'), D('80'))]
        model, result = self.execute(funding=events)
        q = result['trades'][0]['quantity']
        expected = -q * D('150') * D('.01') - q * D('80') * D('-.02')
        self.assertEqual(model.funding_total, expected)
        self.assertEqual(len([e for e in result['events'] if e['type'] == 'funding']), 2)

    def test_short_positive_rate_is_received(self):
        model, result = self.execute(signals={START: analysis(START, -1)},
                                     funding=[Funding(START + 200, D('.01'), D('150'))])
        self.assertEqual(model.funding_total, result['trades'][0]['quantity'] * D('150') * D('.01'))

    def test_existing_stop_prevents_using_same_boundary_for_reversal(self):
        def change(bars, marks, _):
            index = HOUR_MS // FIVE_MS
            bars[index] = marks[index] = bar(START + HOUR_MS, '94', high='95', low='93')
        signals = {START: analysis(START), START + HOUR_MS: analysis(START + HOUR_MS, -1, close='94', ema12='95')}
        model, result = self.execute(signals=signals, change=change)
        self.assertEqual(len(result['trades']), 1)
        self.assertEqual(result['trades'][0]['exit_reason'], 'stop_gap_at_open')
        self.assertEqual(model.skips['position_or_order_at_signal_boundary'], 1)
        self.assertEqual(result['trades'][0]['exit_price'], D('93.9'))

    def test_first_fill_hour_is_completely_excluded_from_trailing(self):
        def change(bars, marks, hours):
            hours[1000] = bar(START, high='130', low='99', duration=HOUR_MS)
        signals = {START: analysis(START), START + HOUR_MS: analysis(START + HOUR_MS, 0)}
        _, result = self.execute(signals=signals, change=change)
        self.assertFalse(any(e['type'] == 'stop_tighten' for e in result['events']))

    def test_trailing_update_does_not_retroactively_exit_previous_hour(self):
        def change(bars, marks, hours):
            hours[1001] = bar(START + HOUR_MS, high='108', low='96', duration=HOUR_MS)
            for index in range(24, 36):
                bars[index] = marks[index] = bar(START + index * FIVE_MS, '104', high='105', low='103')
        signals = {START: analysis(START), START + 2 * HOUR_MS: analysis(START + 2 * HOUR_MS, 0, close='104')}
        _, result = self.execute(signals=signals, change=change)
        tightened = [e for e in result['events'] if e['type'] == 'stop_tighten']
        self.assertEqual(len(tightened), 1)
        self.assertEqual(tightened[0]['new_stop'], D('102'))
        self.assertEqual(result['trades'][0]['exit_reason'], 'period_end_forced_close')

    def test_entry_branch_remains_range_after_later_trend_classification(self):
        signals = {START: analysis(START, branch='range', mid='101'),
                   START + HOUR_MS: analysis(START + HOUR_MS, 0, close='102', mid='101', ema12='200', branch='trend')}
        _, result = self.execute(signals=signals)
        self.assertEqual(result['trades'][0]['branch'], 'range')
        self.assertEqual(result['trades'][0]['exit_reason'], 'sma30_exit')

    def test_delay_uses_frozen_order_quantity_and_reduces_risk_after_fill(self):
        def change(bars, marks, _):
            for i in range(1, len(bars)):
                bars[i] = marks[i] = bar(START + i * FIVE_MS, '200', high='201', low='199')
        model, result = self.execute(Scenario('delayed', fill_delay_ms=FIVE_MS), change=change)
        trade = result['trades'][0]
        self.assertEqual(trade['first_fill_ms'], START + FIVE_MS + 100)
        self.assertLessEqual(trade['quantity'], trade['opening_quantity'])
        self.assertEqual(trade['equity0'], D('100'))
        self.assertEqual(trade['budget0'], D('50'))
        self.assertTrue(trade['adjustments'])
        risk = risk_at_quantity(trade['quantity'], current_quantity=trade['quantity'], average_price=trade['average'],
                                expected_reduce_price=D('199.9'),
                                expected_stop_price=D('195.0'), direction=1,
                                fee_rate=D('.0005'), actual_cost=trade['actual_cost'])
        self.assertLessEqual(risk, trade['budget0'])

    def test_submission_delay_at_five_minutes_expires_every_signal(self):
        model, result = self.execute(Scenario('expired', submit_delay_ms=FIVE_MS))
        self.assertEqual(result['metrics']['trades'], 0)
        self.assertEqual(result['metrics']['ending_equity_usdt'], D('100'))
        self.assertEqual(model.skips['submission_after_signal_expiry'], 1)

    def test_liquidation_is_explicit_loss_bound_not_synthetic_fill(self):
        def change(bars, marks, _):
            marks[1] = bar(START + FIVE_MS, '100', high='101', low='80')
        _, result = self.execute(change=change)
        self.assertEqual(result['metrics']['liquidation_loss_bounds'], 1)
        self.assertEqual(result['metrics']['ending_equity_usdt'], D('0'))
        self.assertFalse(result['metrics']['valid_for_acceptance'])
        self.assertIsNone(result['trades'][0]['exit_price'])

    def test_gap_stop_cannot_hide_simultaneous_mark_maintenance_crossing(self):
        def change(bars, marks, _):
            bars[1] = bar(START + FIVE_MS, '94', high='95', low='93')
            marks[1] = bar(START + FIVE_MS, '80', high='81', low='79')
        _, result = self.execute(change=change)
        self.assertEqual(result['trades'][0]['exit_reason'], 'stop_gap_at_open')
        self.assertFalse(result['metrics']['valid_for_acceptance'])
        self.assertEqual(result['metrics']['intrabar_ambiguities']['gap_stop_and_maintenance_barrier_same_open'], 1)

    def test_adverse_drawdown_bound_includes_closing_fees_and_close_drawdown(self):
        _, result = self.execute(Scenario('stress', slip=D('.001')))
        metrics = result['metrics']
        self.assertGreaterEqual(metrics['max_drawdown_intrabar_adverse_bound_pct'], metrics['max_drawdown_mark_close_pct'])
        self.assertGreater(metrics['max_drawdown_mark_close_pct'], 0)


class AcceptanceTests(unittest.TestCase):
    def test_payoff_ratio_uses_average_wins_and_average_loss_magnitude(self):
        model = Simulator('BTCUSDT', START, START + HOUR_MS, rules(), Scenario('test'))
        profits = [D('10'), D('20'), D('30'), D('-5'), D('-15')]
        model.wallet = D('140')
        model.price_pnl = D('40')
        model.closed = [{'net_profit': p, 'holding_hours': D('1'), 'exit_reason': 'test', 'branch': 'trend'} for p in profits]
        metrics = model.metrics()
        self.assertEqual(metrics['average_win_net_usdt'], D('20'))
        self.assertEqual(metrics['average_loss_net_usdt'], D('10'))
        self.assertEqual(metrics['payoff_ratio'], D('2'))
        self.assertEqual(metrics['profit_factor'], D('3'))
        self.assertEqual(metrics['largest_trade_net_loss_usdt'], D('15'))
        self.assertEqual(metrics['win_rate_pct'], D('60'))

    def test_abandoned_submission_count_is_separate_from_zero_trades(self):
        end, bars, marks, hours = fixture()
        model = Simulator('BTCUSDT', START, end, rules(), Scenario('expired', submit_delay_ms=FIVE_MS))
        result = model.run(bars, marks, hours, [], {START: analysis(START)})
        self.assertEqual(result['metrics']['signals_total'], 1)
        self.assertEqual(result['metrics']['signals_abandoned'], 1)
        self.assertEqual(result['metrics']['trades'], 0)

    def test_seven_wins_does_not_hide_negative_symbol_or_continuous_result(self):
        rows = []
        for symbol in ['BTCUSDT', 'ETHUSDT']:
            for i in range(4):
                rows.append({'symbol': symbol, 'scenario': 'baseline', 'window_type': 'independent_7d',
                             'net_profit_usdt': D('-100') if symbol == 'ETHUSDT' and i == 3 else D('1'),
                             'valid_for_acceptance': True})
            rows.append({'symbol': symbol, 'scenario': 'baseline', 'window_type': 'continuous_28d',
                         'net_profit_usdt': D('1'), 'valid_for_acceptance': True})
        gate = acceptance(rows, 'baseline')
        self.assertEqual(gate['positive_independent_windows'], 7)
        self.assertFalse(gate['passed'])
        next(r for r in rows if r['net_profit_usdt'] < 0)['net_profit_usdt'] = D('1')
        self.assertTrue(acceptance(rows, 'baseline')['passed'])
        rows[-1]['net_profit_usdt'] = D('-1')
        self.assertFalse(acceptance(rows, 'baseline')['passed'])


if __name__ == '__main__':
    unittest.main()
