"""Independent seed, strict-boundary and sizing checks for HourlyTrendRange."""
import sys
import unittest
from decimal import Decimal as D, localcontext
from fractions import Fraction as F
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from execution.strategy import (
    HOUR_MS, Analysis, Candle, MarketRules, Signal, adverse_execution_price, analyze, analyze_series,
    branch_exit, indicator_series, initial_stop, plan_entry, signal_direction,
    tighten_initial_stop, trailing_stop,
)


def bar(index, close="100", high=None, low=None):
    price = D(str(close))
    return Candle(index * HOUR_MS, price,
                  price + 1 if high is None else D(str(high)),
                  price - 1 if low is None else D(str(low)), price,
                  (index + 1) * HOUR_MS - 1)


def market_rules():
    return MarketRules(D(".01"), D(".01"), D(".01"), D("5"))


class RegimeIndicatorTests(unittest.TestCase):
    def assertFraction(self, observed, exact):
        with localcontext() as context:
            context.prec = 40
            expected = D(exact.numerator) / D(exact.denominator)
            self.assertLess(abs(observed - expected), D("1e-35"))

    def test_ema12_and_50_have_independent_mean_seeds(self):
        history = [bar(i, 100 + i) for i in range(51)]
        values = indicator_series(history)
        self.assertIsNone(values[10].ema12)
        self.assertEqual(values[11].ema12, D("105.5"))
        self.assertIsNone(values[48].ema50)
        self.assertEqual(values[49].ema50, D("124.5"))
        with localcontext() as context:
            context.prec = 40
            self.assertEqual(values[12].ema12, D("105.5") + D(2) / 13 * D("6.5"))
            self.assertEqual(values[50].ema50, D("124.5") + D(2) / 51 * D("25.5"))
        analysis = analyze(history, 51 * HOUR_MS)
        self.assertEqual(analysis.ema, analysis.ema50)
        self.assertEqual(analysis.ema12, values[-1].ema12)

    def test_adx_seed_is_mean_of_first14_dx_after_dm_seed(self):
        # Alternating +2/-1 closes produce +DM=2/-DM=1 and TR=3/2.
        # First 14 changes therefore seed ATR=2.5, +DM=1, -DM=.5.
        history, price = [bar(0)], D(100)
        for index in range(1, 30):
            price += D(2) if index % 2 else D(-1)
            history.append(bar(index, price))
        values = indicator_series(history)
        self.assertEqual(values[14].atr14, D("2.5"))
        self.assertIsNone(values[26].adx14)
        decay = F(13, 14)

        def dx(index):
            n = index - 14
            # Closed form geometric Wilder weights, independent of the loop
            # used by the implementation, with the known directional seeds.
            plus = decay ** n + sum((F(2, 14) * decay ** (index - k)
                                     for k in range(15, index + 1) if k % 2), F(0))
            minus = F(1, 2) * decay ** n + sum((F(1, 14) * decay ** (index - k)
                                               for k in range(15, index + 1) if not k % 2), F(0))
            return 100 * abs(plus - minus) / (plus + minus)

        first_adx = sum((dx(index) for index in range(14, 28)), F(0)) / 14
        self.assertFraction(values[27].adx14, first_adx)
        self.assertFraction(values[28].adx14, (first_adx * 13 + dx(28)) / 14)

    def test_directional_movement_tie_counts_neither_side(self):
        expanding = [bar(i, high=101 + i, low=99 - i) for i in range(30)]
        values = indicator_series(expanding)
        self.assertEqual(values[14].atr14, D("17"))
        self.assertEqual(values[27].adx14, D(0))
        self.assertEqual(values[29].rsi14, D(50))

    def test_rsi_wilder_seed_update_and_flat_gain_loss_edges(self):
        history, price = [bar(0)], D(100)
        for index in range(1, 16):
            price += D(2) if index % 2 else D(-1)
            history.append(bar(index, price))
        values = indicator_series(history)
        self.assertIsNone(values[13].rsi14)
        self.assertFraction(values[14].rsi14, F(200, 3))
        self.assertFraction(values[15].rsi14, F(3000, 43))
        self.assertEqual(indicator_series([bar(i) for i in range(30)])[-1].rsi14, D(50))
        self.assertEqual(indicator_series([bar(i, 100 + i) for i in range(30)])[-1].rsi14, D(100))
        self.assertEqual(indicator_series([bar(i, 100 - i) for i in range(30)])[-1].rsi14, D(0))

    def test_bollinger_uses_30_closes_population_deviation_and_signal_bar(self):
        history = [bar(i, 90 if i < 15 else 110) for i in range(30)]
        values = indicator_series(history)
        self.assertIsNone(values[28].boll_mid)
        self.assertEqual(values[29].boll_mid, D(100))
        self.assertEqual(values[29].boll_upper, D(115))
        self.assertEqual(values[29].boll_lower, D(85))
        changed = indicator_series(history[:-1] + [bar(29, 140)])[-1]
        self.assertEqual(changed.boll_mid, D(101))
        self.assertGreater(changed.boll_upper, values[29].boll_upper)

    def test_one_pass_matches_complete_prefixes_and_keeps_original_seeds(self):
        history = [bar(i, D(100) + D(i % 17) / 10) for i in range(1040)]
        series = analyze_series(history, 1040 * HOUR_MS)
        self.assertEqual(len(series), len(history))
        for index in (0, 11, 14, 27, 29, 49, 999, 1000, 1039):
            self.assertEqual(series[index], analyze(history[:index + 1], (index + 1) * HOUR_MS))
        self.assertEqual(analyze_series(history, 1001 * HOUR_MS), series[:1001])
        self.assertFalse(series[999].ready)
        self.assertEqual(series[999].signal.direction, 0)
        self.assertTrue(series[1000].ready)
        # A moving 1000-bar seed has a different EMA, and cannot replace the
        # stable seed merely because the caller wants fewer cached candles.
        rolling = analyze(history[40:], 1040 * HOUR_MS)
        self.assertNotEqual(series[-1].ema50, rolling.ema50)


class StrictBranchEntryAndExitTests(unittest.TestCase):
    def select(self, close="100", **updates):
        params = dict(ema50="99", adx14="20", rsi14="40", boll_mid="100.3",
                      boll_upper="101", boll_lower="100.1", breakout_high="99.5", breakout_low="98")
        params.update(updates)
        return signal_direction(close, **params)

    def test_adx20_chooses_trend_and_just_below_chooses_range(self):
        self.assertEqual(self.select(), (1, "trend"))
        self.assertEqual(self.select(adx14="19.999"), (1, "range"))
        self.assertEqual(self.select(adx14="20", ema50="100"), (0, "trend"))
        self.assertEqual(self.select(adx14="20", breakout_high="100"), (0, "trend"))
        self.assertEqual(self.select(ema50="101", breakout_low="100.5"), (-1, "trend"))
        self.assertEqual(self.select(ema50="101", breakout_low="100"), (0, "trend"))

    def test_range_long_strict_band_rsi_and_close_denominator_distance(self):
        self.assertEqual(self.select(adx14="19", rsi14="44.999"), (1, "range"))
        self.assertEqual(self.select(adx14="19", rsi14="45"), (0, "range"))
        self.assertEqual(self.select(adx14="19", boll_lower="100"), (0, "range"))
        self.assertEqual(self.select(adx14="19", boll_mid="100.2"), (0, "range"))
        # .2001/100 > .002, while .2001/100.2001 < .002. This separates
        # the required close-price denominator from an incorrect band one.
        self.assertEqual(self.select(adx14="19", boll_mid="100.2001"), (1, "range"))

    def test_range_short_strict_band_rsi_and_distance(self):
        params = dict(adx14="19", rsi14="55.001", boll_upper="99.9", boll_mid="99.7")
        self.assertEqual(self.select(**params), (-1, "range"))
        self.assertEqual(self.select(**dict(params, rsi14="55")), (0, "range"))
        self.assertEqual(self.select(**dict(params, boll_upper="100")), (0, "range"))
        self.assertEqual(self.select(**dict(params, boll_mid="99.8")), (0, "range"))
        self.assertEqual(self.select(**dict(params, boll_mid="99.7999")), (-1, "range"))

    def test_actual_oscillating_history_generates_range_signals(self):
        history = [bar(i, "99.7" if not i % 2 else "100.3") for i in range(1000)]
        for price, direction in (("99", 1), ("101.5", -1)):
            analysis = analyze(history + [bar(1000, price)], 1001 * HOUR_MS)
            self.assertTrue(analysis.ready)
            self.assertLess(analysis.adx14, D(20))
            self.assertEqual(analysis.signal.branch, "range")
            self.assertEqual(analysis.signal.direction, direction)
            self.assertEqual(analysis.sma30, analysis.boll_mid)

    def test_branch_specific_exit_is_strict_and_uses_entry_branch(self):
        for direction in (-1, 1):
            self.assertFalse(branch_exit(direction, "100", "trend", "100", "90"))
            self.assertFalse(branch_exit(direction, "100", "range", "90", "100"))
        self.assertTrue(branch_exit(1, "99", "trend", "100", "90"))
        self.assertTrue(branch_exit(-1, "101", "trend", "100", "110"))
        self.assertTrue(branch_exit(1, "101", "range", "110", "100"))
        self.assertTrue(branch_exit(-1, "99", "range", "90", "100"))
        self.assertFalse(branch_exit(1, "99", "trend", None, "90"))
        self.assertFalse(branch_exit(1, "101", "range", "110", None))
        with self.assertRaisesRegex(ValueError, "unknown_entry_branch"):
            branch_exit(1, "100", "unknown", "100", "100")

    def test_old_model_positional_fields_remain_compatible(self):
        signal = Signal(0, HOUR_MS, 1, D(1), D(100), D(99), D(98), D(97))
        analysis = Analysis(True, "", 1001, D(99), D(1), signal)
        self.assertEqual(signal.branch, "trend")
        self.assertIs(analysis.signal, signal)


class NewSizingAndLegacyProtectionTests(unittest.TestCase):
    def test_eth_long_tick_rounding_does_not_allow_the_next_quantity_step(self):
        rules = MarketRules(D('.01'), D('.001'), D('.001'), D('5'))
        plan = plan_entry('100', '3000.01', '29.88', 1, rules)
        self.assertTrue(plan.accepted)
        self.assertEqual(plan.expected_entry_price, D('3000.62'))
        self.assertEqual(plan.stop_trigger, D('2851.22'))
        self.assertEqual(plan.expected_stop_price, D('2850.64'))
        # Exact rational loss computed from separately specified tick prices.
        unit = F('3000.62') - F('2850.64') + F('.0005') * (F('3000.62') + F('2850.64'))
        self.assertEqual(F(plan.unit_loss), unit)
        self.assertEqual(plan.quantity, D('.326'))
        self.assertLessEqual(F(plan.quantity) * unit, F(50))
        self.assertEqual(F('.327') * unit, F('50.00014101'))
        self.assertGreater(F('.327') * unit, F(50))

    def test_eth_short_tick_rounding_does_not_allow_the_next_quantity_step(self):
        rules = MarketRules(D('.01'), D('.001'), D('.001'), D('5'))
        plan = plan_entry('100', '3000.01', '29.842', -1, rules)
        self.assertTrue(plan.accepted)
        self.assertEqual(plan.expected_entry_price, D('2999.4'))
        self.assertEqual(plan.stop_trigger, D('3148.61'))
        self.assertEqual(plan.expected_stop_price, D('3149.24'))
        unit = F('3149.24') - F('2999.4') + F('.0005') * (F('3149.24') + F('2999.4'))
        self.assertEqual(F(plan.unit_loss), unit)
        self.assertEqual(plan.quantity, D('.326'))
        self.assertLessEqual(F(plan.quantity) * unit, F(50))
        self.assertEqual(F('.327') * unit, F('50.00298264'))
        self.assertGreater(F('.327') * unit, F(50))

    def test_independent_caller_equity_freezes50percent15times(self):
        plan = plan_entry("100", "100", "1", 1, market_rules())
        self.assertEqual(plan.budget, D(50))
        self.assertEqual(plan.notional_cap, D(1500))
        self.assertEqual(plan.stop_trigger, D("95.02"))
        self.assertLessEqual(plan.quantity * plan.unit_loss, plan.budget)
        self.assertLessEqual(plan.notional, plan.notional_cap)
        doubled = plan_entry("200", "100", "1", 1, market_rules())
        self.assertEqual(doubled.budget, 2 * plan.budget)
        self.assertEqual(doubled.notional_cap, 2 * plan.notional_cap)

    def test_legacy_multiplier_is_explicit_and_never_widens(self):
        self.assertEqual(initial_stop("100", "1", 1, ".01"), D(95))
        self.assertEqual(initial_stop("100", "1", 1, ".01", atr_multiplier=2), D(98))
        self.assertEqual(tighten_initial_stop("99", "100", "1", 1, ".01", atr_multiplier=2), D(99))
        self.assertEqual(tighten_initial_stop("99", "100", "1", 1, ".01"), D(99))
        history = [bar(i) for i in range(25)]
        modern = trailing_stop("80", 1, 20 * HOUR_MS + 2, history, market_rules(), now_ms=25 * HOUR_MS)
        legacy = trailing_stop("80", 1, 20 * HOUR_MS + 2, history, market_rules(), now_ms=25 * HOUR_MS,
                               atr_multiplier=3)
        self.assertEqual(modern.stop, D(89))
        self.assertEqual(legacy.stop, D(95))
        with self.assertRaisesRegex(ValueError, "invalid_trailing_multiplier"):
            trailing_stop("80", 1, 20 * HOUR_MS, history, market_rules(), now_ms=25 * HOUR_MS, atr_multiplier=0)


class AdverseExecutionTickTests(unittest.TestCase):
    def test_each_buy_rounds_up_and_each_sell_rounds_down_after_slippage(self):
        cases = ((1, True, '3000.62'), (-1, True, '2999.40'),
                 (1, False, '2999.40'), (-1, False, '3000.62'))
        for direction, entry, expected in cases:
            with self.subTest(direction=direction, entry=entry):
                self.assertEqual(adverse_execution_price('3000.01', direction, '.01', entry=entry), D(expected))
        self.assertEqual(adverse_execution_price('100', 1, '.01', entry=True, slippage_rate=0), D(100))
        self.assertEqual(adverse_execution_price('3000.015', 1, '.01', entry=True, slippage_rate=0), D('3000.02'))
        self.assertEqual(adverse_execution_price('3000.015', 1, '.01', entry=False, slippage_rate=0), D('3000.01'))

    def test_invalid_inputs_and_rounding_to_zero_are_rejected(self):
        cases = (('NaN', 1, '.01', '.0002'), ('100', 0, '.01', '.0002'),
                 ('100', 1, 'NaN', '.0002'), ('100', 1, '0', '.0002'),
                 ('100', 1, '.01', '1'), ('100', 1, '.01', '-.01'))
        for price, direction, tick, slip in cases:
            with self.subTest(price=price, direction=direction, tick=tick, slip=slip):
                with self.assertRaises(ValueError):
                    adverse_execution_price(price, direction, tick, entry=True, slippage_rate=slip)
        with self.assertRaisesRegex(ValueError, 'non_positive_execution_price'):
            adverse_execution_price('.001', -1, '.01', entry=True)


if __name__ == "__main__":
    unittest.main()
