"""Hand-calculated and causal checks for the replacement hourly strategy."""
import sys
import unittest
import random
from decimal import Decimal as D, localcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from execution.strategy import (
    HOUR_MS, Analysis, Candle, MarketRules, MarginTier, Signal,
    accumulate_actual_cost, analyze, ema_exit, entry_deviation_allowed,
    execution_window_open, initial_stop, order_rule_reason, plan_entry,
    reduction_target, risk_at_quantity, stop_touched, tighten_initial_stop,
    trailing_stop,
)


def candle(index, close="100", high=None, low=None, open_price=None):
    close = D(str(close))
    opening = close if open_price is None else D(str(open_price))
    high = max(opening, close) + 1 if high is None else D(str(high))
    low = min(opening, close) - 1 if low is None else D(str(low))
    return Candle(index * HOUR_MS, opening, high, low, close, (index + 1) * HOUR_MS - 1)


def rules(**changes):
    params = dict(tick_size=D("0.01"), qty_step=D("0.1"), min_qty=D("0.1"),
                  min_notional=D("5"), max_qty=D("1000"))
    params.update(changes)
    return MarketRules(**params)


class IndicatorAndSignalTests(unittest.TestCase):
    def test_ema_uses_first_50_mean_and_signal_candle(self):
        history = [candle(i, i + 1, low=D(i) + D(".5")) for i in range(51)]
        seed = analyze(history[:50], 50 * HOUR_MS)
        self.assertEqual(seed.ema, D("25.5"))
        next_bar = analyze(history, 51 * HOUR_MS)
        with localcontext() as context:
            context.prec = 40
            expected = D("25.5") + (D(2) / 51) * (51 - D("25.5"))
        self.assertEqual(next_bar.ema, expected)

    def test_atr_ignores_first_bar_tr_and_uses_first_14_valid_tr(self):
        history = [candle(0, "100", high="200", low="1")]
        history += [candle(i, "100", high=100 + i, low="100") for i in range(1, 16)]
        seeded = analyze(history[:15], 15 * HOUR_MS)
        self.assertEqual(seeded.atr, D("7.5"))
        updated = analyze(history, 16 * HOUR_MS)
        with localcontext() as context:
            context.prec = 40
            expected = (D("7.5") * 13 + 15) / 14
        self.assertEqual(updated.atr, expected)

    def test_first_1000_bars_do_not_trade_and_breakout_excludes_signal(self):
        history = [candle(i, D(100) + D('.01') * i) for i in range(999)] + [candle(999, "120")]
        warmup = analyze(history, 1000 * HOUR_MS)
        self.assertFalse(warmup.ready)
        self.assertEqual(warmup.signal.direction, 0)
        ready_history = [candle(i, D(100) + D('.01') * i) for i in range(1000)] + [candle(1000, "120")]
        result = analyze(ready_history, 1001 * HOUR_MS)
        self.assertTrue(result.ready)
        self.assertEqual(result.signal.direction, 1)
        self.assertEqual(result.signal.breakout_high, D("110.99"))
        self.assertEqual(result.signal.branch, "trend")
        self.assertGreater(result.ema, D("100"))
        self.assertGreater(result.atr, D("2"))

    def test_short_and_strict_breakout_equality(self):
        history = [candle(i, D(110) - D('.01') * i) for i in range(1000)]
        short = analyze(history + [candle(1000, "90", high="100", low="89")], 1001 * HOUR_MS)
        self.assertEqual(short.signal.direction, -1)
        equal = analyze(history + [candle(1000, "99.01")], 1001 * HOUR_MS)
        self.assertEqual(equal.signal.close, equal.signal.breakout_low)
        self.assertEqual(equal.signal.direction, 0)

    def test_only_exact_previous_12_hours_define_breakout_range(self):
        history = [candle(i, D(100) + D('.01') * i) for i in range(1000)]
        history[987] = candle(987, high="500")
        latest = candle(1000, "120")
        excluded = analyze(history + [latest], 1001 * HOUR_MS)
        self.assertEqual(excluded.signal.direction, 1)
        history[988] = candle(988, high="500")
        included = analyze(history + [latest], 1001 * HOUR_MS)
        self.assertEqual(included.signal.direction, 0)
        self.assertEqual(included.signal.breakout_high, D("500"))

    def test_ema_equality_does_not_enter_even_with_breakout(self):
        history = [candle(i, "105") for i in range(988)]
        history += [candle(i, "90") for i in range(988, 1000)]
        prior_ema = analyze(history, 1000 * HOUR_MS).ema
        result = analyze(history + [candle(1000, prior_ema)], 1001 * HOUR_MS)
        self.assertGreater(result.signal.close, result.signal.breakout_high)
        self.assertEqual(result.signal.close, result.signal.ema)
        self.assertEqual(result.signal.direction, 0)

    def test_inclusive_kline_close_ms_is_not_yet_complete(self):
        history = [candle(i) for i in range(1000)] + [candle(1000, "109")]
        result = analyze(history, 1001 * HOUR_MS - 1)
        self.assertEqual(result.candle_count, 1000)
        self.assertEqual(result.signal.hour_open_ms, 999 * HOUR_MS)
        self.assertFalse(result.ready)

    def test_missing_or_duplicate_completed_hours_block_signal(self):
        history = [candle(i) for i in range(1001)]
        for bad in (history[:500] + history[501:], history[:500] + [history[499]] + history[500:]):
            result = analyze(bad, 1001 * HOUR_MS)
            self.assertFalse(result.ready)
            self.assertEqual(result.reason, "candle_gap_or_duplicate")

    def test_prefix_is_identical_despite_future_values(self):
        history = [candle(i, D("100") + (i % 17) / D(10)) for i in range(1050)]
        cutoff = 1013 * HOUR_MS
        prefix = analyze(history[:1013], cutoff)
        full = analyze(history, cutoff)
        self.assertEqual(prefix, full)
        future = Candle(1050 * HOUR_MS, D("NaN"), D("NaN"), D("NaN"), D("NaN"), 1051 * HOUR_MS - 1)
        self.assertEqual(analyze(history + [future], cutoff), prefix)

    def test_five_minute_window_is_half_open(self):
        signal = Signal(0, HOUR_MS, 1, D("1"), D("100"), D("99"), D("99"), D("98"))
        self.assertFalse(execution_window_open(signal, HOUR_MS - 1))
        self.assertTrue(execution_window_open(signal, HOUR_MS))
        self.assertTrue(execution_window_open(signal, HOUR_MS + 299999))
        self.assertFalse(execution_window_open(signal, HOUR_MS + 300000))

    def test_reference_deviation_equality_allowed_and_both_thresholds(self):
        signal = Signal(0, HOUR_MS, 1, D("0.1"), D("100"), D("99"), D("99"), D("98"))
        self.assertTrue(entry_deviation_allowed(signal, D("100.5")))
        self.assertTrue(entry_deviation_allowed(signal, D("99.5")))
        self.assertFalse(entry_deviation_allowed(signal, D("100.5001")))
        wider = Signal(0, HOUR_MS, 1, D("1"), D("100"), D("99"), D("99"), D("98"))
        self.assertTrue(entry_deviation_allowed(wider, D("101")))
        self.assertFalse(entry_deviation_allowed(wider, D("101.0001")))


class PositionSizingAndProtectionTests(unittest.TestCase):
    def test_expected_prices_embed_each_side_slippage_once(self):
        plan = plan_entry("1000", "100", "1", 1, rules(), mark_price="100")
        self.assertTrue(plan.accepted)
        self.assertEqual(plan.equity, D("1000"))
        self.assertEqual(plan.budget, D("500"))
        self.assertEqual(plan.notional_cap, D("15000"))
        self.assertEqual(plan.expected_entry_price, D("100.02"))
        self.assertEqual(plan.stop_trigger, D("95.02"))
        self.assertEqual(plan.expected_stop_price, D("95.00"))
        self.assertEqual(plan.unit_loss, D("5.11751"))
        self.assertEqual(plan.quantity, D("97.7"))
        self.assertEqual(plan.initial_margin, D("195.4"))
        self.assertEqual(plan.estimated_open_fee, D("4.885977"))
        self.assertLessEqual(plan.quantity * plan.unit_loss, plan.budget)
        self.assertGreater((plan.quantity + D("0.1")) * plan.unit_loss, plan.budget)

    def test_notional_cap_and_short_adverse_prices(self):
        capped = plan_entry("1000", "100", "0.001", 1, rules(tick_size=D("0.000001")))
        self.assertTrue(capped.accepted)
        self.assertEqual(capped.quantity, D("149.9"))
        self.assertLessEqual(capped.notional, capped.notional_cap)
        short = plan_entry("1000", "100", "1", -1, rules())
        self.assertTrue(short.accepted)
        self.assertEqual(short.expected_entry_price, D("99.98"))
        self.assertEqual(short.stop_trigger, D("104.98"))
        self.assertEqual(short.expected_stop_price, D("105.01"))

    def test_bad_filters_margin_or_50x_tier_reject_without_enlargement(self):
        examples = [
            (rules(min_qty=D("100")), {}, "below_min_quantity"),
            (rules(min_notional=D("15000")), {}, "below_min_notional"),
            (rules(max_qty=D("4")), {}, "above_max_quantity"),
            (rules(), {"mark_price": "100000"}, "margin_exceeds_95_percent_equity"),
            (rules(), {"available_margin": "1"}, "insufficient_available_margin"),
            (rules(margin_tiers=(MarginTier(D(0), D(10000), 20),)), {}, "50x_not_supported_in_margin_tier"),
        ]
        for market_rules, kwargs, reason in examples:
            with self.subTest(reason=reason):
                plan = plan_entry("1000", "100", "1", 1, market_rules, **kwargs)
                self.assertFalse(plan.accepted)
                self.assertEqual(plan.reason, reason)
                self.assertEqual(plan.quantity, 0)

    def test_initial_rounding_and_partial_fill_never_widen(self):
        self.assertEqual(initial_stop("100.03", "1.001", 1, "0.1"), D("95.1"))
        self.assertEqual(initial_stop("100.03", "1.001", -1, "0.1"), D("105.0"))
        self.assertEqual(tighten_initial_stop("99", "100", "2", 1, "0.1"), D("99"))
        self.assertEqual(tighten_initial_stop("101", "100", "2", -1, "0.1"), D("101"))
        self.assertEqual(tighten_initial_stop("99", "111", "2", 1, "0.1"), D("101"))
        self.assertEqual(tighten_initial_stop("101", "89", "2", -1, "0.1"), D("99"))

    def test_trailing_excludes_whole_first_fill_hour(self):
        # Fill in hour 20 (20:00:02): hour 20 must never affect extrema.
        history = [candle(i) for i in range(20)] + [candle(20, "100", high="1000", low="1")]
        first_fill = 20 * HOUR_MS + 2000
        no_eligible = trailing_stop("90", 1, first_fill, history, rules(), now_ms=21 * HOUR_MS)
        self.assertEqual(no_eligible.eligible_count, 0)
        self.assertFalse(no_eligible.updated)
        first = trailing_stop("90", 1, first_fill, history + [candle(21)], rules(), now_ms=22 * HOUR_MS)
        self.assertEqual(first.eligible_count, 1)
        self.assertEqual(first.highest, D("101"))
        self.assertEqual(first.lowest, D("99"))
        self.assertEqual(first.last_hour_open_ms, 21 * HOUR_MS)

    def test_trail_only_tightens_and_new_stop_uses_latest_price(self):
        history = [candle(i) for i in range(25)]
        update = trailing_stop("80", 1, 20 * HOUR_MS + 2, history, rules(),
                               now_ms=25 * HOUR_MS, latest_trade_price="89")
        self.assertEqual(update.stop, D("89"))
        self.assertTrue(update.immediate_exit)  # equality triggers
        # Low=99 of historical hours is never treated as an old trigger.
        safe = trailing_stop("80", 1, 20 * HOUR_MS + 2, history, rules(),
                             now_ms=25 * HOUR_MS, latest_trade_price="100")
        self.assertFalse(safe.immediate_exit)
        held = trailing_stop("98", 1, 20 * HOUR_MS + 2, history, rules(), now_ms=25 * HOUR_MS)
        self.assertEqual(held.stop, D("98"))
        repeated = trailing_stop("90", 1, 20 * HOUR_MS + 2, history, rules(), now_ms=25 * HOUR_MS,
                                 last_processed_hour_open=24 * HOUR_MS)
        self.assertFalse(repeated.updated)
        self.assertEqual(repeated.stop, D("90"))
        short = trailing_stop("120", -1, 20 * HOUR_MS + 2, history, rules(),
                              now_ms=25 * HOUR_MS, latest_trade_price="111")
        self.assertEqual(short.stop, D("111"))
        self.assertTrue(short.immediate_exit)

    def test_ema_exit_is_strict_but_price_stop_includes_equality(self):
        self.assertFalse(ema_exit(1, "100", "100"))
        self.assertFalse(ema_exit(-1, "100", "100"))
        self.assertTrue(ema_exit(1, "99.99", "100"))
        self.assertTrue(ema_exit(-1, "100.01", "100"))
        self.assertTrue(stop_touched(1, "90", "90"))
        self.assertTrue(stop_touched(-1, "110", "110"))


class ActualRiskAndReductionTests(unittest.TestCase):
    def test_profit_does_not_refund_actual_fee_and_loss_budget(self):
        cost = accumulate_actual_cost("0", "1", "-3")
        self.assertEqual(cost, D("4"))
        cost = accumulate_actual_cost(cost, ".1", "100")
        self.assertEqual(cost, D("4.1"))
        self.assertEqual(accumulate_actual_cost(cost, "-.5", "5"), cost)

    def test_retained_quantity_includes_reduction_loss_and_fee(self):
        kwargs = dict(current_quantity="10", average_price="100", expected_reduce_price="95",
                      expected_stop_price="90", direction=1, fee_rate=".0005", actual_cost=".2")
        self.assertEqual(risk_at_quantity("5.8", **kwargs), D("79.660500"))
        target = reduction_target(**kwargs, budget="80", notional_cap="1000", rules=rules())
        self.assertTrue(target.accepted)
        self.assertEqual(target.quantity, D("5.8"))
        self.assertEqual(target.quantity_to_reduce, D("4.2"))
        self.assertLessEqual(target.expected_risk, D("80"))
        self.assertGreater(risk_at_quantity("5.9", **kwargs), D("80"))

    def test_negative_risk_slope_keeps_largest_feasible_quantity(self):
        kwargs = dict(current_quantity="10", average_price="100", expected_reduce_price="90",
                      expected_stop_price="99", direction=1, fee_rate=".0005", actual_cost="0")
        target = reduction_target(**kwargs, budget="40", notional_cap="800", rules=rules())
        self.assertTrue(target.accepted)
        self.assertEqual(target.quantity, D("8"))
        self.assertEqual(target.expected_risk, D("28.4860"))
        self.assertGreater(risk_at_quantity("6", **kwargs), D("40"))
        impossible = reduction_target(**kwargs, budget="40", notional_cap="600", rules=rules())
        self.assertTrue(impossible.full_close)
        self.assertFalse(impossible.accepted)

    def test_budget_consumed_requires_full_close_even_if_stop_would_profit(self):
        target = reduction_target(current_quantity="1", average_price="100", expected_reduce_price="110",
                                  expected_stop_price="105", direction=1, budget="10", notional_cap="1000",
                                  actual_cost="10", rules=rules())
        self.assertTrue(target.full_close)
        self.assertEqual(target.reason, "actual_budget_consumed")

    def test_reduce_only_waives_min_notional_but_opening_does_not(self):
        market_rules = rules(qty_step=D(".01"), min_qty=D(".01"), min_notional=D("100"))
        self.assertEqual(order_rule_reason(D(".04"), D("100"), market_rules), "below_min_notional")
        self.assertEqual(order_rule_reason(D(".04"), D("100"), market_rules, reduce_only=True), "")
        target = reduction_target(current_quantity=".2", average_price="100", expected_reduce_price="100",
                                  expected_stop_price="90", direction=1, budget=".5", notional_cap="1000",
                                  rules=market_rules)
        self.assertTrue(target.accepted)
        self.assertEqual(target.quantity, D(".04"))

    def test_reduction_order_itself_must_meet_quantity_minimum(self):
        target = reduction_target(current_quantity="1", average_price="100", expected_reduce_price="100",
                                  expected_stop_price="90", direction=1, budget="9.5", notional_cap="1000",
                                  fee_rate="0", rules=rules(min_qty=D(".2")))
        self.assertTrue(target.accepted)
        self.assertEqual(target.quantity, D(".8"))
        self.assertEqual(target.quantity_to_reduce, D(".2"))

    def test_short_risk_and_zero_slope(self):
        kwargs = dict(current_quantity="2", average_price="100", expected_reduce_price="105",
                      expected_stop_price="110", direction=-1, fee_rate="0", actual_cost="1")
        self.assertEqual(risk_at_quantity("1", **kwargs), D("16"))
        target = reduction_target(**kwargs, budget="16", notional_cap="1000", rules=rules())
        self.assertTrue(target.accepted)
        self.assertEqual(target.quantity, D("1"))
        flat = reduction_target(current_quantity="2", average_price="100", expected_reduce_price="90",
                                expected_stop_price="90", direction=1, fee_rate="0", actual_cost="1",
                                budget="20", notional_cap="1000", rules=rules())
        self.assertTrue(flat.full_close)

    def test_maximum_quantity_matches_independent_grid_enumeration(self):
        randomizer = random.Random(72)
        market_rules = rules(min_qty=D(".2"))
        for case in range(250):
            q0 = D(randomizer.randint(2, 100)) / 10
            average = D(randomizer.randint(70, 130))
            reduction = D(randomizer.randint(70, 130))
            stop = D(randomizer.randint(70, 130))
            direction = randomizer.choice((-1, 1))
            budget = D(randomizer.randint(1, 200))
            cap = D(randomizer.randint(1, 1500))
            cost = D(randomizer.randint(0, 20)) / 10
            feasible = []
            for index in range(2, int(q0 * 10) + 1):
                retained = D(index) / 10
                delta = q0 - retained
                risk = (cost + delta * (max(direction * (average - reduction), D(0)) + D('.0005') * reduction)
                        + retained * (max(direction * (average - stop), D(0)) + D('.0005') * stop))
                if (cost < budget and retained * average <= cap and risk <= budget
                        and (delta == 0 or delta >= market_rules.min_qty)):
                    feasible.append(retained)
            with self.subTest(case=case):
                result = reduction_target(current_quantity=q0, average_price=average,
                                          expected_reduce_price=reduction, expected_stop_price=stop,
                                          direction=direction, budget=budget, notional_cap=cap,
                                          actual_cost=cost, rules=market_rules)
                self.assertEqual(result.quantity, max(feasible, default=D(0)))


if __name__ == "__main__":
    unittest.main()
