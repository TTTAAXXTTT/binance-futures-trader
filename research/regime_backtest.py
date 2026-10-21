"""Causal 5-minute historical execution for the fixed two-regime strategy.

This module imports only pure strategy arithmetic. It never opens credentials,
live ledgers, RPC endpoints or account APIs. OHLC fills are explicit scenarios,
not reconstructed historical orders. Funding uses individual published events.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal as D
from typing import Callable, Sequence

from execution.strategy import (
    Analysis, Candle, HOUR_MS, MarketRules, adverse_execution_price, analyze_series, branch_exit,
    ceil_step, entry_deviation_allowed, floor_step, initial_stop,
    margin_tier_for, plan_entry, reduction_target, risk_at_quantity,
    stop_touched,
)

FIVE_MS = 300_000
ZERO = D("0")
ONE = D("1")
EXECUTION_MS = 100


@dataclass(frozen=True)
class Funding:
    time_ms: int
    rate: D
    mark: D

    def __post_init__(self):
        object.__setattr__(self, "rate", D(str(self.rate)))
        object.__setattr__(self, "mark", D(str(self.mark)))
        if not self.rate.is_finite() or not self.mark.is_finite() or self.mark <= 0:
            raise ValueError("funding_requires_actual_positive_settlement_mark")


@dataclass(frozen=True)
class Scenario:
    name: str
    fee: D = D("0.0005")
    slip: D = D("0.0002")
    fill_delay_ms: int = 0
    submit_delay_ms: int = 0


SCENARIOS = (
    Scenario("baseline"),
    Scenario("slippage_stress_005pct", slip=D("0.0005")),
    Scenario("slippage_stress_010pct", slip=D("0.001")),
    Scenario("fill_delay_5m", fill_delay_ms=FIVE_MS),
    Scenario("submission_delay_5m_expired", submit_delay_ms=FIVE_MS),
)


def assert_candles(candles: Sequence[Candle], start: int, end: int, duration: int,
                   name: str) -> None:
    expected = (end - start) // duration
    if start % duration or end % duration or len(candles) != expected:
        raise ValueError(f"{name}: incomplete_range expected={expected} actual={len(candles)}")
    for index, item in enumerate(candles):
        if item.open_ms != start + index * duration:
            raise ValueError(f"{name}: missing_duplicate_or_out_of_order_candle")
        if item.close_ms not in (item.open_ms + duration - 1, item.open_ms + duration):
            raise ValueError(f"{name}: invalid_close_time")
        if any(not p.is_finite() or p <= 0 for p in (item.open, item.high, item.low, item.close)):
            raise ValueError(f"{name}: invalid_price")
        if item.low > min(item.open, item.close) or item.high < max(item.open, item.close):
            raise ValueError(f"{name}: invalid_ohlc")


def aggregate(candles: Sequence[Candle], source_ms: int, target_ms: int) -> list[Candle]:
    """Aggregate actual adjacent OHLC bars, rejecting partial groups and gaps."""
    if target_ms % source_ms or not candles:
        raise ValueError("invalid_aggregation")
    start = candles[0].open_ms
    end = candles[-1].open_ms + source_ms
    assert_candles(candles, start, end, source_ms, "aggregation_source")
    if start % target_ms or end % target_ms:
        raise ValueError("aggregation_partial_target_bar")
    count = target_ms // source_ms
    result = []
    for offset in range(0, len(candles), count):
        group = candles[offset:offset + count]
        result.append(Candle(group[0].open_ms, group[0].open,
                             max(item.high for item in group), min(item.low for item in group),
                             group[-1].close, group[0].open_ms + target_ms - 1))
    return result


def prepare_analyses(hours: Sequence[Candle], now_ms: int) -> dict[int, Analysis]:
    result = analyze_series(hours, now_ms)
    if len(result) != len(hours):
        raise ValueError("strategy_rejected_history")
    return {c.open_ms + HOUR_MS: a for c, a in zip(hours, result)}


def adverse(price: D, direction: int, slip: D, *, entry: bool, tick_size: D) -> D:
    return adverse_execution_price(price, direction, tick_size, entry=entry,
                                   slippage_rate=slip)


class Simulator:
    """One independent cross-margin asset wallet, initially flat and 100 USDT.

    Orders submitted at a boundary fill at a modeled +100 ms. Intrabar stop
    fills are timestamped at bar end: OHLC cannot reveal first-crossing time.
    Funding ambiguity and mark/stop ordering ambiguity are counted explicitly.
    Liquidation is a conservative full-wallet loss bound, not an invented
    exchange liquidation fill or fee; any such run fails the acceptance gate.
    """
    def __init__(self, symbol: str, start: int, end: int, rules: MarketRules,
                 scenario: Scenario, initial_equity=D("100")):
        self.symbol, self.start, self.end = symbol, start, end
        self.rules, self.scenario = rules, scenario
        self.initial = D(str(initial_equity))
        self.wallet = self.initial
        self.position = None
        self.pending_entry = None
        self.pending_exit = None
        self.closed = []
        self.events = []
        self.curve = []
        self.skips = Counter()
        self.peak = self.initial
        self.intrabar_peak_bound = self.initial
        self.max_drawdown = ZERO
        self.max_intrabar_drawdown_bound = ZERO
        self.lowest_margin_buffer = None
        self.max_notional_equity = ZERO
        self.liquidations = 0
        self.ambiguities = Counter()
        self.funding_total = ZERO
        self.fees_total = ZERO
        self.price_pnl = ZERO
        self.exposure_bars = 0
        self.halted = False
        self.total_signals = 0

    def equity(self, mark: D) -> D:
        p = self.position
        return self.wallet if p is None else self.wallet + p['direction'] * p['quantity'] * (mark - p['average'])

    def _fund(self, event: Funding) -> None:
        p = self.position
        if p is None or p['first_fill_ms'] >= event.time_ms:
            return
        payment = -p['direction'] * p['quantity'] * event.mark * event.rate
        self.wallet += payment
        self.funding_total += payment
        p['funding'] += payment
        self.events.append({'time_ms': event.time_ms, 'type': 'funding', 'quantity': p['quantity'],
                            'settlement_mark': event.mark, 'rate': event.rate, 'wallet_delta': payment})

    def _close(self, reference: D, time_ms: int, reason: str) -> None:
        p = self.position
        if p is None:
            return
        execution = adverse(reference, p['direction'], self.scenario.slip, entry=False,
                            tick_size=self.rules.tick_size)
        fee = p['quantity'] * execution * self.scenario.fee
        gross = p['direction'] * p['quantity'] * (execution - p['average'])
        self.wallet += gross - fee
        self.fees_total += fee
        self.price_pnl += gross
        p['fees'] += fee
        p['gross_price_pnl'] += gross
        p.update(exit_ms=time_ms, exit_price=execution, exit_reason=reason,
                 net_profit=p['gross_price_pnl'] - p['fees'] + p['funding'], wallet_after=self.wallet,
                 holding_hours=D(time_ms - p['first_fill_ms']) / HOUR_MS)
        self.closed.append(dict(p))
        self.events.append({'time_ms': time_ms, 'type': 'exit', 'reason': reason, 'price': execution,
                            'quantity': p['quantity'], 'fee': fee})
        self.position = self.pending_exit = None

    def _liquidate_bound(self, mark: D, time_ms: int, ambiguous: bool) -> None:
        p = self.position
        if p is None:
            return
        self.liquidations += 1
        if ambiguous:
            self.ambiguities['liquidation_and_contract_stop_same_5m'] += 1
        # No historical liquidation-order book/insurance fee is available.
        # End at zero as an explicitly conservative account-loss bound.
        residual = -self.wallet
        p['gross_price_pnl'] += residual
        self.price_pnl += residual
        self.wallet = ZERO
        p.update(exit_ms=time_ms, exit_price=None, exit_reason='liquidation_full_wallet_loss_bound',
                 liquidation_mark=mark, liquidation_loss_bound_adjustment=residual,
                 net_profit=-p['equity0'], wallet_after=ZERO,
                 holding_hours=D(time_ms - p['first_fill_ms']) / HOUR_MS)
        self.closed.append(dict(p))
        self.position = self.pending_entry = self.pending_exit = None
        self.halted = True

    def _margin(self, mark: D) -> tuple[D, D]:
        p = self.position
        if p is None:
            return self.wallet, ZERO
        notional = p['quantity'] * mark
        tier = margin_tier_for(notional, self.rules)
        if tier is None:
            raise ValueError("missing_maintenance_tier_for_position")
        maintenance = max(notional * tier.maintenance_rate - tier.maintenance_amount, ZERO)
        equity = self.equity(mark)
        buffer = equity - maintenance
        if self.lowest_margin_buffer is None or buffer < self.lowest_margin_buffer:
            self.lowest_margin_buffer = buffer
        if equity > 0:
            self.max_notional_equity = max(self.max_notional_equity, notional / equity)
        return equity, maintenance

    def _fill_entry(self, bar: Candle, mark: Candle, time_ms: int) -> None:
        pending = self.pending_entry
        self.pending_entry = None
        if pending is None or self.position is not None or self.halted:
            return
        sig, plan = pending['signal'], pending['plan']
        quantity = plan.quantity
        entry = adverse(bar.open, sig.direction, self.scenario.slip, entry=True,
                        tick_size=self.rules.tick_size)
        fee = quantity * entry * self.scenario.fee
        stop = initial_stop(entry, sig.atr, sig.direction, self.rules.tick_size)
        self.wallet -= fee
        self.fees_total += fee
        self.position = {
            'symbol': self.symbol, 'branch': sig.branch, 'direction': sig.direction,
            'entry_boundary_ms': sig.boundary_ms, 'submitted_ms': pending['submitted_ms'],
            'first_fill_ms': time_ms, 'entry_price': entry, 'average': entry,
            'opening_quantity': quantity, 'quantity': quantity, 'entry_atr': sig.atr,
            'equity0': plan.equity, 'budget0': plan.budget, 'notional0': plan.notional_cap,
            'confirmed_stop': stop, 'initial_stop': stop, 'fees': fee, 'funding': ZERO,
            'gross_price_pnl': ZERO, 'actual_cost': fee, 'adjustments': [],
        }
        self.events.append({'time_ms': time_ms, 'type': 'entry', 'price': entry, 'quantity': quantity,
                            'branch': sig.branch, 'fee': fee, 'stop': stop})
        p = self.position
        stop_execution = adverse(stop, sig.direction, self.scenario.slip, entry=False,
                                 tick_size=self.rules.tick_size)
        risk = risk_at_quantity(quantity, current_quantity=quantity, average_price=entry,
            expected_reduce_price=adverse(bar.open, sig.direction, self.scenario.slip, entry=False,
                                          tick_size=self.rules.tick_size),
            expected_stop_price=stop_execution, direction=sig.direction,
            fee_rate=self.scenario.fee, actual_cost=fee)
        if quantity * mark.open / 50 + fee > plan.equity * D('.95'):
            self._close(bar.open, time_ms, 'post_fill_margin_over_limit')
            return
        if risk > plan.budget or quantity * entry > plan.notional_cap:
            target = reduction_target(current_quantity=quantity, average_price=entry,
                expected_reduce_price=adverse(bar.open, sig.direction, self.scenario.slip, entry=False,
                                              tick_size=self.rules.tick_size),
                expected_stop_price=stop_execution, direction=sig.direction, budget=plan.budget,
                notional_cap=plan.notional_cap, rules=self.rules, fee_rate=self.scenario.fee, actual_cost=fee)
            if target.full_close or not target.accepted:
                self._close(bar.open, time_ms, 'post_fill_no_compliant_quantity')
                return
            delta = quantity - target.quantity
            if delta > 0:
                price = adverse(bar.open, sig.direction, self.scenario.slip, entry=False,
                                tick_size=self.rules.tick_size)
                reduction_fee = delta * price * self.scenario.fee
                pnl = sig.direction * delta * (price - entry)
                self.wallet += pnl - reduction_fee
                self.fees_total += reduction_fee
                self.price_pnl += pnl
                p['fees'] += reduction_fee
                p['gross_price_pnl'] += pnl
                p['actual_cost'] += reduction_fee + max(-pnl, ZERO)
                p['quantity'] = target.quantity
                p['adjustments'].append({'time_ms': time_ms, 'quantity_reduced': delta,
                                          'price': price, 'fee': reduction_fee, 'realized_price_pnl': pnl})
        self._margin(mark.open)

    def _hour(self, time_ms: int, bar: Candle, mark: Candle, analysis: Analysis | None,
              hours: Sequence[Candle], owned_at_boundary: bool) -> None:
        if analysis is None or not analysis.ready or not analysis.signal:
            return
        p = self.position
        if p is not None:
            eligible_start = (p['first_fill_ms'] // HOUR_MS + 1) * HOUR_MS
            eligible = [c for c in hours if eligible_start <= c.open_ms and c.open_ms + HOUR_MS <= time_ms]
            if eligible and analysis.atr is not None:
                raw = (max(c.high for c in eligible) - 6 * analysis.atr if p['direction'] == 1
                       else min(c.low for c in eligible) + 6 * analysis.atr)
                candidate = ceil_step(raw, self.rules.tick_size) if p['direction'] == 1 else floor_step(raw, self.rules.tick_size)
                old = p['confirmed_stop']
                new = max(old, candidate) if p['direction'] == 1 else min(old, candidate)
                if new != old:
                    p['confirmed_stop'] = new
                    self.events.append({'time_ms': time_ms, 'type': 'stop_tighten', 'old_stop': old, 'new_stop': new})
                    if stop_touched(p['direction'], bar.open, new):
                        self._close(bar.open, time_ms + EXECUTION_MS, 'trailing_immediate_exit')
            p = self.position
            if p is not None and branch_exit(p['direction'], analysis.signal.close, p['branch'],
                                             analysis.ema12, analysis.boll_mid):
                if self.pending_exit is None:
                    self.pending_exit = {'due_ms': time_ms + self.scenario.fill_delay_ms,
                                         'reason': 'ema12_exit' if p['branch'] == 'trend' else 'sma30_exit'}
                if self.pending_exit['due_ms'] == time_ms:
                    self._close(bar.open, time_ms + EXECUTION_MS, self.pending_exit['reason'])
        sig = analysis.signal
        if sig.direction == 0:
            return
        self.total_signals += 1
        if owned_at_boundary or self.position is not None or self.pending_entry or self.pending_exit:
            self.skips['position_or_order_at_signal_boundary'] += 1
            return
        if self.halted or self.wallet <= 0:
            self.skips['wallet_unavailable'] += 1
            return
        if self.scenario.submit_delay_ms >= FIVE_MS:
            self.skips['submission_after_signal_expiry'] += 1
            return
        if not entry_deviation_allowed(sig, bar.open):
            self.skips['entry_deviation'] += 1
            return
        plan = plan_entry(self.wallet, bar.open, sig.atr, sig.direction, self.rules,
                          mark_price=mark.open, available_margin=self.wallet,
                          fee_rate=self.scenario.fee, slippage_rate=self.scenario.slip)
        if not plan.accepted:
            self.skips[plan.reason] += 1
            return
        self.pending_entry = {'signal': sig, 'plan': plan, 'submitted_ms': time_ms,
                              'due_ms': time_ms + self.scenario.fill_delay_ms}
        if not self.scenario.fill_delay_ms:
            self._fill_entry(bar, mark, time_ms + EXECUTION_MS)

    def run(self, bars: Sequence[Candle], marks: Sequence[Candle], hours: Sequence[Candle],
            funding: Sequence[Funding], analyses: dict[int, Analysis] | None = None) -> dict:
        assert_candles(bars, self.start, self.end, FIVE_MS, 'execution_5m')
        assert_candles(marks, self.start, self.end, FIVE_MS, 'mark_5m')
        assert_candles(hours, self.start - 1000 * HOUR_MS, self.end, HOUR_MS, 'warmup_and_signal_1h')
        if analyses is None:
            analyses = prepare_analyses(hours, self.end)
        events = sorted(funding, key=lambda item: item.time_ms)
        if len({event.time_ms for event in events}) != len(events):
            raise ValueError('duplicate_funding_event')
        events = [event for event in events if self.start <= event.time_ms < self.end]
        fund_index = 0
        for bar, mark in zip(bars, marks):
            t = bar.open_ms
            owned = bool(self.position or self.pending_entry or self.pending_exit)
            # Actual settlement timestamps commonly fall a few ms after the
            # boundary. They precede our explicitly modeled 100ms market fill.
            while fund_index < len(events) and events[fund_index].time_ms <= t + EXECUTION_MS:
                self._fund(events[fund_index])
                fund_index += 1
            p = self.position
            if p is not None and stop_touched(p['direction'], bar.open, p['confirmed_stop']):
                opening_equity, opening_maintenance = self._margin(mark.open)
                if opening_equity <= opening_maintenance:
                    self.ambiguities['gap_stop_and_maintenance_barrier_same_open'] += 1
                self._close(bar.open, t + EXECUTION_MS, 'stop_gap_at_open')
            if self.position is not None:
                equity, maintenance = self._margin(mark.open)
                if equity <= maintenance:
                    self._liquidate_bound(mark.open, t + EXECUTION_MS, False)
            if self.pending_exit is not None and self.pending_exit['due_ms'] <= t:
                self._close(bar.open, t + EXECUTION_MS, self.pending_exit['reason'])
            if self.pending_entry is not None and self.pending_entry['due_ms'] <= t:
                self._fill_entry(bar, mark, t + EXECUTION_MS)
            if t % HOUR_MS == 0:
                self._hour(t, bar, mark, analyses.get(t), hours, owned)
            p = self.position
            stop_in_bar = bool(p and stop_touched(p['direction'], bar.low if p['direction'] == 1 else bar.high,
                                                 p['confirmed_stop']))
            if stop_in_bar:
                self.ambiguities['intrabar_stop_first_touch_timestamp_unknown'] += 1
            while fund_index < len(events) and events[fund_index].time_ms < t + FIVE_MS:
                if stop_in_bar:
                    self.ambiguities['funding_and_intrabar_stop_same_5m'] += 1
                self._fund(events[fund_index])
                fund_index += 1
            p = self.position
            if p is not None:
                self.exposure_bars += 1
                worst_mark = mark.low if p['direction'] == 1 else mark.high
                best_mark = mark.high if p['direction'] == 1 else mark.low
                self.intrabar_peak_bound = max(self.intrabar_peak_bound, self.equity(best_mark))
                equity, maintenance = self._margin(worst_mark)
                if self.intrabar_peak_bound > 0:
                    self.max_intrabar_drawdown_bound = max(self.max_intrabar_drawdown_bound,
                        (self.intrabar_peak_bound - equity) / self.intrabar_peak_bound)
                if equity <= maintenance:
                    self._liquidate_bound(worst_mark, bar.close_ms, stop_in_bar)
                elif stop_in_bar:
                    self._close(p['confirmed_stop'], bar.close_ms, 'stop_intrabar')
            equity = self.equity(mark.close)
            self.peak = max(self.peak, equity)
            self.intrabar_peak_bound = max(self.intrabar_peak_bound, equity)
            if self.peak > 0:
                self.max_drawdown = max(self.max_drawdown, (self.peak - equity) / self.peak)
            if self.intrabar_peak_bound > 0:
                self.max_intrabar_drawdown_bound = max(self.max_intrabar_drawdown_bound,
                    (self.intrabar_peak_bound - equity) / self.intrabar_peak_bound, self.max_drawdown)
            self.curve.append({'time_ms': bar.close_ms, 'wallet': self.wallet, 'equity_mark': equity,
                               'quantity': self.position['quantity'] if self.position else ZERO})
        if self.position is not None:
            self._close(bars[-1].close, self.end - 1, 'period_end_forced_close')
            self.curve[-1].update(wallet=self.wallet, equity_mark=self.wallet, quantity=ZERO)
            if self.peak > 0:
                self.max_drawdown = max(self.max_drawdown, (self.peak - self.wallet) / self.peak)
            if self.intrabar_peak_bound > 0:
                self.max_intrabar_drawdown_bound = max(self.max_intrabar_drawdown_bound,
                    (self.intrabar_peak_bound - self.wallet) / self.intrabar_peak_bound, self.max_drawdown)
        self.pending_entry = self.pending_exit = None
        return {'metrics': self.metrics(), 'trades': self.closed, 'events': self.events, 'equity_curve': self.curve}

    def metrics(self) -> dict:
        profits = [trade['net_profit'] for trade in self.closed]
        positive = sum((max(p, ZERO) for p in profits), ZERO)
        negative = -sum((min(p, ZERO) for p in profits), ZERO)
        streak = maximum_streak = 0
        for value in profits:
            streak = streak + 1 if value < 0 else 0
            maximum_streak = max(maximum_streak, streak)
        count = len(profits)
        wins = sum(value > 0 for value in profits)
        losses = sum(value < 0 for value in profits)
        average_win = positive / wins if wins else ZERO
        average_loss = negative / losses if losses else ZERO
        account_net = self.wallet - self.initial
        if not self.liquidations and account_net != self.price_pnl - self.fees_total + self.funding_total:
            if abs(account_net - (self.price_pnl - self.fees_total + self.funding_total)) > D('1e-20'):
                raise AssertionError('accounting_identity_broken')
        if abs(sum(profits, ZERO) - account_net) > D('1e-20'):
            raise AssertionError('trade_sum_does_not_match_wallet')
        return {
            'symbol': self.symbol, 'scenario': self.scenario.name, 'start_ms': self.start, 'end_ms': self.end,
            'fee_rate_per_side': self.scenario.fee, 'adverse_slippage_rate_per_side': self.scenario.slip,
            'entry_and_indicator_fill_delay_minutes': D(self.scenario.fill_delay_ms) / 60_000,
            'order_submission_delay_minutes': D(self.scenario.submit_delay_ms) / 60_000,
            'initial_equity_usdt': self.initial, 'ending_equity_usdt': self.wallet,
            'net_profit_usdt': account_net, 'return_pct': account_net / self.initial * 100,
            'gross_price_profit_usdt': self.price_pnl, 'fees_usdt': self.fees_total,
            'funding_usdt': self.funding_total, 'trades': count,
            'wins': wins, 'losses': losses,
            'win_rate_pct': D(wins) / count * 100 if count else ZERO,
            'average_win_net_usdt': average_win, 'average_loss_net_usdt': average_loss,
            'payoff_ratio': average_win / average_loss if average_loss else (None if average_win else ZERO),
            'largest_trade_net_loss_usdt': max(-min(profits, default=ZERO), ZERO),
            'worst_trade_net_profit_usdt': min(profits, default=ZERO),
            'signals_total': self.total_signals, 'signals_abandoned': sum(self.skips.values()),
            'profit_factor': positive / negative if negative else (None if positive else ZERO),
            'profit_factor_no_losses': bool(positive and not negative),
            'average_trade_profit_usdt': account_net / count if count else ZERO,
            'average_holding_hours': sum((t['holding_hours'] for t in self.closed), ZERO) / count if count else ZERO,
            'maximum_consecutive_losses': maximum_streak, 'max_drawdown_mark_close_pct': self.max_drawdown * 100,
            'max_drawdown_intrabar_adverse_bound_pct': self.max_intrabar_drawdown_bound * 100,
            'exposure_pct': D(self.exposure_bars * FIVE_MS) / (self.end - self.start) * 100,
            'lowest_maintenance_buffer_usdt': self.lowest_margin_buffer,
            'maximum_notional_to_mark_equity': self.max_notional_equity,
            'liquidation_loss_bounds': self.liquidations, 'intrabar_ambiguities': dict(self.ambiguities),
            'skip_reasons': dict(self.skips), 'exit_reasons': dict(Counter(t['exit_reason'] for t in self.closed)),
            'entry_branches': dict(Counter(t['branch'] for t in self.closed)),
            'valid_for_acceptance': not self.liquidations and not self.ambiguities.get(
                'gap_stop_and_maintenance_barrier_same_open') and self.wallet >= 0,
        }


def acceptance(results: Sequence[dict], scenario_name: str) -> dict:
    selected = [r for r in results if r['scenario'] == scenario_name]
    periods = [r for r in selected if r['window_type'] == 'independent_7d']
    continuous = [r for r in selected if r['window_type'] == 'continuous_28d']
    sums = {symbol: sum((D(str(r['net_profit_usdt'])) for r in periods if r['symbol'] == symbol), ZERO)
            for symbol in ('BTCUSDT', 'ETHUSDT')}
    contiguous = {r['symbol']: D(str(r['net_profit_usdt'])) for r in continuous}
    positive = sum(D(str(r['net_profit_usdt'])) > 0 for r in periods)
    complete = len(periods) == 8 and len(continuous) == 2
    valid = all(r['valid_for_acceptance'] for r in selected)
    passed = (complete and valid and positive >= 7 and all(v > 0 for v in sums.values())
              and len(contiguous) == 2 and all(v > 0 for v in contiguous.values()))
    return {'scenario': scenario_name, 'complete': complete, 'valid_execution_model': valid,
            'positive_independent_windows': positive, 'required_positive_windows': 7,
            'independent_net_sums_usdt': sums, 'continuous_net_usdt': contiguous, 'passed': passed}
