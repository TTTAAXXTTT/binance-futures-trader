"""Pure arithmetic for HourlyTrendRange, a trend/range hourly strategy.

This module has no exchange, filesystem, or account side effects.  Callers must
keep an append-only, gap-free candle history with its original seed candle.
The first 1,000 complete candles are warm-up; candle 1,001 can signal.  All
prices and money are Decimal values, and prices supplied to risk_at_quantity
already include any anticipated adverse execution movement.
"""

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from typing import Sequence


HOUR_MS = 3_600_000
ENTRY_WINDOW_MS = 300_000
WARMUP_CANDLES = 1_000
ZERO = Decimal("0")
ONE = Decimal("1")
FEE_RATE = Decimal("0.0005")
SLIPPAGE_RATE = Decimal("0.0002")
LEVERAGE = 50
RISK_FRACTION = Decimal("0.50")
NOTIONAL_MULTIPLIER = Decimal("15")
INITIAL_ATR_MULTIPLIER = Decimal("5")
TRAILING_ATR_MULTIPLIER = Decimal("6")
ADX_TREND_THRESHOLD = Decimal("20")
BOLLINGER_MULTIPLIER = Decimal("1.5")
MIN_MIDDLE_DISTANCE = Decimal("0.002")


def decimal(value) -> Decimal:
    """Convert API decimal strings without introducing binary float error."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def floor_step(value: Decimal, step: Decimal) -> Decimal:
    value, step = decimal(value), decimal(step)
    if step <= 0 or not value.is_finite() or not step.is_finite():
        raise ValueError("invalid_step_or_value")
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_step(value: Decimal, step: Decimal) -> Decimal:
    value, step = decimal(value), decimal(step)
    if step <= 0 or not value.is_finite() or not step.is_finite():
        raise ValueError("invalid_step_or_value")
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def _direction(direction: int) -> int:
    if direction not in (-1, 1):
        raise ValueError("direction_must_be_1_or_minus_1")
    return direction


def adverse_execution_price(
    reference_price, direction: int, tick_size, *, entry: bool,
    slippage_rate=SLIPPAGE_RATE,
) -> Decimal:
    """Estimate a fill once, rounding to a tick against the position.

    Buys round up and sells round down after adverse slippage. This helper
    estimates future fills; exchange-confirmed actual average prices must
    retain their original value and must never pass through it again.
    """
    _direction(direction)
    price, tick, slip = map(decimal, (reference_price, tick_size, slippage_rate))
    if any(not item.is_finite() for item in (price, tick, slip)) or min(price, tick) <= ZERO or not ZERO <= slip < ONE:
        raise ValueError("invalid_execution_price_inputs")
    buy = direction == 1 if entry else direction == -1
    predicted = price * (ONE + slip if buy else ONE - slip)
    rounded = ceil_step(predicted, tick) if buy else floor_step(predicted, tick)
    if rounded <= ZERO:
        raise ValueError("non_positive_execution_price")
    return rounded


@dataclass(frozen=True, slots=True)
class Candle:
    open_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    close_ms: int

    def __post_init__(self):
        for name in ("open", "high", "low", "close"):
            object.__setattr__(self, name, decimal(getattr(self, name)))


@dataclass(frozen=True, slots=True)
class Signal:
    hour_open_ms: int
    boundary_ms: int
    direction: int
    atr: Decimal
    close: Decimal
    ema: Decimal
    breakout_high: Decimal
    breakout_low: Decimal
    branch: str = "trend"


@dataclass(frozen=True, slots=True)
class Analysis:
    ready: bool
    reason: str
    candle_count: int
    ema: Decimal | None = None
    atr: Decimal | None = None
    signal: Signal | None = None
    ema50: Decimal | None = None
    ema12: Decimal | None = None
    adx14: Decimal | None = None
    rsi14: Decimal | None = None
    boll_mid: Decimal | None = None
    boll_upper: Decimal | None = None
    boll_lower: Decimal | None = None

    @property
    def sma30(self):
        return self.boll_mid

    @property
    def adx(self):
        return self.adx14

    @property
    def rsi(self):
        return self.rsi14


@dataclass(frozen=True, slots=True)
class IndicatorSnapshot:
    ema50: Decimal | None = None
    ema12: Decimal | None = None
    atr14: Decimal | None = None
    adx14: Decimal | None = None
    rsi14: Decimal | None = None
    boll_mid: Decimal | None = None
    boll_upper: Decimal | None = None
    boll_lower: Decimal | None = None


def _complete_candles(candles: Sequence[Candle], now_ms: int):
    complete = []
    previous_open = None
    for candle in candles:
        if candle.open_ms + HOUR_MS > now_ms:
            continue
        if candle.open_ms % HOUR_MS:
            return [], "non_hourly_open"
        if candle.close_ms not in (
            candle.open_ms + HOUR_MS - 1, candle.open_ms + HOUR_MS
        ):
            return [], "non_hourly_close"
        values = (candle.open, candle.high, candle.low, candle.close)
        if any(not value.is_finite() or value <= 0 for value in values):
            return [], "invalid_price"
        if candle.high < max(candle.open, candle.close) or candle.low > min(
            candle.open, candle.close
        ):
            return [], "invalid_ohlc"
        if previous_open is not None and candle.open_ms != previous_open + HOUR_MS:
            return [], "candle_gap_or_duplicate"
        previous_open = candle.open_ms
        # Binance's close timestamp is inclusive.  A full hour is confirmed
        # only at its following boundary, never at its final millisecond.
        complete.append(candle)
    return complete, ""


def indicator_series(candles: Sequence[Candle]) -> list[IndicatorSnapshot]:
    """Compute anchored indicator seeds once, retaining every causal snapshot.

    EMA seeds use their first N closes. Wilder ATR, directional movement and
    RSI seeds use the first 14 valid changes (bars 1 through 14). ADX seeds
    from the first 14 DX observations (bars 14 through 27). A zero directional
    movement sum gives DX=0; an entirely flat RSI seed gives neutral RSI=50.
    Bollinger deviation is the population deviation of the last 30 closes.
    The caller validates candles and removes unfinished hours before calling.
    """
    result = []
    with localcontext() as context:
        context.prec = 40
        ema50 = ema12 = atr = plus_dm = minus_dm = gain = loss = adx = None
        tr_seed = plus_seed = minus_seed = gain_seed = loss_seed = ZERO
        dx_seed = ZERO
        alpha50, alpha12 = Decimal(2) / 51, Decimal(2) / 13
        for index, candle in enumerate(candles):
            if index == 11:
                ema12 = sum((item.close for item in candles[:12]), ZERO) / 12
            elif index > 11:
                ema12 += alpha12 * (candle.close - ema12)
            if index == 49:
                ema50 = sum((item.close for item in candles[:50]), ZERO) / 50
            elif index > 49:
                ema50 += alpha50 * (candle.close - ema50)
            rsi = None
            if index:
                previous = candles[index - 1]
                tr = max(candle.high - candle.low, abs(candle.high - previous.close),
                         abs(candle.low - previous.close))
                upward, downward = candle.high - previous.high, previous.low - candle.low
                plus = upward if upward > downward and upward > ZERO else ZERO
                minus = downward if downward > upward and downward > ZERO else ZERO
                change = candle.close - previous.close
                current_gain, current_loss = max(change, ZERO), max(-change, ZERO)
                if index <= 14:
                    tr_seed += tr
                    plus_seed += plus
                    minus_seed += minus
                    gain_seed += current_gain
                    loss_seed += current_loss
                    if index == 14:
                        atr, plus_dm, minus_dm, gain, loss = (
                            value / 14 for value in (tr_seed, plus_seed, minus_seed, gain_seed, loss_seed)
                        )
                else:
                    atr, plus_dm, minus_dm, gain, loss = (
                        (old * 13 + new) / 14 for old, new in (
                            (atr, tr), (plus_dm, plus), (minus_dm, minus),
                            (gain, current_gain), (loss, current_loss))
                    )
                if index >= 14:
                    if gain == ZERO and loss == ZERO:
                        rsi = Decimal(50)
                    elif loss == ZERO:
                        rsi = Decimal(100)
                    else:
                        rsi = Decimal(100) - Decimal(100) / (ONE + gain / loss)
                    dm_sum = plus_dm + minus_dm
                    dx = ZERO if dm_sum == ZERO else Decimal(100) * abs(plus_dm - minus_dm) / dm_sum
                    if index <= 27:
                        dx_seed += dx
                        if index == 27:
                            adx = dx_seed / 14
                    else:
                        adx = (adx * 13 + dx) / 14
            middle = upper = lower = None
            if index >= 29:
                window = candles[index - 29:index + 1]
                middle = sum((item.close for item in window), ZERO) / 30
                variance = sum(((item.close - middle) ** 2 for item in window), ZERO) / 30
                width = BOLLINGER_MULTIPLIER * variance.sqrt()
                upper, lower = middle + width, middle - width
            result.append(IndicatorSnapshot(ema50, ema12, atr, adx, rsi, middle, upper, lower))
    return result


def _indicator_series(candles: Sequence[Candle]):
    """Compatibility tuple; EMA now denotes the entry filter, EMA50."""
    snapshots = indicator_series(candles)
    return [item.ema50 for item in snapshots], [item.atr14 for item in snapshots]


def signal_direction(
    close, *, ema50, adx14, rsi14, boll_mid, boll_upper, boll_lower,
    breakout_high, breakout_low,
) -> tuple[int, str]:
    """Select exactly one ADX branch, with strict entry comparisons.

    Distance from the middle band is relative to the signal close. Expressing
    the threshold by multiplication preserves strict equality on Decimals.
    """
    close, ema50, adx, rsi, middle, upper, lower, high, low = map(decimal, (
        close, ema50, adx14, rsi14, boll_mid, boll_upper, boll_lower,
        breakout_high, breakout_low,
    ))
    if any(not item.is_finite() for item in (close, ema50, adx, rsi, middle, upper, lower, high, low)) or close <= ZERO:
        raise ValueError("invalid_signal_inputs")
    if adx >= ADX_TREND_THRESHOLD:
        if close > ema50 and close > high:
            return 1, "trend"
        if close < ema50 and close < low:
            return -1, "trend"
        return 0, "trend"
    if close < lower and rsi < Decimal(45) and middle - close > close * MIN_MIDDLE_DISTANCE:
        return 1, "range"
    if close > upper and rsi > Decimal(55) and close - middle > close * MIN_MIDDLE_DISTANCE:
        return -1, "range"
    return 0, "range"


def _analysis_at(complete, index: int, indicators: IndicatorSnapshot) -> Analysis:
    count = index + 1
    values = dict(ema=indicators.ema50, atr=indicators.atr14,
                  ema50=indicators.ema50, ema12=indicators.ema12,
                  adx14=indicators.adx14, rsi14=indicators.rsi14,
                  boll_mid=indicators.boll_mid, boll_upper=indicators.boll_upper,
                  boll_lower=indicators.boll_lower)
    if any(value is None for value in (
        indicators.ema50, indicators.ema12, indicators.atr14, indicators.adx14,
        indicators.rsi14, indicators.boll_mid, indicators.boll_upper, indicators.boll_lower,
    )) or index < 12:
        return Analysis(False, "insufficient_indicator_history", count, **values)
    latest = complete[index]
    previous_12 = complete[index - 12:index]
    upper, lower = max(item.high for item in previous_12), min(item.low for item in previous_12)
    direction, branch = signal_direction(
        latest.close, ema50=indicators.ema50, adx14=indicators.adx14,
        rsi14=indicators.rsi14, boll_mid=indicators.boll_mid,
        boll_upper=indicators.boll_upper, boll_lower=indicators.boll_lower,
        breakout_high=upper, breakout_low=lower,
    )
    warmed_up = count > WARMUP_CANDLES
    ready = warmed_up and indicators.atr14 > ZERO
    signal = Signal(latest.open_ms, latest.open_ms + HOUR_MS, direction if ready else 0,
                    indicators.atr14, latest.close, indicators.ema50, upper, lower, branch)
    reason = "" if ready else ("zero_atr" if warmed_up else "warmup")
    return Analysis(ready, reason, count, signal=signal, **values)


def analyze_series(candles: Sequence[Candle], now_ms: int) -> list[Analysis]:
    """All complete-hour analyses from one anchored, causal indicator pass.

    A malformed completed history returns one failed analysis, like analyze.
    Each valid item is identical to analyze(history[:i+1], hour_boundary).
    """
    complete, reason = _complete_candles(candles, now_ms)
    if reason:
        return [Analysis(False, reason, 0)]
    return [_analysis_at(complete, index, snapshot)
            for index, snapshot in enumerate(indicator_series(complete))]


def analyze(candles: Sequence[Candle], now_ms: int) -> Analysis:
    """Use complete trade-price hours only; never include future candles."""
    complete, reason = _complete_candles(candles, now_ms)
    if reason:
        return Analysis(False, reason, 0)
    if not complete:
        return Analysis(False, "no_complete_candles", 0)
    return _analysis_at(complete, len(complete) - 1, indicator_series(complete)[-1])


def execution_window_open(signal: Signal, now_ms: int) -> bool:
    return signal.direction in (-1, 1) and (
        signal.boundary_ms <= now_ms < signal.boundary_ms + ENTRY_WINDOW_MS
    )


def entry_deviation_allowed(signal: Signal, reference_price: Decimal) -> bool:
    price = decimal(reference_price)
    if not price.is_finite() or price <= 0 or signal.close <= 0:
        return False
    allowed = max(Decimal("0.005"), signal.atr / signal.close)
    return abs(price - signal.close) / signal.close <= allowed


def initial_stop(
    average_price, atr_snapshot, direction: int, tick_size, *,
    atr_multiplier=INITIAL_ATR_MULTIPLIER,
) -> Decimal:
    _direction(direction)
    price, atr, tick, multiplier = map(decimal, (average_price, atr_snapshot, tick_size, atr_multiplier))
    if any(not item.is_finite() for item in (price, atr, tick, multiplier)) or min(price, atr, tick, multiplier) <= 0:
        raise ValueError("invalid_initial_stop_inputs")
    candidate = price - direction * multiplier * atr
    if candidate <= 0:
        raise ValueError("non_positive_stop")
    return ceil_step(candidate, tick) if direction == 1 else floor_step(candidate, tick)


def tighten_initial_stop(
    confirmed_stop, average_price, atr_snapshot, direction: int, tick_size, *,
    atr_multiplier=INITIAL_ATR_MULTIPLIER,
) -> Decimal:
    old = decimal(confirmed_stop)
    candidate = initial_stop(average_price, atr_snapshot, direction, tick_size, atr_multiplier=atr_multiplier)
    return max(old, candidate) if direction == 1 else min(old, candidate)


def stop_touched(direction: int, price, stop) -> bool:
    _direction(direction)
    price, stop = decimal(price), decimal(stop)
    return price <= stop if direction == 1 else price >= stop


def ema_exit(direction: int, close, ema) -> bool:
    _direction(direction)
    close, ema = decimal(close), decimal(ema)
    return close < ema if direction == 1 else close > ema


def branch_exit(direction: int, close, branch: str, ema12, sma30) -> bool:
    """Exit using the branch captured at entry, regardless of the latest ADX.

    Both exits are strict: trend longs close below EMA12 (shorts above), while
    range longs close above SMA30 (shorts below). Equality does not exit.
    """
    _direction(direction)
    if branch == "trend":
        if ema12 is None:
            return False
        return ema_exit(direction, close, ema12)
    if branch == "range":
        if sma30 is None:
            return False
        close, middle = decimal(close), decimal(sma30)
        return close > middle if direction == 1 else close < middle
    raise ValueError("unknown_entry_branch")


@dataclass(frozen=True, slots=True)
class MarginTier:
    notional_floor: Decimal
    notional_cap: Decimal
    max_leverage: int
    maintenance_rate: Decimal = ZERO
    maintenance_amount: Decimal = ZERO

    def __post_init__(self):
        for name in ("notional_floor", "notional_cap", "maintenance_rate", "maintenance_amount"):
            object.__setattr__(self, name, decimal(getattr(self, name)))
        if self.notional_floor < 0 or self.notional_cap <= self.notional_floor:
            raise ValueError("invalid_margin_tier")
        if self.max_leverage < 1 or self.maintenance_rate < 0 or self.maintenance_amount < 0:
            raise ValueError("invalid_margin_tier")


@dataclass(frozen=True, slots=True)
class MarketRules:
    tick_size: Decimal
    qty_step: Decimal
    min_qty: Decimal
    min_notional: Decimal
    max_qty: Decimal | None = None
    margin_tiers: tuple[MarginTier, ...] = ()
    leverage: int = LEVERAGE

    def __post_init__(self):
        for name in ("tick_size", "qty_step", "min_qty", "min_notional"):
            object.__setattr__(self, name, decimal(getattr(self, name)))
        if self.max_qty is not None:
            object.__setattr__(self, "max_qty", decimal(self.max_qty))
        object.__setattr__(self, "margin_tiers", tuple(self.margin_tiers))
        if self.tick_size <= 0 or self.qty_step <= 0 or self.min_qty <= 0 or self.min_notional < 0:
            raise ValueError("invalid_market_rules")
        if self.max_qty is not None and self.max_qty < self.min_qty:
            raise ValueError("invalid_market_rules")
        if self.leverage != LEVERAGE:
            raise ValueError("strategy_requires_50x")


def margin_tier_for(notional, rules: MarketRules) -> MarginTier | None:
    notional = decimal(notional)
    for tier in sorted(rules.margin_tiers, key=lambda item: item.notional_floor):
        if tier.notional_floor <= notional <= tier.notional_cap:
            return tier
    return None


def order_rule_reason(quantity, price, rules: MarketRules, *, reduce_only=False) -> str:
    quantity, price = decimal(quantity), decimal(price)
    if quantity <= 0 or price <= 0:
        return "non_positive_quantity_or_price"
    if floor_step(quantity, rules.qty_step) != quantity:
        return "quantity_not_on_step"
    if quantity < rules.min_qty:
        return "below_min_quantity"
    if rules.max_qty is not None and quantity > rules.max_qty:
        return "above_max_quantity"
    if not reduce_only and quantity * price < rules.min_notional:
        return "below_min_notional"
    return ""


@dataclass(frozen=True, slots=True)
class EntryPlan:
    accepted: bool
    reason: str
    equity: Decimal
    budget: Decimal
    notional_cap: Decimal
    expected_entry_price: Decimal = ZERO
    expected_stop_price: Decimal = ZERO
    stop_trigger: Decimal = ZERO
    unit_loss: Decimal = ZERO
    quantity: Decimal = ZERO
    notional: Decimal = ZERO
    initial_margin: Decimal = ZERO
    estimated_open_fee: Decimal = ZERO


def plan_entry(
    equity, reference_price, atr, direction: int, rules: MarketRules, *,
    mark_price=None, available_margin=None, fee_rate=FEE_RATE, slippage_rate=SLIPPAGE_RATE
) -> EntryPlan:
    """Freeze E0/B0/N0 and calculate one opening order; no quantity enlargement.

    The executable bid/ask is reference_price.  The predicted entry and stop
    execution prices each include adverse slippage and tick rounding once.
    The actual caller
    must supply the exchange mark_price and available balance for live checks.
    """
    equity, reference, atr, fee, slip = map(
        decimal, (equity, reference_price, atr, fee_rate, slippage_rate)
    )
    budget, cap = equity * RISK_FRACTION, equity * NOTIONAL_MULTIPLIER
    values = (equity, reference, atr, fee, slip)
    if direction not in (-1, 1) or any(not item.is_finite() for item in values):
        return EntryPlan(False, "invalid_entry_inputs", equity, budget, cap)
    if equity <= 0 or reference <= 0 or atr <= 0 or fee < 0 or not ZERO <= slip < ONE:
        return EntryPlan(False, "invalid_entry_inputs", equity, budget, cap)
    try:
        entry = adverse_execution_price(reference, direction, rules.tick_size,
                                        entry=True, slippage_rate=slip)
        trigger = initial_stop(entry, atr, direction, rules.tick_size)
        stop_execution = adverse_execution_price(trigger, direction, rules.tick_size,
                                                 entry=False, slippage_rate=slip)
    except ValueError as error:
        return EntryPlan(False, str(error), equity, budget, cap)
    if stop_touched(direction, entry, trigger):
        return EntryPlan(False, "initial_stop_at_entry", equity, budget, cap)
    unit_loss = max(direction * (entry - stop_execution), ZERO) + fee * (entry + stop_execution)
    if unit_loss <= 0:
        return EntryPlan(False, "non_positive_unit_loss", equity, budget, cap)
    quantity = floor_step(min(budget / unit_loss, cap / entry), rules.qty_step)
    notional = quantity * entry
    mark = entry if mark_price is None else decimal(mark_price)
    margin = quantity * mark / LEVERAGE
    opening_fee = quantity * entry * fee
    reason = order_rule_reason(quantity, entry, rules)
    if not mark.is_finite() or mark <= 0:
        reason = "invalid_mark_price"
    elif not reason and margin + opening_fee > equity * Decimal("0.95"):
        reason = "margin_exceeds_95_percent_equity"
    elif not reason and available_margin is not None and margin + opening_fee > decimal(available_margin):
        reason = "insufficient_available_margin"
    if not reason and rules.margin_tiers:
        tier = margin_tier_for(quantity * mark, rules)
        if tier is None or tier.max_leverage < LEVERAGE:
            reason = "50x_not_supported_in_margin_tier"
    return EntryPlan(
        not bool(reason), reason, equity, budget, cap, entry, stop_execution,
        trigger, unit_loss, quantity if not reason else ZERO,
        notional if not reason else ZERO, margin if not reason else ZERO,
        opening_fee if not reason else ZERO,
    )


@dataclass(frozen=True, slots=True)
class TrailingResult:
    stop: Decimal
    candidate: Decimal | None
    highest: Decimal | None
    lowest: Decimal | None
    eligible_count: int
    last_hour_open_ms: int | None
    updated: bool
    immediate_exit: bool
    reason: str = ""


def trailing_stop(
    current_stop, direction: int, first_fill_ms: int, candles: Sequence[Candle],
    rules: MarketRules, *, now_ms: int, latest_trade_price=None,
    last_processed_hour_open=None,
    atr_multiplier=TRAILING_ATR_MULTIPLIER,
) -> TrailingResult:
    """Candidate for the newest complete eligible hour, without retroactive exits.

    first_fill_ms is the exchange's earliest fill time and never changes for
    later partial fills.  current_stop must be an exchange-confirmed trigger.
    The caller confirms replacement protection before saving this candidate.
    """
    _direction(direction)
    old = decimal(current_stop)
    multiplier = decimal(atr_multiplier)
    if not multiplier.is_finite() or multiplier <= ZERO:
        raise ValueError("invalid_trailing_multiplier")
    complete, reason = _complete_candles(candles, now_ms)
    if reason:
        return TrailingResult(old, None, None, None, 0, None, False, False, reason)
    eligible_start = (first_fill_ms // HOUR_MS + 1) * HOUR_MS
    eligible = [item for item in complete if item.open_ms >= eligible_start]
    if not eligible:
        return TrailingResult(old, None, None, None, 0, None, False, False)
    highest = max(item.high for item in eligible)
    lowest = min(item.low for item in eligible)
    latest = eligible[-1]
    if last_processed_hour_open is not None and latest.open_ms <= last_processed_hour_open:
        return TrailingResult(old, None, highest, lowest, len(eligible), latest.open_ms, False, False)
    _, atr_series = _indicator_series(complete)
    atr = atr_series[-1]
    if atr is None or atr <= 0:
        return TrailingResult(old, None, highest, lowest, len(eligible), latest.open_ms, False, False, "atr_unavailable")
    raw = highest - multiplier * atr if direction == 1 else lowest + multiplier * atr
    candidate = ceil_step(raw, rules.tick_size) if direction == 1 else floor_step(raw, rules.tick_size)
    stop = max(old, candidate) if direction == 1 else min(old, candidate)
    tightened = stop > old if direction == 1 else stop < old
    touched = tightened and latest_trade_price is not None and stop_touched(direction, latest_trade_price, stop)
    return TrailingResult(stop, candidate, highest, lowest, len(eligible), latest.open_ms, True, touched)


def accumulate_actual_cost(actual_cost, fee, realized_price_pnl) -> Decimal:
    """Risk ledger only: positive fees and individual losses consume budget.

    This does not debit the account.  A profit (or fee rebate) cannot replenish
    B0.  Prices already incorporate actual execution slippage.
    """
    cost, fee, pnl = map(decimal, (actual_cost, fee, realized_price_pnl))
    if cost < 0 or any(not item.is_finite() for item in (cost, fee, pnl)):
        raise ValueError("invalid_actual_cost")
    return cost + max(fee, ZERO) + max(-pnl, ZERO)


def risk_at_quantity(
    quantity, *, current_quantity, average_price, expected_reduce_price,
    expected_stop_price, direction: int, fee_rate=FEE_RATE, actual_cost=ZERO,
) -> Decimal:
    _direction(direction)
    q, q0, average, reduction, stop, fee, cost = map(decimal, (
        quantity, current_quantity, average_price, expected_reduce_price,
        expected_stop_price, fee_rate, actual_cost,
    ))
    if not ZERO <= q <= q0 or average <= 0 or reduction <= 0 or stop <= 0 or fee < 0 or cost < 0:
        raise ValueError("invalid_risk_inputs")
    reducing_unit = max(direction * (average - reduction), ZERO) + fee * reduction
    retained_unit = max(direction * (average - stop), ZERO) + fee * stop
    return cost + (q0 - q) * reducing_unit + q * retained_unit


@dataclass(frozen=True, slots=True)
class ReductionPlan:
    accepted: bool
    quantity: Decimal
    quantity_to_reduce: Decimal
    expected_risk: Decimal
    reason: str
    full_close: bool


def reduction_target(
    *, current_quantity, average_price, expected_reduce_price, expected_stop_price,
    direction: int, budget, notional_cap, rules: MarketRules,
    fee_rate=FEE_RATE, actual_cost=ZERO, reduce_only=True,
) -> ReductionPlan:
    """Find the largest compliant retained quantity, including reduction cost.

    R(q) is linear but is not necessarily increasing.  Solve its interval
    analytically, then enforce the quantity lattice and executable reduction
    order.  No binary search assuming monotonic increasing risk is used.
    minNotional is waived only for reduce-only orders, not opening orders.
    """
    _direction(direction)
    q0, average, reduction, stop, budget, cap, fee, cost = map(decimal, (
        current_quantity, average_price, expected_reduce_price, expected_stop_price,
        budget, notional_cap, fee_rate, actual_cost,
    ))
    inputs = (q0, average, reduction, stop, budget, cap, fee, cost)
    if any(not item.is_finite() for item in inputs) or q0 <= 0 or min(average, reduction, stop) <= 0:
        raise ValueError("invalid_reduction_inputs")
    if budget < 0 or cap < 0 or fee < 0 or cost < 0:
        raise ValueError("invalid_reduction_inputs")
    risk_kwargs = dict(
        current_quantity=q0, average_price=average, expected_reduce_price=reduction,
        expected_stop_price=stop, direction=direction, fee_rate=fee, actual_cost=cost,
    )

    def close(reason):
        return ReductionPlan(False, ZERO, q0, risk_at_quantity(ZERO, **risk_kwargs), reason, True)

    if cost >= budget:
        return close("actual_budget_consumed")
    if floor_step(q0, rules.qty_step) != q0:
        return close("current_quantity_not_on_step")
    reducing_unit = max(direction * (average - reduction), ZERO) + fee * reduction
    retained_unit = max(direction * (average - stop), ZERO) + fee * stop
    intercept = cost + q0 * reducing_unit
    slope = retained_unit - reducing_unit
    lower = rules.min_qty
    upper = min(q0, cap / average)
    if not reduce_only and rules.min_notional:
        lower = max(lower, rules.min_notional / average)
    if slope > 0:
        upper = min(upper, (budget - intercept) / slope)
    elif slope < 0:
        lower = max(lower, (budget - intercept) / slope)
    elif intercept > budget:
        return close("no_feasible_risk_quantity")
    retained = floor_step(max(upper, ZERO), rules.qty_step)
    lower = ceil_step(max(lower, ZERO), rules.qty_step)
    if retained < lower or retained <= 0:
        return close("no_nonzero_compliant_quantity")
    delta = q0 - retained
    if delta > 0:
        min_reduction = ceil_step(rules.min_qty, rules.qty_step)
        if not reduce_only and rules.min_notional:
            min_reduction = max(min_reduction, ceil_step(rules.min_notional / reduction, rules.qty_step))
        if delta < min_reduction:
            retained = floor_step(q0 - min_reduction, rules.qty_step)
            delta = q0 - retained
        if retained < lower or retained <= 0:
            return close("reduction_order_below_minimum")
        if rules.max_qty is not None and delta > rules.max_qty:
            return close("reduction_requires_multiple_orders")
        reason = order_rule_reason(delta, reduction, rules, reduce_only=reduce_only)
        if reason:
            return close(reason)
    expected_risk = risk_at_quantity(retained, **risk_kwargs)
    if retained * average > cap or expected_risk > budget:
        # Decimal division followed by rounding is checked against the actual
        # inequality, so context precision cannot authorize an excess order.
        return close("rounded_quantity_exceeds_budget")
    return ReductionPlan(True, retained, delta, expected_risk, "", False)
