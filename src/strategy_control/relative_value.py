"""Pure, fail-closed primitives for frozen BTC/ETH relative-value rotation.

This module consumes only in-memory, already authorised observations.  It has no
filesystem, network, exchange, or order-routing capability.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from statistics import median
from typing import Any

from strategy_control.trend import (
    DailyBar,
    Fill,
    TrendError,
    cscv_pbo,
    deflated_sharpe_probability,
    stationary_bootstrap,
)


class RelativeValueError(TrendError):
    """Raised when the frozen relative-value contract cannot be satisfied."""


SYMBOLS = ("BTCUSDT", "ETHUSDT")
CASH = "CASH"


@dataclass(frozen=True)
class Trial:
    name: str
    horizons: tuple[int, ...]
    risk_adjusted: bool
    cash_filter: bool
    gap: float
    tie_from_cash_btc: bool = False

    def __post_init__(self) -> None:
        if not self.horizons or any(h < 1 for h in self.horizons):
            raise RelativeValueError("trial horizons must be positive")
        if not math.isfinite(self.gap) or self.gap < 0:
            raise RelativeValueError("trial gap must be finite and non-negative")


PRIMARY = Trial("primary_risk_adjusted_20_60_120", (20, 60, 120), True, True, 0.25)
TRIALS: Mapping[str, Trial] = {
    PRIMARY.name: PRIMARY,
    "raw_60_session_relative_strength_rotation": Trial(
        "raw_60_session_relative_strength_rotation", (60,), False, True, 0.0
    ),
    "short_10_30_60_horizons": Trial("short_10_30_60_horizons", (10, 30, 60), True, True, 0.25),
    "long_60_120_180_horizons": Trial("long_60_120_180_horizons", (60, 120, 180), True, True, 0.25),
    "raw_unadjusted_20_60_120": Trial("raw_unadjusted_20_60_120", (20, 60, 120), False, True, 0.25),
    "wide_0_50_rotation_gap": Trial("wide_0_50_rotation_gap", (20, 60, 120), True, True, 0.5),
    "always_in_higher_score_no_cash_filter": Trial(
        "always_in_higher_score_no_cash_filter", (20, 60, 120), True, False, 0.0, True
    ),
}
TRIAL_ORDER = tuple(TRIALS)
PARAMETER_NEIGHBORS = (
    "short_10_30_60_horizons",
    "long_60_120_180_horizons",
    "raw_unadjusted_20_60_120",
    "wide_0_50_rotation_gap",
)


@dataclass(frozen=True)
class Score:
    raw_returns: tuple[float, ...]
    score: float


@dataclass(frozen=True)
class RotationDecision:
    desired: str
    actual: str
    pending: str | None
    eligible: bool
    segment: int
    quarantined: bool


@dataclass(frozen=True)
class QuarantineAction:
    """Causally safe response to a known incomplete session or canonical fill."""

    target: str
    cancel_pending: bool
    requires_priced_liquidation: bool


@dataclass(frozen=True)
class PortfolioInterval:
    start: datetime
    end: datetime
    net_return: float
    equity: float
    turnover: float
    cost: float
    gross_pnl: Mapping[str, float]
    cost_attribution: Mapping[str, float]
    segment: int
    quarantine_liquidation: bool = False


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise RelativeValueError("timestamp must be timezone-aware UTC")
    return value.astimezone(UTC)


def _finite_positive(value: object, label: str) -> float:
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise RelativeValueError(f"{label} must be finite and positive")
    return float(value)


def _contiguous_complete(days: Sequence[DailyBar], index: int, needed: int) -> bool:
    if index < needed:
        return False
    prior: datetime | None = None
    for day in days[index - needed : index + 1]:
        session = _utc(day.session)
        if not day.complete or _finite_or_none(day.close) is None:
            return False
        if prior is not None and session != prior + timedelta(days=1):
            return False
        prior = session
    return True


def _finite_or_none(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) and math.isfinite(value) else None


def score_at(days: Sequence[DailyBar], index: int, trial: Trial = PRIMARY) -> Score | None:
    """Causal score using current and prior completed contiguous sessions only."""
    needed = max(max(trial.horizons), 20)
    if not _contiguous_complete(days, index, needed):
        return None
    closes = [float(d.close) for d in days]
    simple = [closes[p] / closes[p - 1] - 1.0 for p in range(index - 19, index + 1)]
    if any(not math.isfinite(x) for x in simple):
        return None
    mean = sum(simple) / 20
    variance = sum((x - mean) ** 2 for x in simple) / 19
    volatility = math.sqrt(variance)
    if trial.risk_adjusted and (not math.isfinite(volatility) or volatility <= 0):
        return None
    raw = tuple(math.log(closes[index] / closes[index - h]) for h in trial.horizons)
    if any(not math.isfinite(x) for x in raw):
        return None
    components = (
        tuple(
            value / (volatility * math.sqrt(h))
            for value, h in zip(raw, trial.horizons, strict=True)
        )
        if trial.risk_adjusted
        else raw
    )
    score = sum(components) / len(components)
    return Score(raw, score) if math.isfinite(score) else None


def decide(btc: Score | None, eth: Score | None, actual: str, trial: Trial = PRIMARY) -> str:
    """Frozen atomic BTC/ETH/cash decision from completed information."""
    if actual not in {CASH, *SYMBOLS}:
        raise RelativeValueError("actual target is invalid")
    if btc is None or eth is None:
        return CASH
    values = {"BTCUSDT": btc, "ETHUSDT": eth}
    if actual in SYMBOLS and trial.cash_filter and median(values[actual].raw_returns) <= 0:
        return CASH
    difference = btc.score - eth.score
    if not math.isfinite(difference):
        raise RelativeValueError("nonfinite relative score")
    if difference == 0:
        return "BTCUSDT" if actual == CASH and trial.tie_from_cash_btc else actual
    winner = "BTCUSDT" if difference > 0 else "ETHUSDT"
    loser = "ETHUSDT" if winner == "BTCUSDT" else "BTCUSDT"
    winner_score = values[winner]
    allowed = (not trial.cash_filter) or median(winner_score.raw_returns) > 0
    if not allowed:
        return CASH if actual in (CASH, winner) else actual
    lead = winner_score.score - values[loser].score
    if actual == CASH:
        return winner if lead >= trial.gap else CASH
    if actual == winner:
        # A held asset exits on ineligibility / failed cash filter only.
        return winner
    # rotate only at the fixed threshold and winner cash filter.
    return winner if lead >= trial.gap else actual


def target_weights(target: str) -> Mapping[str, float]:
    if target == CASH:
        return {"BTCUSDT": 0.0, "ETHUSDT": 0.0}
    if target in SYMBOLS:
        return {asset: 1.0 if asset == target else 0.0 for asset in SYMBOLS}
    raise RelativeValueError("invalid atomic target")


def quarantine_action(actual: str, pending: str | None) -> QuarantineAction:
    """Cancel signal state; exposed cash must be priced by a caller-provided fill."""
    if actual not in {CASH, *SYMBOLS} or pending not in {None, CASH, *SYMBOLS}:
        raise RelativeValueError("invalid quarantine state")
    return QuarantineAction(CASH, pending is not None, actual != CASH)


def fold_prefix_indices(
    sessions: Sequence[datetime], start: datetime, end: datetime
) -> tuple[int, ...]:
    """Return causal input indexes strictly before the half-open fold end."""
    lower, upper = _utc(start), _utc(end)
    if upper <= lower:
        raise RelativeValueError("invalid fold boundary")
    normalized = tuple(_utc(value) for value in sessions)
    if any(normalized[i] <= normalized[i - 1] for i in range(1, len(normalized))):
        raise RelativeValueError("sessions must be strictly chronological")
    return tuple(i for i, value in enumerate(normalized) if value < upper)


def rotation_state_machine(
    btc_days: Sequence[DailyBar],
    eth_days: Sequence[DailyBar],
    trial: Trial = PRIMARY,
    *,
    execution_delay_sessions: int = 0,
    recovery_sessions: int = 150,
    decision_start: datetime | None = None,
) -> list[RotationDecision]:
    """Pure base/delayed immutable pending clocks with quarantine precedence.

    Entry fill mapping belongs to the pipeline: a row's pending target executes
    before the next synchronized completed session.  A gap while risky raises so
    the caller must price a forced-liquidation interval rather than invent PnL.
    """
    if len(btc_days) != len(eth_days) or execution_delay_sessions < 0 or recovery_sessions < 1:
        raise RelativeValueError("invalid state machine inputs")
    start = _utc(decision_start) if decision_start is not None else None
    actual, pending, run, segment = CASH, None, 0, 0
    previous: datetime | None = None
    output: list[RotationDecision] = []
    for index, (btc_day, eth_day) in enumerate(zip(btc_days, eth_days, strict=True)):
        session = _utc(btc_day.session)
        synchronized = session == _utc(eth_day.session)
        contiguous = previous is None or session == previous + timedelta(days=1)
        previous = session
        valid_day = synchronized and contiguous and btc_day.complete and eth_day.complete
        if not valid_day:
            if actual != CASH:
                raise RelativeValueError("exposed quarantine requires priced forced liquidation")
            pending, run, segment = None, 0, segment + 1
            output.append(RotationDecision(CASH, CASH, None, False, segment, True))
            continue
        # A queued target can fill only on a valid canonical execution event.  It is
        # deliberately applied before the new decision from this completed session.
        if pending is not None:
            target, remaining = pending
            if remaining == 0:
                actual, pending = target, None
            else:
                pending = (target, remaining - 1)
        run += 1
        if start is not None and session < start:
            if actual != CASH or pending is not None:
                raise RelativeValueError("fold warmup may not carry a target or pending order")
            output.append(RotationDecision(CASH, CASH, None, False, segment, False))
            continue
        btc_score, eth_score = score_at(btc_days, index, trial), score_at(eth_days, index, trial)
        eligible = run >= recovery_sessions and btc_score is not None and eth_score is not None
        desired = decide(btc_score, eth_score, actual, trial) if eligible else CASH
        if pending is None and desired != actual:
            pending = (desired, execution_delay_sessions)
        output.append(
            RotationDecision(
                desired, actual, pending[0] if pending else None, eligible, segment, False
            )
        )
    return output


def atomic_fills(
    timestamps: Sequence[datetime], prices: Sequence[Mapping[str, float]], targets: Sequence[str]
) -> list[Fill]:
    if not (len(timestamps) == len(prices) == len(targets)):
        raise RelativeValueError("fill vectors must have identical length")
    result: list[Fill] = []
    prior: datetime | None = None
    for stamp, price, target in zip(timestamps, prices, targets, strict=True):
        current = _utc(stamp)
        if prior is not None and current <= prior:
            raise RelativeValueError("fills must be strictly chronological")
        prior = current
        if set(price) != set(SYMBOLS):
            raise RelativeValueError("fills require exact BTCUSDT/ETHUSDT price vector")
        for asset in SYMBOLS:
            _finite_positive(price[asset], f"price {asset}")
        result.append(Fill(current, dict(price), target_weights(target)))
    return result


def self_financing_with_attribution(
    fills: Sequence[Fill],
    *,
    one_way_cost_bps: float = 14.0,
    segments: Sequence[int] | None = None,
    quarantine_liquidations: Sequence[bool] | None = None,
) -> list[PortfolioInterval]:
    """Three-weight cash-inclusive accounting, with frozen risky-leg attribution."""
    if one_way_cost_bps < 0 or len(fills) < 2:
        raise RelativeValueError("invalid accounting inputs")
    if segments is None:
        segments = [0] * len(fills)
    if quarantine_liquidations is None:
        quarantine_liquidations = [False] * len(fills)
    if len(segments) != len(fills) or len(quarantine_liquidations) != len(fills):
        raise RelativeValueError("segment vector length mismatch")
    wealth, weights = 1.0, {"BTCUSDT": 0.0, "ETHUSDT": 0.0, CASH: 1.0}
    rate, result = one_way_cost_bps / 10_000.0, []

    def validated_target(fill: Fill) -> dict[str, float]:
        target = dict(fill.targets)
        if (
            set(target) != set(SYMBOLS)
            or any(v not in {0.0, 1.0} for v in target.values())
            or sum(target.values()) > 1
        ):
            raise RelativeValueError("targets must be exact atomic long-only weights")
        for asset in SYMBOLS:
            _finite_positive(fill.prices[asset], f"price {asset}")
        target[CASH] = 1 - sum(target.values())
        return target

    def risky_cost_allocation(
        cost: float, before: Mapping[str, float], after: Mapping[str, float]
    ) -> dict[str, float]:
        changes = {asset: abs(after[asset] - before[asset]) for asset in SYMBOLS}
        denominator = sum(changes.values())
        return {
            asset: cost * changes[asset] / denominator if denominator else 0.0
            for asset in SYMBOLS
        }

    initial_target = validated_target(fills[0])
    initial_turnover = 0.5 * sum(
        abs(initial_target[asset] - weights[asset]) for asset in (*SYMBOLS, CASH)
    )
    initial_cost = wealth * initial_turnover * rate
    initial_allocation = risky_cost_allocation(initial_cost, weights, initial_target)
    interval_baseline = wealth
    wealth -= initial_cost
    if wealth <= 0:
        raise RelativeValueError("cost depleted wealth")
    weights = initial_target
    pending_turnover = initial_turnover
    pending_cost = initial_cost
    pending_cost_allocation = initial_allocation
    prior_time = _utc(fills[0].timestamp)
    for i in range(1, len(fills)):
        fill, previous = fills[i], fills[i - 1]
        time = _utc(fill.timestamp)
        if time <= prior_time:
            raise RelativeValueError("fills must be strictly chronological")
        if segments[i] != segments[i - 1]:
            if any(weights[a] > 1e-12 for a in SYMBOLS):
                raise RelativeValueError("return interval may not bridge quarantine boundary")
            target = validated_target(fill)
            interval_baseline = wealth
            turnover = 0.5 * sum(
                abs(target[asset] - weights[asset]) for asset in (*SYMBOLS, CASH)
            )
            pending_cost = wealth * turnover * rate
            pending_turnover = turnover
            pending_cost_allocation = risky_cost_allocation(pending_cost, weights, target)
            wealth -= pending_cost
            if wealth <= 0:
                raise RelativeValueError("cost depleted wealth")
            weights = target
            prior_time = time
            continue
        returns = {
            a: _finite_positive(fill.prices[a], "price")
            / _finite_positive(previous.prices[a], "price")
            - 1
            for a in SYMBOLS
        }
        gross = 1 + sum(weights[a] * returns[a] for a in SYMBOLS)
        if not math.isfinite(gross) or gross <= 0:
            raise RelativeValueError("invalid gross return")
        drift = {a: weights[a] * (1 + returns[a]) / gross for a in SYMBOLS}
        drift[CASH] = weights[CASH] / gross
        target = validated_target(fill)
        turnover = 0.5 * sum(abs(target[a] - drift[a]) for a in (*SYMBOLS, CASH))
        post_gross, rebalance_cost = wealth * gross, wealth * gross * turnover * rate
        next_wealth = post_gross - rebalance_cost
        if not math.isfinite(next_wealth) or next_wealth <= 0:
            raise RelativeValueError("cost depleted wealth")
        gross_pnl = {a: wealth * drift[a] * returns[a] for a in SYMBOLS}
        rebalance_allocation = risky_cost_allocation(rebalance_cost, drift, target)
        cost_alloc = {
            asset: pending_cost_allocation[asset] + rebalance_allocation[asset]
            for asset in SYMBOLS
        }
        result.append(
            PortfolioInterval(
                prior_time,
                time,
                next_wealth / interval_baseline - 1,
                next_wealth,
                pending_turnover + turnover,
                pending_cost + rebalance_cost,
                gross_pnl,
                cost_alloc,
                segments[i],
                quarantine_liquidations[i],
            )
        )
        wealth, weights, prior_time = next_wealth, target, time
        interval_baseline = wealth
        pending_cost = 0.0
        pending_turnover = 0.0
        pending_cost_allocation = {asset: 0.0 for asset in SYMBOLS}
    return result


def completed_holds(fills: Sequence[Fill]) -> Mapping[str, int]:
    opened, count = {a: False for a in SYMBOLS}, {a: 0 for a in SYMBOLS}
    for fill in fills:
        for asset in SYMBOLS:
            now = fill.targets[asset] == 1.0
            if now:
                opened[asset] = True
            elif opened[asset]:
                count[asset] += 1
                opened[asset] = False
    return count


def regime_history(days: Sequence[DailyBar]) -> list[str | None]:
    """Prior-only, gap-reset BTC regime labels; current volatility is excluded from median."""
    result: list[str | None] = []
    closes: list[float] = []
    estimates: list[float] = []
    previous: datetime | None = None
    for day in days:
        session = _utc(day.session)
        if not day.complete or (previous is not None and session != previous + timedelta(days=1)):
            closes, estimates = [], []
            result.append(None)
            previous = session
            continue
        close = _finite_or_none(day.close)
        if close is None or close <= 0:
            raise RelativeValueError("nonfinite BTC close")
        closes.append(close)
        label: str | None = None
        if len(closes) >= 61:
            returns = [closes[p] / closes[p - 1] - 1 for p in range(len(closes) - 60, len(closes))]
            if len(returns) == 60:
                avg = sum(returns) / 60
                vol = math.sqrt(sum((r - avg) ** 2 for r in returns) / 59)
                if len(estimates) >= 120 and len(closes) >= 121:
                    trend = "up" if math.log(closes[-1] / closes[-121]) > 0 else "down"
                    label = f"{trend}_{'high' if vol > median(estimates) else 'low'}"
                estimates.append(vol)
        result.append(label)
        previous = session
    return result


def bootstrap(
    values: Sequence[float], *, resamples: int = 2000, rng: object | None = None
) -> Mapping[str, Any]:
    if len(values) < 2 or any(not math.isfinite(x) for x in values):
        raise RelativeValueError("bootstrap requires at least two finite observations")
    return stationary_bootstrap(
        values,
        resamples=resamples,
        block_length=20,
        experiment_id="btc-eth-relative-value-rotation-v1",
        rng=rng,
    )


def deflated_sharpe(primary: Sequence[float], alternatives: Sequence[Sequence[float]]) -> float:
    if (
        len(primary) < 3
        or len(alternatives) != 7
        or any(len(a) != len(primary) for a in alternatives)
    ):
        return 0.0
    if any(not math.isfinite(x) for x in primary) or any(
        not math.isfinite(x) for a in alternatives for x in a
    ):
        return 0.0
    try:
        value = deflated_sharpe_probability(primary, alternatives)
    except (ArithmeticError, TrendError, ValueError):
        return 0.0
    return value if math.isfinite(value) else 0.0


def pbo(alternatives: Sequence[Sequence[float]]) -> float:
    if (
        len(alternatives) != 7
        or not alternatives
        or any(len(a) != len(alternatives[0]) for a in alternatives)
    ):
        return 1.0
    if len(alternatives[0]) < 8 or any(not math.isfinite(x) for a in alternatives for x in a):
        return 1.0
    try:
        value = cscv_pbo(alternatives)
    except (ArithmeticError, TrendError, ValueError):
        return 1.0
    return value if math.isfinite(value) else 1.0


def concentration(intervals: Sequence[PortfolioInterval]) -> Mapping[str, object]:
    pnl = [
        item.equity - (intervals[i - 1].equity if i else 1.0) for i, item in enumerate(intervals)
    ]
    positives = sorted((x for x in pnl if x > 0), reverse=True)
    denominator = sum(positives)
    largest = positives[0] / denominator if denominator else None
    top_five = sum(positives[:5]) / denominator if denominator else None
    return {
        "largest_positive_interval_fraction_of_positive_total_pnl": largest,
        "top_five_positive_intervals_fraction_of_positive_total_pnl": top_five,
        "pass": bool(
            denominator > 0
            and largest is not None
            and top_five is not None
            and largest <= 0.5
            and top_five <= 0.75
        ),
    }


def gate_checks(metrics: Mapping[str, object], gates: Mapping[str, object]) -> Mapping[str, bool]:
    """Exact frozen development gate names; unknown/missing evidence fails closed."""
    counts = {
        "fold_count",
        "positive_folds_minimum",
        "parameter_neighbor_count",
        "positive_parameter_neighbors_minimum",
        "completed_entries_total_minimum",
        "completed_holds_each_asset_minimum",
    }
    categorical = {
        "baseline_superiority",
        "exceptional_profit_gate",
        "regime_gate",
        "no_material_leakage",
    }
    checks: dict[str, bool] = {}
    for name, required in gates.items():
        value = metrics.get(name)
        if name.endswith("_gt"):
            checks[name] = (
                isinstance(value, (int, float))
                and isinstance(required, (int, float))
                and math.isfinite(value)
                and value > required
            )
        elif name.endswith("_gte"):
            checks[name] = (
                isinstance(value, (int, float))
                and isinstance(required, (int, float))
                and math.isfinite(value)
                and value >= required
            )
        elif name.endswith("_lte"):
            checks[name] = (
                isinstance(value, (int, float))
                and isinstance(required, (int, float))
                and math.isfinite(value)
                and value <= required
            )
        elif name in counts:
            checks[name] = (
                isinstance(value, int) and isinstance(required, int) and value >= required
            )
        elif name in categorical:
            checks[name] = value is True or (required == "pass" and value == "pass")
        else:
            checks[name] = False
    return checks


def bootstrap_seed() -> int:
    return int.from_bytes(hashlib.sha256(b"btc-eth-relative-value-rotation-v1").digest()[:8], "big")
