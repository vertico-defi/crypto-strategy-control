"""Pure, fail-closed primitives for the frozen long-only mean-reversion study.

This module deliberately has no filesystem, network, exchange, or order-routing
dependencies.  A pipeline must supply already-authorized complete daily bars and
synchronised fills; this module only turns them into causal desired targets and
accounts for those targets.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from strategy_control.trend import (
    DailyBar,
    Fill,
    IntervalResult,
    TrendError,
    aggregate_return,
    cscv_pbo,
    deflated_sharpe_probability,
    maximum_drawdown,
    regime_labels,
    self_financing,
    stationary_bootstrap,
)


class MeanReversionError(TrendError):
    """Raised when the frozen mean-reversion contract cannot be satisfied."""


@dataclass(frozen=True)
class MeanReversionConfig:
    """The immutable signal parameters for one declared multiplicity trial."""

    horizon: int = 3
    volatility_lookback: int = 20
    entry_z: float = -1.5
    exit_z: float = -0.25
    maximum_holding_intervals: int = 5
    raw_drawdown: bool = False
    raw_entry_return: float = -0.05
    raw_exit_daily_return: float = 0.0

    def __post_init__(self) -> None:
        if self.horizon < 1 or self.volatility_lookback < 2:
            raise MeanReversionError("invalid mean-reversion lookback")
        if self.maximum_holding_intervals < 1:
            raise MeanReversionError("holding interval count must be positive")
        numeric = (
            self.entry_z,
            self.exit_z,
            self.raw_entry_return,
            self.raw_exit_daily_return,
        )
        if any(not math.isfinite(value) for value in numeric):
            raise MeanReversionError("signal thresholds must be finite")
        if not self.raw_drawdown and self.entry_z >= self.exit_z:
            raise MeanReversionError("standardized entry must be below exit")
        if self.raw_drawdown and self.raw_entry_return >= self.raw_exit_daily_return:
            raise MeanReversionError("raw entry must be below exit")


@dataclass(frozen=True)
class AssetDecision:
    """A decision after a completed daily session, never a fill or trade."""

    desired_long: bool
    actual_long: bool
    pending: bool
    holding_intervals: int | None
    valid_input: bool


@dataclass(frozen=True)
class TargetEvent:
    """An atomic per-asset desired state scheduled at a supplied fill index."""

    fill_index: int
    target: float


PRIMARY = MeanReversionConfig()
VARIANTS: Mapping[str, MeanReversionConfig] = {
    "primary_standardized_shock": PRIMARY,
    "raw_three_session_drawdown_baseline": MeanReversionConfig(raw_drawdown=True),
    "shorter_two_session_shock": MeanReversionConfig(horizon=2, maximum_holding_intervals=4),
    "longer_five_session_shock": MeanReversionConfig(horizon=5, maximum_holding_intervals=7),
    "shallower_entry": MeanReversionConfig(entry_z=-1.25),
    "deeper_entry": MeanReversionConfig(entry_z=-1.75),
    "slower_volatility_estimator": MeanReversionConfig(volatility_lookback=40),
}
TRIAL_ORDER = tuple(VARIANTS)
PARAMETER_NEIGHBORS = (
    "shorter_two_session_shock",
    "longer_five_session_shock",
    "shallower_entry",
    "deeper_entry",
)


def standardized_shocks(
    days: Sequence[DailyBar], config: MeanReversionConfig = PRIMARY
) -> list[float | None]:
    """Compute only completed-session z scores; gaps and bad inputs fail closed."""

    output: list[float | None] = [None] * len(days)
    closes = [day.close for day in days]
    for index, day in enumerate(days):
        if not day.complete or index < max(config.horizon, config.volatility_lookback):
            continue
        window = days[index - config.volatility_lookback : index + 1]
        if not all(item.complete for item in window) or not all(
            math.isfinite(item.close) and item.close > 0 for item in window
        ):
            continue
        returns = [
            closes[pos] / closes[pos - 1] - 1.0
            for pos in range(index - config.volatility_lookback + 1, index + 1)
        ]
        mean = sum(returns) / len(returns)
        variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
        volatility = math.sqrt(variance)
        shock = closes[index] / closes[index - config.horizon] - 1.0
        if math.isfinite(volatility) and volatility > 0 and math.isfinite(shock):
            output[index] = shock / (volatility * math.sqrt(config.horizon))
    return output


def _input_at(
    days: Sequence[DailyBar], index: int, config: MeanReversionConfig
) -> tuple[bool, float | None, float | None]:
    horizon_days = days[index - config.horizon : index + 1]
    if (
        index < config.horizon
        or not days[index].complete
        or len(horizon_days) != config.horizon + 1
        or not all(item.complete for item in horizon_days)
        or not all(math.isfinite(item.close) and item.close > 0 for item in horizon_days)
    ):
        return False, None, None
    daily_return = days[index].close / days[index - 1].close - 1.0 if index else None
    raw = days[index].close / days[index - config.horizon].close - 1.0
    if not math.isfinite(raw) or daily_return is None or not math.isfinite(daily_return):
        return False, None, None
    if config.raw_drawdown:
        return True, raw, daily_return
    z = standardized_shocks(days, config)[index]
    return z is not None, z, daily_return


def asset_state_machine(
    days: Sequence[DailyBar],
    config: MeanReversionConfig = PRIMARY,
    *,
    execution_delay_sessions: int = 0,
    recovery_sessions: int = 150,
    decision_start: datetime | None = None,
) -> list[AssetDecision]:
    """Run an individual asset's cash/long/pending/holding-clock state machine.

    ``execution_delay_sessions=0`` is base execution.  A positive delay queues
    an unchanged target for that many later *eligible* fills.  The caller must
    map decisions to synchronised fills; no price is inspected here.
    """

    if execution_delay_sessions < 0 or recovery_sessions < 1:
        raise MeanReversionError("invalid execution recovery settings")
    if decision_start is not None:
        if decision_start.tzinfo is None:
            raise MeanReversionError("decision start must be timezone-aware UTC")
        decision_start = decision_start.astimezone(UTC)
    actual = False
    pending: tuple[bool, int] | None = None
    held = 0
    complete_run = 0
    previous_session: datetime | None = None
    result: list[AssetDecision] = []
    for index, day in enumerate(days):
        session = day.session
        if session.tzinfo is None or session.astimezone(UTC).utcoffset() != timedelta(0):
            raise MeanReversionError("sessions must be timezone-aware UTC")
        contiguous = previous_session is None or session == previous_session + timedelta(days=1)
        previous_session = session
        # Apply an already queued target at the fill preceding this completed
        # session.  Only then can a gap in this session assess actual exposure.
        executing_pending = pending is not None
        if pending is not None:
            target, remaining = pending
            if remaining == 0:
                actual = target
                held = 0
                pending = None
            else:
                pending = (target, remaining - 1)
        if not day.complete or not contiguous:
            if actual:
                raise MeanReversionError("quarantine reached risky or pending state")
            # A pending entry has no economic exposure and is cancelled exactly
            # as frozen; pending exits necessarily have actual risky exposure.
            pending = None
            held = 0
            complete_run = 0
            result.append(AssetDecision(False, False, False, None, False))
            continue
        complete_run += 1
        if decision_start is not None and session < decision_start:
            if actual or pending is not None:
                raise MeanReversionError("pre-fold state must remain cash")
            result.append(AssetDecision(False, False, False, None, False))
            continue
        if complete_run < recovery_sessions:
            if actual or pending is not None:
                raise MeanReversionError("recovery cannot retain exposure")
            result.append(AssetDecision(False, False, False, None, False))
            continue
        valid, value, daily_return = _input_at(days, index, config)
        desired = actual
        if executing_pending:
            # The queued target is the only state transition allowed at this
            # synchronised fill; the next completed session may make a decision.
            result.append(
                AssetDecision(actual, actual, pending is not None, held if actual else None, valid)
            )
            continue
        # Exposed intervals are counted after actual entry; actual state changes
        # at a fill, represented by advancing the clock on later sessions.
        if actual:
            held += 1
            mandatory_exit_due = (
                held + execution_delay_sessions + 1 >= config.maximum_holding_intervals
            )
            if mandatory_exit_due or (valid and (
                daily_return is not None and daily_return > config.raw_exit_daily_return
                if config.raw_drawdown
                else value is not None and value >= config.exit_z
            )):
                desired = False
        elif valid and value is not None and value <= (
            config.raw_entry_return if config.raw_drawdown else config.entry_z
        ):
            desired = True
        if pending is None and desired != actual:
            pending = (desired, execution_delay_sessions)
        result.append(
            AssetDecision(desired, actual, pending is not None, held if actual else None, valid)
        )
    return result


def target_events(
    decisions: Sequence[AssetDecision], *, asset_weight: float = 0.5
) -> list[TargetEvent]:
    """Extract state changes.  The supplied index is the causal fill index."""

    if not 0 <= asset_weight <= 1:
        raise MeanReversionError("asset weight must be between zero and one")
    previous = False
    events: list[TargetEvent] = []
    for index, decision in enumerate(decisions):
        if decision.actual_long != previous:
            events.append(TargetEvent(index, asset_weight if decision.actual_long else 0.0))
        previous = decision.actual_long
    return events


def atomic_portfolio_fills(
    timestamps: Sequence[datetime],
    prices: Sequence[Mapping[str, float]],
    targets: Sequence[Mapping[str, float]],
) -> list[Fill]:
    """Validate synchronised atomic target vectors before self-financing marks."""

    if not (len(timestamps) == len(prices) == len(targets)):
        raise MeanReversionError("fill vectors must have identical length")
    if not timestamps:
        return []
    symbols = {"BTCUSDT", "ETHUSDT"}
    if set(prices[0]) != symbols:
        raise MeanReversionError("frozen portfolio requires BTCUSDT and ETHUSDT")
    output: list[Fill] = []
    previous_timestamp: datetime | None = None
    for timestamp, price, target in zip(timestamps, prices, targets, strict=True):
        if set(price) != symbols or set(target) != symbols:
            raise MeanReversionError("non-synchronised portfolio fill")
        if timestamp.tzinfo is None:
            raise MeanReversionError("fill timestamp must be timezone-aware UTC")
        normalized = timestamp.astimezone(UTC)
        if previous_timestamp is not None and normalized <= previous_timestamp:
            raise MeanReversionError("fills must be strictly chronological")
        previous_timestamp = normalized
        if any(
            not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
            for value in price.values()
        ):
            raise MeanReversionError("fill prices must be finite and positive")
        if any(
            not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value not in {0.0, 0.5}
            for value in target.values()
        ):
            raise MeanReversionError("portfolio targets must be exactly zero or one half")
        if sum(target.values()) > 1.0 + 1e-12:
            raise MeanReversionError("gross exposure exceeds frozen maximum")
        output.append(Fill(normalized, price, target))
    return output


def forced_terminal_cash(fills: Sequence[Fill]) -> list[Fill]:
    """Declare the terminal cash target without mutating prior desired targets."""

    if not fills:
        return []
    last = fills[-1]
    return [*fills[:-1], Fill(last.timestamp, last.prices, {asset: 0.0 for asset in last.targets})]


def completed_entries(fills: Sequence[Fill]) -> Mapping[str, int]:
    """Count actual costed cash→long→cash transitions within an evaluation boundary."""

    if not fills:
        return {}
    assets = tuple(fills[0].targets)
    opened = {asset: False for asset in assets}
    counts = {asset: 0 for asset in assets}
    for fill in fills:
        for asset in assets:
            long = fill.targets[asset] > 0
            if long:
                opened[asset] = True
            elif opened[asset]:
                counts[asset] += 1
                opened[asset] = False
    return counts


# Re-export frozen deterministic accounting primitives under this experiment's
# module and wrap statistical degeneracies exactly as newly frozen.
account_portfolio = self_financing
aggregate_net_return = aggregate_return
max_drawdown = maximum_drawdown
regimes = regime_labels


def concentration(intervals: Sequence[IntervalResult]) -> dict[str, object]:
    """Apply frozen positive-PnL concentration denominators and edge cases."""

    equity = 1.0
    pnl: list[float] = []
    for item in intervals:
        current = item.equity
        if not math.isfinite(current):
            raise MeanReversionError("concentration requires finite interval equity")
        pnl.append(current - equity)
        equity = current
    positives = sorted((value for value in pnl if value > 0), reverse=True)
    denominator = sum(positives)
    largest = positives[0] / denominator if denominator > 0 else None
    top_five = sum(positives[:5]) / denominator if denominator > 0 else None
    return {
        "largest_positive_day_fraction_of_positive_total_pnl": largest,
        "top_five_positive_days_fraction_of_positive_total_pnl": top_five,
        "pass": bool(
            denominator > 0
            and largest is not None
            and top_five is not None
            and largest <= 0.5
            and top_five <= 0.75
        ),
    }


def gate_checks(metrics: Mapping[str, object], gates: Mapping[str, object]) -> dict[str, bool]:
    """Evaluate every frozen mean-reversion gate and fail unknown gates closed."""

    counts = {
        "fold_count",
        "parameter_neighbor_count",
        "positive_folds_minimum",
        "positive_parameter_neighbors_minimum",
        "completed_entries_total_minimum",
        "completed_entries_each_asset_minimum",
    }
    categorical = {
        "no_material_leakage",
        "exceptional_trade_gate",
        "regime_gate",
        "baseline_superiority",
    }
    output: dict[str, bool] = {}
    for name, requirement in gates.items():
        value = metrics.get(name)
        if name.endswith("_gt"):
            output[name] = (
                isinstance(value, (int, float))
                and isinstance(requirement, (int, float))
                and value > requirement
            )
        elif name.endswith("_gte"):
            output[name] = (
                isinstance(value, (int, float))
                and isinstance(requirement, (int, float))
                and value >= requirement
            )
        elif name.endswith("_lte"):
            output[name] = (
                isinstance(value, (int, float))
                and isinstance(requirement, (int, float))
                and value <= requirement
            )
        elif name in counts:
            output[name] = (
                isinstance(value, int)
                and isinstance(requirement, int)
                and value >= requirement
            )
        elif name in categorical:
            output[name] = value is True or (requirement == "pass" and value == "pass")
        else:
            output[name] = False
    return output


def bootstrap(
    values: Sequence[float],
    resamples: int = 2000,
    block_length: int = 20,
    rng: object | None = None,
) -> dict[str, object]:
    """Use the experiment-specific frozen PCG64 seed and fail on short samples."""

    if len(values) < 2:
        raise MeanReversionError("bootstrap requires at least two observations")
    return stationary_bootstrap(
        values,
        resamples=resamples,
        block_length=block_length,
        experiment_id="btc-eth-long-only-mean-reversion-v1",
        rng=rng,
    )


def deflated_sharpe(
    primary: Sequence[float], alternatives: Sequence[Sequence[float]]
) -> float:
    """Return frozen probability zero for statistical degeneracy."""

    if len(alternatives) != 7 or any(len(item) != len(primary) for item in alternatives):
        raise MeanReversionError("DSR requires exactly seven aligned trials")
    if len(primary) < 4 or any(
        not math.isfinite(value) for item in alternatives for value in item
    ):
        return 0.0
    try:
        value = deflated_sharpe_probability(primary, alternatives)
    except (ArithmeticError, TrendError, ValueError):
        return 0.0
    return value if math.isfinite(value) else 0.0


def pbo(alternatives: Sequence[Sequence[float]]) -> float:
    """Return frozen PBO one for undersized or nonfinite aligned inputs."""

    if len(alternatives) != 7 or any(
        len(item) != len(alternatives[0]) for item in alternatives
    ):
        raise MeanReversionError("PBO requires exactly seven aligned trials")
    if len(alternatives[0]) < 8 or any(
        not math.isfinite(value) for item in alternatives for value in item
    ):
        return 1.0
    try:
        value = cscv_pbo(alternatives)
    except (ArithmeticError, TrendError, ValueError):
        return 1.0
    return value if math.isfinite(value) else 1.0


def bootstrap_seed() -> int:
    """Expose the frozen experiment seed for deterministic validation."""

    return int.from_bytes(
        hashlib.sha256(b"btc-eth-long-only-mean-reversion-v1").digest()[:8], "big"
    )
