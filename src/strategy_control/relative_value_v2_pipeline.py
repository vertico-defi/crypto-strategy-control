"""No-I/O production-facing validation and accounting helpers for v2."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TypeVar, cast

from strategy_control.relative_value_v2 import (
    CASH,
    SYMBOLS,
    DecisionTrace,
    RelativeValueV2Error,
    finite_equal,
    target_weights,
)

GATE_NAMES = (
    "aggregate_net_return_gt",
    "annualized_sharpe_gte",
    "maximum_drawdown_lte",
    "fold_count",
    "positive_folds_minimum",
    "doubled_cost_aggregate_net_return_gt",
    "additional_delay_aggregate_net_return_gt",
    "positive_parameter_neighbors_minimum",
    "parameter_neighbor_count",
    "bootstrap_mean_daily_net_return_lower_95_ci_gt",
    "deflated_sharpe_probability_gte",
    "probability_of_backtest_overfitting_lte",
    "baseline_superiority",
    "completed_entries_total_minimum",
    "completed_holds_each_asset_minimum",
    "asset_net_contribution_each_gt",
    "exceptional_profit_gate",
    "regime_gate",
    "no_material_leakage",
)
T = TypeVar("T")


def reject_preapproval_path(relative_path: str) -> None:
    """Pure lexical guard deliberately called before any path operation."""
    normalized = relative_path.replace("\\", "/")
    if "year=2026" in normalized or "/2026/" in normalized or normalized.startswith("2026/"):
        raise RelativeValueV2Error("holdout path rejected before resolution")


def strict_prefix(
    items: Sequence[T], timestamps: Sequence[datetime], end: datetime
) -> tuple[T, ...]:
    """Validate original chronological stream, then isolate its strict prefix."""
    if len(items) != len(timestamps):
        raise RelativeValueV2Error("boundary index length mismatch")
    boundary = _utc(end)
    prior: datetime | None = None
    normalized = []
    for stamp in timestamps:
        instant = _utc(stamp)
        if prior is not None and instant <= prior:
            raise RelativeValueV2Error("nonmonotonic or duplicate input")
        prior = instant
        normalized.append(instant)
    return tuple(item for item, stamp in zip(items, normalized, strict=True) if stamp < boundary)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RelativeValueV2Error("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def gate_map(
    metrics: Mapping[str, object], requirements: Mapping[str, object]
) -> Mapping[str, bool]:
    """Exact nineteen-gate map: all numerical inputs must be finite before compare."""
    if tuple(requirements) != GATE_NAMES or set(metrics) != set(GATE_NAMES):
        raise RelativeValueV2Error("unknown, missing, or reordered gate")
    result: dict[str, bool] = {}
    for name, threshold in requirements.items():
        value = metrics[name]
        if name.endswith(("_gt", "_gte", "_lte")):
            if not (_finite_number(value) and _finite_number(threshold)):
                result[name] = False
                continue
            numeric_value = float(cast(int | float, value))
            numeric_threshold = float(cast(int | float, threshold))
            result[name] = (
                numeric_value > numeric_threshold
                if name.endswith("_gt")
                else (
                    numeric_value >= numeric_threshold
                    if name.endswith("_gte")
                    else numeric_value <= numeric_threshold
                )
            )
        elif name in {
            "baseline_superiority",
            "exceptional_profit_gate",
            "regime_gate",
            "no_material_leakage",
        }:
            result[name] = value is True or value == "pass"
        else:
            result[name] = (
                isinstance(value, int)
                and not isinstance(value, bool)
                and isinstance(threshold, int)
                and not isinstance(threshold, bool)
                and value >= threshold
            )
    return result


def rebalance(
    wealth: float, actual: str, target: str, interval_returns: Mapping[str, float], cost_rate: float
) -> tuple[float, float, float, Mapping[str, float]]:
    """Frozen self-financing target/fill/cost accounting over one synthetic interval."""
    if actual not in {CASH, *SYMBOLS} or target not in {CASH, *SYMBOLS}:
        raise RelativeValueV2Error("invalid target")
    if not _finite_number(wealth) or wealth <= 0 or not _finite_number(cost_rate) or cost_rate < 0:
        raise RelativeValueV2Error("invalid accounting input")
    if set(interval_returns) != set(SYMBOLS) or not all(
        _finite_number(x) for x in interval_returns.values()
    ):
        raise RelativeValueV2Error("invalid interval return")
    weights = target_weights(actual)
    gross = 1 + sum(weights[a] * float(interval_returns[a]) for a in SYMBOLS)
    if not math.isfinite(gross) or gross <= 0:
        raise RelativeValueV2Error("invalid gross wealth")
    drifted = {a: weights[a] * (1 + float(interval_returns[a])) / gross for a in SYMBOLS}
    drifted_cash = 1 - sum(drifted.values())
    target_w = target_weights(target)
    turnover = 0.5 * (
        sum(abs(target_w[a] - drifted[a]) for a in SYMBOLS)
        + abs((1 - sum(target_w.values())) - drifted_cash)
    )
    cost = wealth * gross * turnover * float(cost_rate)
    next_wealth = wealth * gross - cost
    if not math.isfinite(next_wealth) or next_wealth <= 0:
        raise RelativeValueV2Error("invalid net wealth")
    return (
        next_wealth,
        turnover,
        cost,
        {a: wealth * drifted[a] * float(interval_returns[a]) for a in SYMBOLS},
    )


def reconcile_traces(production: Sequence[DecisionTrace], oracle: Sequence[DecisionTrace]) -> None:
    if len(production) != len(oracle):
        raise RelativeValueV2Error("trace length mismatch")
    for left, right in zip(production, oracle, strict=True):
        if left.__dataclass_fields__.keys() != right.__dataclass_fields__.keys():
            raise RelativeValueV2Error("trace schema mismatch")
        for field in left.__dataclass_fields__:
            value, other = getattr(left, field), getattr(right, field)
            if isinstance(value, float):
                if not isinstance(other, float) or not finite_equal(value, other):
                    raise RelativeValueV2Error(f"trace mismatch: {field}")
            elif value != other:
                raise RelativeValueV2Error(f"trace mismatch: {field}")
