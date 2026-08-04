"""Production-interface guards and independent-friendly trace reconciliation for v2.

No function in this module opens paths or computes market returns.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime
from typing import TypeVar

from strategy_control.relative_value_v2 import DecisionTrace, RelativeValueV2Error, finite_equal

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
    """Reject holdout tokens before any caller can resolve a path."""
    if "2026" in relative_path.replace("\\", "/"):
        raise RelativeValueV2Error("holdout path rejected before resolution")


def strict_prefix(
    items: Sequence[T], timestamps: Sequence[datetime], end: datetime
) -> tuple[T, ...]:
    if len(items) != len(timestamps):
        raise RelativeValueV2Error("boundary index length mismatch")
    return tuple(item for item, stamp in zip(items, timestamps, strict=True) if stamp < end)


def gate_map(
    metrics: Mapping[str, object], requirements: Mapping[str, object]
) -> Mapping[str, bool]:
    if tuple(requirements) != GATE_NAMES:
        raise RelativeValueV2Error("unknown, missing, or reordered gate")
    result: dict[str, bool] = {}
    for name, threshold in requirements.items():
        value = metrics.get(name)
        if name.endswith("_gt"):
            result[name] = (
                isinstance(value, float) and isinstance(threshold, float) and value > threshold
            )
        elif name.endswith(("_gte", "_lte")):
            result[name] = (
                isinstance(value, float)
                and isinstance(threshold, float)
                and (value >= threshold if name.endswith("_gte") else value <= threshold)
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
                isinstance(value, int) and isinstance(threshold, int) and value >= threshold
            )
    return result


def reconcile_traces(production: Sequence[DecisionTrace], oracle: Sequence[DecisionTrace]) -> None:
    """Exact schema comparison; finite numeric fields use the frozen 1e-12 tolerance."""
    if len(production) != len(oracle):
        raise RelativeValueV2Error("trace length mismatch")
    for left, right in zip(production, oracle, strict=True):
        a, b = asdict(left), asdict(right)
        if a.keys() != b.keys():
            raise RelativeValueV2Error("trace schema mismatch")
        for key, value in a.items():
            other = b[key]
            if isinstance(value, float):
                if not isinstance(other, float) or not finite_equal(value, other):
                    raise RelativeValueV2Error(f"trace mismatch: {key}")
            elif value != other:
                raise RelativeValueV2Error(f"trace mismatch: {key}")
