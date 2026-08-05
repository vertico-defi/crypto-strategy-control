"""No-I/O production-facing validation and accounting helpers for v2."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TypeVar, cast

from strategy_control.mean_reversion_v2_pipeline import FillIdentity, JointSession
from strategy_control.relative_value_v2 import (
    CASH,
    DEVELOPMENT_FOLDS,
    DOUBLE_ONE_WAY_COST,
    SYMBOLS,
    TRIAL_ORDER,
    CanonicalVector,
    DecisionTrace,
    MinuteRow,
    Observation,
    RelativeValueV2Error,
    SessionExecutionBinding,
    SignalSession,
    finite_equal,
    simulate_bound_period,
    simulate_period,
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
    """Isolate before validation: later corrupt records are outside the fold."""
    if len(items) != len(timestamps):
        raise RelativeValueV2Error("boundary index length mismatch")
    boundary = _utc(end)
    retained = [
        (item, _utc(stamp))
        for item, stamp in zip(items, timestamps, strict=True)
        if _utc(stamp) < boundary
    ]
    prior: datetime | None = None
    for _, instant in retained:
        if prior is not None and instant <= prior:
            raise RelativeValueV2Error("nonmonotonic or duplicate input")
        prior = instant
    return tuple(item for item, _ in retained)


@dataclass(frozen=True)
class GovernanceEvidence:
    performance_evidence: bool
    holdout_accessed: bool
    capital_permitted: int
    gpu_seconds_permitted: int
    mining_changed: bool


def governance_evidence(
    *,
    trace_count: int,
    holdout_accessed: bool = False,
    capital_permitted: int = 0,
    gpu_seconds_permitted: int = 0,
    mining_changed: bool = False,
) -> GovernanceEvidence:
    """A synthetic trace cannot become economic evidence merely by existing."""
    if trace_count < 0 or capital_permitted < 0 or gpu_seconds_permitted < 0:
        raise RelativeValueV2Error("invalid governance evidence")
    return GovernanceEvidence(
        False, holdout_accessed, capital_permitted, gpu_seconds_permitted, mining_changed
    )


def build_period_run(
    trial: str,
    observations: Mapping[str, Sequence[Observation]],
    vectors: Sequence[CanonicalVector],
    *,
    delayed: bool = False,
) -> tuple[DecisionTrace, ...]:
    """Legacy synthetic convenience API; not permitted for production input.

    Production must use ``build_production_bindings`` and ``run_bound_period``;
    this retained helper exists for useful compact synthetic fixtures only.
    """
    if not vectors or set(observations) != set(SYMBOLS):
        raise RelativeValueV2Error("incomplete period inputs")
    return simulate_period(trial, observations, vectors, delayed=delayed)


def run_bound_period(
    trial: str, bindings: Sequence[SessionExecutionBinding], *, delayed: bool = False
) -> tuple[DecisionTrace, ...]:
    """Production-facing entry point; only explicit session/fill bindings are accepted."""
    return simulate_bound_period(trial, bindings, delayed=delayed)


def build_production_bindings(
    sessions: Sequence[JointSession], fills: Sequence[FillIdentity], *, end: datetime
) -> tuple[SessionExecutionBinding, ...]:
    """No-I/O adapter from verified production session/fill evidence.

    ``sessions`` and ``fills`` are deliberately joined by their immutable
    session identities, never by collection position.  The narrow structural
    access here keeps the adapter independent of any loader while allowing the
    verified production-row types to remain in their existing module.
    """
    boundary = _utc(end)
    by_session: dict[datetime, FillIdentity] = {}
    for fill in fills:
        session = _utc(fill.session)
        if session in by_session:
            raise RelativeValueV2Error("duplicate production fill session")
        by_session[session] = fill
    ordered = tuple(sessions)
    grid: list[SignalSession] = []
    for source in ordered:
        session_at = _utc(source.session)
        complete = source.complete is True
        cutoff = source.information_cutoff
        closes = source.closes
        identity = str(getattr(source, "identity", session_at.isoformat()))
        if not complete:
            signal = SignalSession(identity, session_at, None)
        else:
            if cutoff is None or set(closes) != set(SYMBOLS):
                raise RelativeValueV2Error("malformed complete production session")
            observations = (
                Observation(
                    SYMBOLS[0],
                    cutoff,
                    cutoff,
                    float(closes[SYMBOLS[0]]),
                    f"{identity}:{SYMBOLS[0]}",
                ),
                Observation(
                    SYMBOLS[1],
                    cutoff,
                    cutoff,
                    float(closes[SYMBOLS[1]]),
                    f"{identity}:{SYMBOLS[1]}",
                ),
            )
            signal = SignalSession(identity, session_at, observations)
        grid.append(signal)
    # Construct recovery/segment state from the full grid first.  We then bind
    # the already verified exact fill identities by session key.
    from strategy_control.relative_value_v2 import BoundaryIndex, bind_session_grid

    # bind_session_grid needs an index only for its ordinary-fill resolution;
    # production fills below replace it.  A boundary-empty index gives us the
    # authoritative grid-derived segment/recovery state without any scan.
    state = bind_session_grid(grid, BoundaryIndex((), boundary))
    output: list[SessionExecutionBinding] = []
    for item in state:
        selected_fill = by_session.get(item.session_at)
        base: CanonicalVector | None = None
        delayed: CanonicalVector | None = None
        if selected_fill is not None:
            if item.cutoff is None or item.recovery_count != 150:
                raise RelativeValueV2Error("fill supplied for ineligible production session")
            base_stamp = _utc(selected_fill.base_timestamp)
            required = item.cutoff.replace(second=0, microsecond=0) + timedelta(minutes=1)
            if base_stamp != required:
                raise RelativeValueV2Error("production fill is not the exact required vector")
            prices = selected_fill.base_prices
            identities = selected_fill.base_row_identities
            base = CanonicalVector(
                base_stamp,
                (
                    MinuteRow(
                        SYMBOLS[0],
                        base_stamp,
                        float(prices[SYMBOLS[0]]),
                        str(identities[SYMBOLS[0]]),
                    ),
                    MinuteRow(
                        SYMBOLS[1],
                        base_stamp,
                        float(prices[SYMBOLS[1]]),
                        str(identities[SYMBOLS[1]]),
                    ),
                ),
            )
            delayed_stamp = selected_fill.delayed_timestamp
            if delayed_stamp is not None:
                delayed_stamp = _utc(delayed_stamp)
                if delayed_stamp <= base_stamp:
                    raise RelativeValueV2Error("delayed fill does not follow base fill")
                delayed_prices, delayed_ids = (
                    selected_fill.delayed_prices,
                    selected_fill.delayed_row_identities,
                )
                delayed = CanonicalVector(
                    delayed_stamp,
                    (
                        MinuteRow(
                            SYMBOLS[0],
                            delayed_stamp,
                            float(delayed_prices[SYMBOLS[0]]),
                            str(delayed_ids[SYMBOLS[0]]),
                        ),
                        MinuteRow(
                            SYMBOLS[1],
                            delayed_stamp,
                            float(delayed_prices[SYMBOLS[1]]),
                            str(delayed_ids[SYMBOLS[1]]),
                        ),
                    ),
                )
        terminal = (
            base
            if base is not None
            and base.timestamp
            == max((_utc(value.base_timestamp) for value in fills), default=boundary)
            else None
        )
        output.append(
            SessionExecutionBinding(
                item.session_id,
                item.session_at,
                item.observations,
                item.cutoff,
                item.segment,
                item.recovery_count,
                base,
                delayed,
                terminal,
            )
        )
    return tuple(output)


def run_development_folds(
    observations: Mapping[str, Sequence[Observation]], vectors: Sequence[CanonicalVector]
) -> Mapping[tuple[datetime, datetime], Mapping[str, tuple[DecisionTrace, ...]]]:
    """Synthetic-only fold convenience helper; production uses bound sessions."""
    result: dict[tuple[datetime, datetime], Mapping[str, tuple[DecisionTrace, ...]]] = {}
    for start, end in DEVELOPMENT_FOLDS:
        selected = tuple(v for v in vectors if start <= v.timestamp < end)
        # observations remain prefix-only; score_at returns cash during warmup.
        prefix = {
            asset: tuple(o for o in observations[asset] if o.event_at < end) for asset in SYMBOLS
        }
        fold: dict[str, tuple[DecisionTrace, ...]] = {}
        for trial in TRIAL_ORDER:
            fold[trial] = (
                simulate_period(trial, prefix, selected, delayed=False) if selected else ()
            )
            fold[f"{trial}:doubled_cost"] = (
                simulate_period(trial, prefix, selected, cost_rate=DOUBLE_ONE_WAY_COST)
                if selected
                else ()
            )
            fold[f"{trial}:delayed"] = (
                simulate_period(trial, prefix, selected, delayed=True) if selected else ()
            )
        result[(start, end)] = fold
    return result


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
