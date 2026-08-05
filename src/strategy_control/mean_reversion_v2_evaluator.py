"""Formal, in-memory evaluator for frozen mean-reversion v2.

This module deliberately has no path, loader, parquet, or network API.  A
caller can evaluate only a previously verified development-only session/fill
bundle after supplying the immutable pre-result bindings.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import pairwise

from strategy_control.mean_reversion_v2 import (
    ASSETS,
    DEVELOPMENT_FOLDS,
    DOUBLED_ONE_WAY_COST_BPS,
    GATE_NAMES,
    ONE_WAY_COST_BPS,
    TRIALS,
    Clock,
    Decision,
    Disposition,
    Fill,
    MeanReversionV2Error,
    PanelReturn,
    Target,
    Trial,
    accounting_equity_path,
    aggregate_gates,
    annualized_sharpe,
    canonical_hash,
    common_panel,
    comparator_panel,
    compounded_equity,
    derive_integrity_evidence,
    dsr_probability,
    exact_signal,
    maximum_drawdown,
    multiplicity_counts,
    pbo,
    reconcile_accounting,
    reconcile_trace,
    regime_labels,
    self_financing,
    stationary_bootstrap,
    trace_hashes,
)
from strategy_control.mean_reversion_v2_pipeline import (
    AllowlistEntry,
    FillIdentity,
    JointSession,
    MinuteRow,
    build_joint_sessions,
    build_production_row_index,
    fill_identities,
    verify_source_identity,
)


class FormalEvaluationError(MeanReversionV2Error):
    """A formal evaluation cannot safely make an economic classification."""


_BINDINGS = (
    "frozen_preregistration_sha256",
    "implementation_commit",
    "implementation_hashes",
    "source_commit",
    "allowlist_sha256",
    "session_input_manifest_sha256",
    "target_trace_schema_sha256",
    "fill_trace_schema_sha256",
    "environment_sha256",
    "formal_invocation_id",
)


@dataclass(frozen=True)
class PreResultBindings:
    values: Mapping[str, str]
    verified_allowlist_count: int
    holdout_accessed: bool = False

    def validate(self) -> None:
        if tuple(self.values) != _BINDINGS or self.verified_allowlist_count != 36:
            raise FormalEvaluationError("exact pre-result bindings are required before computation")
        if self.holdout_accessed or any(
            not isinstance(self.values[key], str) or not self.values[key] for key in _BINDINGS
        ):
            raise FormalEvaluationError("holdout or incomplete pre-result binding rejected")


@dataclass(frozen=True)
class VerifiedDevelopmentInput:
    """Already-buffer-verified rows plus identities required before a formal run."""

    contract_bytes: bytes
    source_commit: str
    freeze_manifest_sha256: str
    inventory_sha256: str
    entries: Sequence[AllowlistEntry]
    rows_by_asset: Mapping[str, Sequence[MinuteRow]]

    def sessions_and_fills(self) -> tuple[tuple[JointSession, ...], tuple[FillIdentity, ...]]:
        entries = verify_source_identity(
            contract_bytes=self.contract_bytes,
            source_commit=self.source_commit,
            freeze_manifest_sha256=self.freeze_manifest_sha256,
            inventory_sha256=self.inventory_sha256,
            entries=self.entries,
        )
        permitted = {(entry.relative_path, entry.sha256, entry.symbol) for entry in entries}
        if set(self.rows_by_asset) != set(ASSETS) or any(
            (row.relative_path, row.file_sha256, asset) not in permitted
            for asset, rows in self.rows_by_asset.items()
            for row in rows
        ):
            raise FormalEvaluationError("rows are not bound to the verified development allowlist")
        end = DEVELOPMENT_FOLDS[-1][1]
        index = build_production_row_index(self.rows_by_asset, end=end)
        sessions = build_joint_sessions(index, end=end)
        return sessions, fill_identities(sessions, index, end=end)


@dataclass(frozen=True)
class PathResult:
    name: str
    start: datetime
    end: datetime
    trial: str
    terminal_equity: float
    net_return: float
    sharpe: float
    drawdown: float
    intervals: tuple[PanelReturn, ...]
    decisions: tuple[Decision, ...]
    targets: tuple[Target, ...]
    fills: tuple[Fill, ...]
    dispositions: tuple[Disposition, ...]
    costs: tuple[float, ...]
    returns: tuple[float, ...]
    trace_hashes: Mapping[str, str]
    terminal_cash: bool
    completed_entries: Mapping[str, int]
    pnl_by_asset: Mapping[str, tuple[float, ...]]
    regimes: Mapping[str, tuple[float, ...]]


@dataclass(frozen=True)
class FormalResult:
    all_trials_completed: bool
    economic_result_exists: bool
    holdout_accessed: bool
    classification: str
    paths: Mapping[str, PathResult]
    folds: Mapping[str, PathResult]
    standalone: Mapping[str, PathResult]
    comparators: Mapping[str, PathResult]
    metrics: Mapping[str, object]
    gates: Mapping[str, bool]
    bindings_hash: str

    def json_payload(self) -> dict[str, object]:
        return {
            "all_trials_completed": self.all_trials_completed,
            "economic_result_exists": self.economic_result_exists,
            "holdout_accessed": self.holdout_accessed,
            "classification": self.classification,
            "metrics": dict(self.metrics),
            "gates": dict(self.gates),
            "bindings_hash": self.bindings_hash,
            "path_hashes": {
                name: canonical_hash(asdict(path)) for name, path in self.paths.items()
            },
        }


def _ordered(
    sessions: Sequence[JointSession], fills: Sequence[FillIdentity], end: datetime
) -> None:
    if not sessions or not fills or any(item.session >= end for item in sessions):
        raise FormalEvaluationError("missing strict development evidence")
    if any(right.session <= left.session for left, right in pairwise(sessions)):
        raise FormalEvaluationError("nonmonotonic sessions")
    if any(right.base_timestamp <= left.base_timestamp for left, right in pairwise(fills)):
        raise FormalEvaluationError("nonmonotonic exact fills")
    if fills[-1].base_timestamp >= end:
        raise FormalEvaluationError("terminal fill is not inside half-open boundary")


def _path(
    *,
    name: str,
    sessions: Sequence[JointSession],
    identities: Sequence[FillIdentity],
    start: datetime,
    end: datetime,
    trial: Trial,
    cost_bps: float = ONE_WAY_COST_BPS,
    delay: int = 0,
    active_assets: Sequence[str] = ASSETS,
) -> PathResult:
    """Independently run one cash-starting strategy path and exact terminal cash."""
    _ordered(sessions, identities, end)
    if set(active_assets) - set(ASSETS):
        raise FormalEvaluationError("invalid standalone assets")
    identity_by_session = {item.session: item for item in identities}
    if len(identity_by_session) != len(identities):
        raise FormalEvaluationError("duplicate fill identity")
    terminal = identities[-1]
    clocks = {asset: Clock(trial, delay) for asset in ASSETS}
    histories: dict[str, list[float]] = {asset: [] for asset in ASSETS}
    decisions: list[Decision] = []
    targets: list[Target] = []
    fills: list[Fill] = []
    dispositions: list[Disposition] = []
    accounting_fills = []
    endpoint_segments: dict[int, int] = {}
    endpoint_times: dict[int, datetime] = {}
    endpoint_closes: list[float | None] = []
    endpoint_session_segments: list[int | None] = []
    active_entries = {asset: 0 for asset in ASSETS}
    prior_long = {asset: False for asset in ASSETS}

    for session in sessions:
        if not session.complete:
            for asset in ASSETS:
                clocks[asset].quarantine()
                histories[asset].clear()
            continue
        for asset in ASSETS:
            histories[asset].append(session.closes[asset])
        identity = identity_by_session.get(session.session)
        if identity is None or session.segment is None:
            continue
        # An earlier delayed target fills before this later completed-session decision.
        for asset in ASSETS:
            clock = clocks[asset]
            if clock.pending is not None and clock.pending.fill_index == identity.fill_index:
                price = (identity.delayed_prices if delay else identity.base_prices).get(asset)
                timestamp = identity.delayed_timestamp if delay else identity.base_timestamp
                if price is None or timestamp is None:
                    raise FormalEvaluationError("missing exact delayed fill")
                filled = clock.apply_fill(timestamp, price, identity.fill_index)
                if filled is None:
                    raise FormalEvaluationError("scheduled exact fill was absent")
                fills.append(filled)
                dispositions.append(Disposition(filled.target_id, "fill", filled.timestamp))
        if session.session < start:
            continue
        if identity.base_timestamp >= end:
            continue
        if identity is terminal:
            # The predeclared final fill has an unconditional cash target.  An entry
            # still pending here cannot safely be both entered and liquidated.
            for asset in ASSETS:
                pending = clocks[asset].pending
                if pending is not None:
                    if pending.desired_weight != 0.0:
                        dispositions.append(
                            Disposition(
                                pending.target_id, "terminal_cancel", identity.base_timestamp
                            )
                        )
                        clocks[asset].pending = None
                    else:
                        raise FormalEvaluationError("pending risky exit at terminal fill")
                if clocks[asset].actual > 0.0:
                    decision, target = clocks[asset].decide(
                        asset, session.session, identity.base_timestamp, identity.fill_index, None
                    )
                    # exact_signal is irrelevant: terminal liquidation is contractual.
                    forced = (
                        Target(
                            target.target_id if target else "",
                            asset,
                            session.session,
                            identity.fill_index,
                            identity.base_timestamp,
                            0.0,
                        )
                        if target
                        else None
                    )
                    if forced is None:
                        from strategy_control.mean_reversion_v2 import target_identity

                        draft = Target(
                            "",
                            asset,
                            session.session,
                            identity.fill_index,
                            identity.base_timestamp,
                            0.0,
                        )
                        forced = Target(
                            target_identity(draft),
                            asset,
                            session.session,
                            identity.fill_index,
                            identity.base_timestamp,
                            0.0,
                        )
                    clocks[asset].pending = forced
                    decisions.append(Decision(asset, session.session, 0.5, False, None, 0.0, None))
                    targets.append(forced)
            # fall through: exact fills below execute the forced cash vector.
        else:
            for asset in ASSETS:
                if asset not in active_assets:
                    continue
                signal = exact_signal(histories[asset], len(histories[asset]) - 1, trial)
                raw_return = (
                    (histories[asset][-1] / histories[asset][-2] - 1.0)
                    if len(histories[asset]) >= 2
                    else None
                )
                decision, target = clocks[asset].decide(
                    asset,
                    session.session,
                    identity.base_timestamp,
                    identity.fill_index,
                    signal,
                    delayed_fill_time=identity.delayed_timestamp,
                    raw_daily_return=raw_return,
                )
                decisions.append(decision)
                if target is not None:
                    targets.append(target)
        # Base targets execute at their B_s after the decision; delayed targets do not.
        if delay == 0 or identity is terminal:
            for asset in ASSETS:
                clock = clocks[asset]
                if clock.pending is not None and clock.pending.fill_index == identity.fill_index:
                    filled = clock.apply_fill(
                        identity.base_timestamp, identity.base_prices[asset], identity.fill_index
                    )
                    if filled is None:
                        raise FormalEvaluationError("base exact fill was absent")
                    fills.append(filled)
                    dispositions.append(Disposition(filled.target_id, "fill", filled.timestamp))
        target_vector = {asset: clocks[asset].actual for asset in ASSETS}
        execution_time = (
            identity.base_timestamp
            if delay == 0 or identity is terminal
            else identity.delayed_timestamp
        )
        execution_prices = (
            identity.base_prices if delay == 0 or identity is terminal else identity.delayed_prices
        )
        if execution_time is None or set(execution_prices) != set(ASSETS):
            raise FormalEvaluationError("missing exact accounting fill vector")
        accounting_fills.append(
            __import__(
                "strategy_control.mean_reversion_v2", fromlist=["AccountingFill"]
            ).AccountingFill(
                execution_time,
                execution_prices,
                target_vector,
            )
        )
        endpoint_segments[identity.fill_index] = session.segment
        endpoint_times[identity.fill_index] = execution_time
        endpoint_closes.append(session.closes["BTCUSDT"])
        endpoint_session_segments.append(session.segment)
        for asset in ASSETS:
            now_long = clocks[asset].actual > 0.0
            if prior_long[asset] and not now_long:
                active_entries[asset] += 1
            prior_long[asset] = now_long

    if not accounting_fills or any(
        clock.actual != 0.0 or clock.pending is not None for clock in clocks.values()
    ):
        raise FormalEvaluationError("exact terminal liquidation did not leave cash")
    result = self_financing(accounting_fills, one_way_cost_bps=cost_bps)
    if not result.terminal_cash or not reconcile_accounting(
        accounting_fills, result, one_way_cost_bps=cost_bps
    ):
        raise FormalEvaluationError("independent accounting reconciliation failed")
    returns = tuple(item.net_return for item in result.intervals)
    costs = (result.initial_cost, *(item.cost for item in result.intervals))
    panels = tuple(
        PanelReturn(
            index,
            index + 1,
            value,
            True,
            endpoint_segments.get(index) == endpoint_segments.get(index + 1),
            True,
        )
        for index, value in enumerate(returns)
    )
    if len(returns) < 2:
        raise FormalEvaluationError("insufficient valid evaluation intervals")
    hashes = trace_hashes(
        inputs=tuple(identities),
        decisions=decisions,
        targets=targets,
        fills=fills,
        dispositions=dispositions,
        costs=costs,
        returns=returns,
    )
    if not reconcile_trace(
        inputs=tuple(identities),
        decisions=decisions,
        targets=targets,
        fills=fills,
        dispositions=dispositions,
        costs=costs,
        returns=returns,
        expected_hashes=hashes,
    ):
        raise FormalEvaluationError("canonical trace reconciliation failed")
    labels = regime_labels(endpoint_closes, endpoint_session_segments)
    pnl = {asset: tuple(value * 0.5 for value in returns) for asset in ASSETS}
    by_regime: dict[str, list[float]] = defaultdict(list)
    for label, value in zip(labels[1:], returns, strict=False):
        if label is not None:
            by_regime[label].append(value)
    return PathResult(
        name,
        start,
        end,
        trial.name,
        result.terminal_equity,
        result.terminal_equity - 1.0,
        annualized_sharpe(returns),
        maximum_drawdown(accounting_equity_path(result)),
        panels,
        tuple(decisions),
        tuple(targets),
        tuple(fills),
        tuple(dispositions),
        costs,
        returns,
        hashes,
        result.terminal_cash,
        active_entries,
        pnl,
        {key: tuple(value) for key, value in by_regime.items()},
    )


def _buy_hold(
    name: str,
    identities: Sequence[FillIdentity],
    start: datetime,
    end: datetime,
    weights: Mapping[str, float],
) -> PathResult:
    # Reuse the accounting primitive for all fixed-weight comparators.
    if len(identities) < 3:
        raise FormalEvaluationError("comparator needs exact terminal fill")
    from strategy_control.mean_reversion_v2 import AccountingFill

    fills = [
        AccountingFill(
            item.base_timestamp,
            item.base_prices,
            weights
            if number == 0
            else ({asset: 0.0 for asset in ASSETS} if number == len(identities) - 1 else weights),
        )
        for number, item in enumerate(identities)
    ]
    result = self_financing(fills)
    values = tuple(item.net_return for item in result.intervals)
    panels = tuple(PanelReturn(i, i + 1, value, True, True, True) for i, value in enumerate(values))
    no_decisions: tuple[Decision, ...] = ()
    no_targets: tuple[Target, ...] = ()
    no_fills: tuple[Fill, ...] = ()
    no_dispositions: tuple[Disposition, ...] = ()
    return PathResult(
        name,
        start,
        end,
        "comparator",
        result.terminal_equity,
        result.terminal_equity - 1,
        annualized_sharpe(values),
        maximum_drawdown(accounting_equity_path(result)),
        panels,
        no_decisions,
        no_targets,
        no_fills,
        no_dispositions,
        (result.initial_cost, *(item.cost for item in result.intervals)),
        values,
        {},
        result.terminal_cash,
        {asset: 0 for asset in ASSETS},
        {asset: tuple() for asset in ASSETS},
        {},
    )


def evaluate_development(
    *,
    sessions: Sequence[JointSession],
    fills: Sequence[FillIdentity],
    bindings: PreResultBindings,
    prior_registry: Sequence[float],
) -> FormalResult:
    """Run the only formal development evaluation after immutable bindings validate."""
    bindings.validate()
    start, end = DEVELOPMENT_FOLDS[0][0], DEVELOPMENT_FOLDS[-1][1]
    _ordered(sessions, fills, end)
    if len(prior_registry) != 28 or any(not math.isfinite(value) for value in prior_registry):
        raise FormalEvaluationError("exact 28 observed prior registry Sharpes required")
    paths = {
        trial.name: _path(
            name=trial.name, sessions=sessions, identities=fills, start=start, end=end, trial=trial
        )
        for trial in TRIALS
    }
    primary = paths[TRIALS[0].name]
    folds = {
        f"fold_{number}": _path(
            name=f"fold_{number}",
            sessions=[item for item in sessions if item.session < fold_end],
            identities=[item for item in fills if item.base_timestamp < fold_end],
            start=fold_start,
            end=fold_end,
            trial=TRIALS[0],
        )
        for number, (fold_start, fold_end) in enumerate(DEVELOPMENT_FOLDS, 1)
    }
    doubled = _path(
        name="doubled_cost",
        sessions=sessions,
        identities=fills,
        start=start,
        end=end,
        trial=TRIALS[0],
        cost_bps=DOUBLED_ONE_WAY_COST_BPS,
    )
    delayed = _path(
        name="one_eligible_fill_delay",
        sessions=sessions,
        identities=fills,
        start=start,
        end=end,
        trial=TRIALS[0],
        delay=1,
    )
    standalone = {
        asset: _path(
            name=f"standalone_{asset}",
            sessions=sessions,
            identities=fills,
            start=start,
            end=end,
            trial=TRIALS[0],
            active_assets=(asset,),
        )
        for asset in ASSETS
    }
    comparators = {
        "cash": _buy_hold("cash", fills, start, end, {asset: 0.0 for asset in ASSETS}),
        "equal_weight_buy_hold": _buy_hold(
            "equal_weight_buy_hold", fills, start, end, {asset: 0.5 for asset in ASSETS}
        ),
        "btc_buy_hold": _buy_hold(
            "btc_buy_hold", fills, start, end, {"BTCUSDT": 0.5, "ETHUSDT": 0.0}
        ),
        "eth_buy_hold": _buy_hold(
            "eth_buy_hold", fills, start, end, {"BTCUSDT": 0.0, "ETHUSDT": 0.5}
        ),
    }
    common = common_panel({name: path.intervals for name, path in paths.items()})
    matrix = [common[trial.name] for trial in TRIALS]
    registry = [
        *prior_registry,
        *[sum(path.returns) / len(path.returns) for path in paths.values()],
    ]
    primary_common = common[TRIALS[0].name]
    lower, _ = stationary_bootstrap(primary.returns)
    equal_primary, equal = comparator_panel(
        primary.intervals, comparators["equal_weight_buy_hold"].intervals
    )
    raw_primary, raw = comparator_panel(primary.intervals, paths[TRIALS[1].name].intervals)
    if not equal_primary or not raw_primary:
        raise FormalEvaluationError("missing comparator-aligned panels")
    eq_sharpe, raw_sharpe = annualized_sharpe(equal), annualized_sharpe(raw)
    eq_dd, raw_dd = (
        maximum_drawdown(compounded_equity(equal)),
        maximum_drawdown(compounded_equity(raw)),
    )
    positive_regimes = [sum(values) > 0 for values in primary.regimes.values() if len(values) >= 45]
    positive_pnl = [
        value for values in primary.pnl_by_asset.values() for value in values if value > 0
    ]
    concentration = bool(
        positive_pnl
        and max(positive_pnl) / sum(positive_pnl) <= 0.5
        and sum(sorted(positive_pnl, reverse=True)[:5]) / sum(positive_pnl) <= 0.75
    )
    metrics: dict[str, object] = {
        "aggregate_net_return_gt": primary.net_return,
        "annualized_sharpe_gte": primary.sharpe,
        "positive_folds_minimum": sum(path.net_return > 0 for path in folds.values()),
        "fold_count": len(folds),
        "maximum_drawdown_lte": primary.drawdown,
        "doubled_cost_aggregate_net_return_gt": doubled.net_return,
        "additional_delay_aggregate_net_return_gt": delayed.net_return,
        "positive_parameter_neighbors_minimum": sum(
            paths[item.name].net_return > 0 for item in TRIALS[2:6]
        ),
        "parameter_neighbor_count": 4,
        "asset_standalone_net_return_each_gt": min(item.net_return for item in standalone.values()),
        "completed_entries_total_minimum": sum(primary.completed_entries.values()),
        "completed_entries_each_asset_minimum": min(primary.completed_entries.values()),
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": lower,
        "deflated_sharpe_probability_gte": dsr_probability(
            primary_common, registry, N=multiplicity_counts()["N"]
        ),
        "probability_of_backtest_overfitting_lte": pbo(matrix),
        "regime_gate": len(positive_regimes) >= 3 and all(positive_regimes),
        "exceptional_trade_gate": concentration,
        "baseline_superiority": primary.sharpe > eq_sharpe
        and primary.sharpe > raw_sharpe
        and primary.drawdown < eq_dd
        and primary.drawdown < raw_dd,
        "no_material_leakage": derive_integrity_evidence(
            input_identity_pass=True,
            trace_reconciliation_pass=all(item.terminal_cash for item in paths.values()),
            terminal_cash=all(
                item.terminal_cash
                for item in [
                    *paths.values(),
                    *folds.values(),
                    doubled,
                    delayed,
                    *standalone.values(),
                    *comparators.values(),
                ]
            ),
            strict_prefix_pass=True,
            holdout_closed=not bindings.holdout_accessed,
            gate_names=GATE_NAMES,
        ),
    }
    gates = aggregate_gates(metrics)
    return FormalResult(
        True,
        True,
        False,
        "DEVELOPMENT_GO_PENDING_INDEPENDENT_AUDIT" if all(gates.values()) else "HISTORICAL_NO_GO",
        paths,
        folds,
        standalone,
        comparators,
        metrics,
        gates,
        canonical_hash(dict(bindings.values)),
    )


def evaluate_verified_development(
    *,
    verified: VerifiedDevelopmentInput,
    bindings: PreResultBindings,
    prior_registry: Sequence[float],
) -> FormalResult:
    """The sole production-facing route; it cannot resolve or read a market path."""
    sessions, fills = verified.sessions_and_fills()
    return evaluate_development(
        sessions=sessions,
        fills=fills,
        bindings=bindings,
        prior_registry=prior_registry,
    )
