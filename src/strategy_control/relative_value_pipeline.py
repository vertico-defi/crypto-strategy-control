"""Development-only evaluator for the frozen BTC/ETH relative-value experiment."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from strategy_control.relative_value import (
    CASH,
    PARAMETER_NEIGHBORS,
    PRIMARY,
    SYMBOLS,
    TRIAL_ORDER,
    TRIALS,
    PortfolioInterval,
    RelativeValueError,
    Trial,
    atomic_fills,
    bootstrap,
    completed_holds,
    concentration,
    decide,
    deflated_sharpe,
    gate_checks,
    pbo,
    regime_history,
    score_at,
    self_financing_with_attribution,
)
from strategy_control.trend import DailyBar, Fill, aggregate_return, daily_sharpe, maximum_drawdown
from strategy_control.trend_pipeline import DevelopmentMarket

DEVELOPMENT_START = datetime(2025, 1, 1, tzinfo=UTC)
DEVELOPMENT_END = datetime(2026, 1, 1, tzinfo=UTC)
DEVELOPMENT_FOLDS = (
    (DEVELOPMENT_START, datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), DEVELOPMENT_END),
)
BASE_COST_BPS = 14.0
DOUBLED_COST_BPS = 28.0
RECOVERY_SESSIONS = 150


@dataclass(frozen=True)
class ExecutionRun:
    fills: tuple[Fill, ...]
    segments: tuple[int, ...]
    quarantine_liquidations: tuple[bool, ...]
    signal_sessions: tuple[datetime, ...]
    entry_decision_sessions: tuple[datetime | None, ...]
    prepared_days: tuple[DailyBar, ...]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise RelativeValueError("period boundaries must be timezone-aware UTC")
    return value.astimezone(UTC)


def _incomplete(day: DailyBar) -> DailyBar:
    return DailyBar(
        day.session,
        day.available_at,
        day.open,
        day.high,
        day.low,
        day.close,
        False,
    )


def _finite_positive(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def _prefix_market(
    market: DevelopmentMarket, end: datetime
) -> tuple[list[DailyBar], list[DailyBar], list[datetime]]:
    """Materialize only the causal fold prefix before any state-machine call."""

    end = _utc(end)
    if market.holdout_values_read or end > DEVELOPMENT_END:
        raise RelativeValueError("development boundary or holdout-read violation")
    btc = [day for day in market.days[SYMBOLS[0]] if _utc(day.session) < end]
    eth = [day for day in market.days[SYMBOLS[1]] if _utc(day.session) < end]
    btc_sessions = [_utc(day.session) for day in btc]
    eth_sessions = [_utc(day.session) for day in eth]
    if not btc_sessions or btc_sessions != eth_sessions:
        raise RelativeValueError("BTC and ETH fold-prefix sessions are not aligned")
    if any(btc_sessions[index] <= btc_sessions[index - 1] for index in range(1, len(btc))):
        raise RelativeValueError("fold-prefix sessions are not strictly chronological")
    return btc, eth, btc_sessions


def _canonical_fill(
    market: DevelopmentMarket,
    btc: DailyBar,
    eth: DailyBar,
    *,
    end: datetime,
) -> tuple[datetime, dict[str, float]] | None:
    session = _utc(btc.session)
    entries = [market.causal_fills[symbol].get(session) for symbol in SYMBOLS]
    if any(entry is None for entry in entries):
        return None
    resolved = [entry for entry in entries if entry is not None]
    timestamps = {_utc(entry[0]) for entry in resolved}
    if len(timestamps) != 1:
        return None
    timestamp = _utc(resolved[0][0])
    cutoff = max(
        session + timedelta(days=1),
        _utc(btc.available_at),
        _utc(eth.available_at),
    )
    if timestamp <= cutoff or timestamp >= end:
        return None
    prices = {symbol: resolved[position][1] for position, symbol in enumerate(SYMBOLS)}
    if any(not _finite_positive(prices[symbol]) for symbol in SYMBOLS):
        return None
    return timestamp, {symbol: float(prices[symbol]) for symbol in SYMBOLS}


def build_period_run(
    market: DevelopmentMarket,
    trial: Trial,
    start: datetime,
    end: datetime,
    *,
    execution_delay_sessions: int = 0,
) -> ExecutionRun:
    """Execute one independent cash-initialized, half-open causal period.

    Missing information while exposed is never dropped: the held position is
    liquidated at the earliest later synchronized priced fill, or evaluation fails.
    Cash quarantine and recovery time create no synthetic zero-return intervals.
    """

    start, end = _utc(start), _utc(end)
    if end <= start or end > DEVELOPMENT_END or execution_delay_sessions not in {0, 1}:
        raise RelativeValueError("invalid development period or execution delay")
    btc, eth, sessions = _prefix_market(market, end)
    prepared_btc, prepared_eth = list(btc), list(eth)
    events: list[tuple[datetime, dict[str, float]] | None] = []
    previous: datetime | None = None
    for index, session in enumerate(sessions):
        contiguous = previous is None or session == previous + timedelta(days=1)
        previous = session
        fill = _canonical_fill(market, btc[index], eth[index], end=end)
        signal_complete = btc[index].complete and eth[index].complete and contiguous
        # A missing fill is a causal gap only when its ordinary post-close time lies
        # inside the boundary.  The final session whose C_s lies outside is ignored.
        expected_inside = session + timedelta(days=1, minutes=1) < end
        usable = signal_complete and fill is not None
        if not usable and expected_inside:
            prepared_btc[index] = _incomplete(prepared_btc[index])
            prepared_eth[index] = _incomplete(prepared_eth[index])
        # Preserve a synchronized finite price vector even when the signal session
        # itself is quarantined: an already-exposed portfolio must liquidate at the
        # earliest such later fill and retain the full economic interval.
        events.append(fill)

    in_boundary_events = [
        event[0]
        for event in events
        if event is not None and start <= event[0] < end
    ]
    if len(in_boundary_events) < 2:
        raise RelativeValueError("insufficient synchronized canonical fills in period")
    terminal_sessions = [
        session for session in sessions if session + timedelta(days=1, minutes=1) < end
    ]
    if not terminal_sessions:
        raise RelativeValueError("period has no ex-ante terminal signal session")
    terminal_session = terminal_sessions[-1]

    timestamps: list[datetime] = []
    prices: list[Mapping[str, float]] = []
    targets: list[str] = []
    segments: list[int] = []
    liquidations: list[bool] = []
    fill_sessions: list[datetime] = []
    entry_sessions: list[datetime | None] = []
    actual = CASH
    pending: tuple[str, datetime] | None = None
    segment = 0
    run = 0
    active = False
    awaiting_liquidation = False

    def append_fill(
        event: tuple[datetime, dict[str, float]],
        target: str,
        signal_session: datetime,
        *,
        liquidation: bool = False,
        entry_session: datetime | None = None,
    ) -> None:
        timestamp, price = event
        if timestamp < start or timestamp >= end:
            return
        if timestamps and timestamp <= timestamps[-1]:
            raise RelativeValueError("canonical fills are not strictly chronological")
        timestamps.append(timestamp)
        prices.append(price)
        targets.append(target)
        segments.append(segment)
        liquidations.append(liquidation)
        fill_sessions.append(signal_session)
        entry_sessions.append(entry_session)

    for index, session in enumerate(sessions):
        event = events[index]
        expected_inside = session + timedelta(days=1, minutes=1) < end
        signal_valid = prepared_btc[index].complete and prepared_eth[index].complete

        if awaiting_liquidation:
            if event is not None and event[0] >= start:
                append_fill(event, CASH, session, liquidation=True)
                actual = CASH
                awaiting_liquidation = False
                segment += 1
                run = 0
                active = False
            continue

        if not signal_valid or (event is None and expected_inside):
            pending = None
            run = 0
            if actual != CASH:
                if event is not None and event[0] >= start:
                    append_fill(event, CASH, session, liquidation=True)
                    actual = CASH
                    segment += 1
                    active = False
                else:
                    awaiting_liquidation = True
            else:
                segment += 1
                active = False
            continue

        if event is None:
            continue
        run += 1
        timestamp = event[0]
        if timestamp < start:
            continue
        if not active:
            if run < RECOVERY_SESSIONS:
                continue
            active = True

        terminal = session == terminal_session
        filled_entry_session: datetime | None = None
        if execution_delay_sessions == 1 and pending is not None:
            previous_target = actual
            actual, filled_entry_session = pending
            pending = None
            if actual == previous_target:
                filled_entry_session = None
        if terminal:
            actual = CASH
            pending = None
            filled_entry_session = None
        elif session >= start:
            desired = decide(
                score_at(prepared_btc, index, trial),
                score_at(prepared_eth, index, trial),
                actual,
                trial,
            )
            if execution_delay_sessions == 0:
                if desired != actual:
                    actual = desired
                    filled_entry_session = session if desired != CASH else None
            elif desired != actual:
                pending = (desired, session)
        append_fill(
            event,
            actual,
            session,
            entry_session=filled_entry_session,
        )

    if awaiting_liquidation or actual != CASH:
        raise RelativeValueError("DATA_INTEGRITY_FAILURE: exposed liquidation endpoint unavailable")
    fills = atomic_fills(timestamps, prices, targets)
    if len(fills) < 2 or fills[-1].targets != {symbol: 0.0 for symbol in SYMBOLS}:
        raise RelativeValueError("period did not end in exact cash")
    return ExecutionRun(
        tuple(fills),
        tuple(segments),
        tuple(liquidations),
        tuple(fill_sessions),
        tuple(entry_sessions),
        tuple(prepared_btc),
    )


def _returns(intervals: Sequence[PortfolioInterval]) -> list[float]:
    return [item.net_return for item in intervals]


def _summary(intervals: Sequence[PortfolioInterval]) -> dict[str, Any]:
    values = _returns(intervals)
    if not values:
        raise RelativeValueError("empty period result")
    sharpe = daily_sharpe(values)
    return {
        "intervals": len(values),
        "net_return": aggregate_return(values),
        "annualized_sharpe": sharpe * math.sqrt(365) if math.isfinite(sharpe) else None,
        "maximum_drawdown": maximum_drawdown(values),
        "turnover": sum(item.turnover for item in intervals),
        "cost": sum(item.cost for item in intervals),
    }


def _account_run(run: ExecutionRun, cost_bps: float) -> list[PortfolioInterval]:
    return self_financing_with_attribution(
        run.fills,
        one_way_cost_bps=cost_bps,
        segments=run.segments,
        quarantine_liquidations=run.quarantine_liquidations,
    )


def _benchmark_calendar_run(
    market: DevelopmentMarket,
    start: datetime,
    end: datetime,
    *,
    exposed_when_active: bool,
) -> ExecutionRun:
    """Build a data-only cash or buy-and-hold quarantine/recovery calendar."""

    start, end = _utc(start), _utc(end)
    if end <= start or end > DEVELOPMENT_END:
        raise RelativeValueError("invalid benchmark development period")
    btc, eth, sessions = _prefix_market(market, end)
    events: list[tuple[datetime, dict[str, float]] | None] = []
    valid_signals: list[bool] = []
    previous: datetime | None = None
    for index, session in enumerate(sessions):
        contiguous = previous is None or session == previous + timedelta(days=1)
        previous = session
        event = _canonical_fill(market, btc[index], eth[index], end=end)
        expected_inside = session + timedelta(days=1, minutes=1) < end
        valid_signals.append(
            bool(btc[index].complete and eth[index].complete and contiguous and event is not None)
            or not expected_inside
        )
        events.append(event)
    terminal_sessions = [
        session for session in sessions if session + timedelta(days=1, minutes=1) < end
    ]
    if not terminal_sessions:
        raise RelativeValueError("benchmark has no ex-ante terminal session")
    terminal_session = terminal_sessions[-1]
    timestamps: list[datetime] = []
    prices: list[Mapping[str, float]] = []
    segments: list[int] = []
    liquidations: list[bool] = []
    fill_sessions: list[datetime] = []
    segment = 0
    run = 0
    active = False
    awaiting_liquidation = False

    def append_event(
        event: tuple[datetime, dict[str, float]], session: datetime, *, liquidation: bool = False
    ) -> None:
        if not (start <= event[0] < end):
            return
        if timestamps and event[0] <= timestamps[-1]:
            raise RelativeValueError("benchmark fills are not strictly chronological")
        timestamps.append(event[0])
        prices.append(event[1])
        segments.append(segment)
        liquidations.append(liquidation)
        fill_sessions.append(session)

    for index, session in enumerate(sessions):
        event = events[index]
        expected_inside = session + timedelta(days=1, minutes=1) < end
        signal_valid = valid_signals[index]
        if awaiting_liquidation:
            if event is not None and event[0] >= start:
                append_event(event, session, liquidation=True)
                awaiting_liquidation = False
                segment += 1
                run = 0
                active = False
            continue
        if not signal_valid or (event is None and expected_inside):
            run = 0
            if active and exposed_when_active:
                if event is not None and event[0] >= start:
                    append_event(event, session, liquidation=True)
                    segment += 1
                    active = False
                else:
                    awaiting_liquidation = True
            else:
                segment += 1
                active = False
            continue
        if event is None:
            continue
        run += 1
        if event[0] < start:
            continue
        if not active:
            if run < RECOVERY_SESSIONS:
                continue
            active = True
        append_event(event, session)
        if session == terminal_session:
            active = False
    if awaiting_liquidation:
        raise RelativeValueError(
            "DATA_INTEGRITY_FAILURE: benchmark liquidation endpoint unavailable"
        )
    fills = atomic_fills(timestamps, prices, [CASH] * len(timestamps))
    if len(fills) < 2:
        raise RelativeValueError("benchmark has insufficient valid intervals")
    return ExecutionRun(
        tuple(fills),
        tuple(segments),
        tuple(liquidations),
        tuple(fill_sessions),
        tuple(None for _ in fills),
        tuple(btc),
    )


def _segmented_buy_and_hold(
    run: ExecutionRun, weights: Mapping[str, float], cost_bps: float = BASE_COST_BPS
) -> list[PortfolioInterval]:
    if set(weights) != set(SYMBOLS) or any(value < 0 for value in weights.values()):
        raise RelativeValueError("invalid benchmark weights")
    if sum(weights.values()) > 1.0 + 1e-12:
        raise RelativeValueError("benchmark exposure exceeds one")
    result: list[PortfolioInterval] = []
    wealth = 1.0
    rate = cost_bps / 10_000.0
    cursor = 0
    while cursor < len(run.fills):
        segment = run.segments[cursor]
        stop = cursor + 1
        while stop < len(run.fills) and run.segments[stop] == segment:
            stop += 1
        group = run.fills[cursor:stop]
        if len(group) < 2:
            cursor = stop
            continue
        baseline = wealth
        entry_turnover = sum(weights.values())
        entry_cost = wealth * entry_turnover * rate
        wealth -= entry_cost
        holdings = {
            symbol: wealth * weights[symbol] / group[0].prices[symbol] for symbol in SYMBOLS
        }
        cash = wealth * (1.0 - sum(weights.values()))
        prior_equity = baseline
        for position in range(1, len(group)):
            fill = group[position]
            marked = cash + sum(
                holdings[symbol] * fill.prices[symbol] for symbol in SYMBOLS
            )
            turnover = entry_turnover if position == 1 else 0.0
            cost = entry_cost if position == 1 else 0.0
            if position == len(group) - 1:
                exit_turnover = (marked - cash) / marked
                exit_cost = marked * exit_turnover * rate
                marked -= exit_cost
                turnover += exit_turnover
                cost += exit_cost
            result.append(
                PortfolioInterval(
                    group[position - 1].timestamp,
                    fill.timestamp,
                    marked / prior_equity - 1.0,
                    marked,
                    turnover,
                    cost,
                    {symbol: 0.0 for symbol in SYMBOLS},
                    {symbol: 0.0 for symbol in SYMBOLS},
                    segment,
                    run.quarantine_liquidations[cursor + position],
                )
            )
            prior_equity = marked
        wealth = prior_equity
        cursor = stop
    if not result:
        raise RelativeValueError("benchmark has no valid intervals")
    return result


def _completed_entry_regimes(
    run: ExecutionRun, labels: Mapping[datetime, str | None]
) -> dict[str, int]:
    counts: dict[str, int] = {}
    open_entry: tuple[str, str | None] | None = None
    prior = CASH
    for index, fill in enumerate(run.fills):
        target = next((symbol for symbol in SYMBOLS if fill.targets[symbol] == 1.0), CASH)
        if target == prior:
            continue
        if prior != CASH and open_entry is not None:
            _, label = open_entry
            if label is not None:
                counts[label] = counts.get(label, 0) + 1
            open_entry = None
        if target != CASH:
            decision_session = run.entry_decision_sessions[index]
            label = labels.get(decision_session) if decision_session is not None else None
            open_entry = (
                target,
                label,
            )
        prior = target
    return counts


def _regime_report(
    run: ExecutionRun, intervals: Sequence[PortfolioInterval]
) -> tuple[dict[str, dict[str, Any]], bool]:
    regime_values = regime_history(run.prepared_days)
    labels = {
        _utc(day.session): regime_values[index] for index, day in enumerate(run.prepared_days)
    }
    signal_by_fill = {
        fill.timestamp: run.signal_sessions[index] for index, fill in enumerate(run.fills)
    }
    returns: dict[str, list[float]] = {}
    for interval in intervals:
        interval_session = signal_by_fill.get(interval.start)
        label = labels.get(interval_session) if interval_session is not None else None
        if label is not None:
            returns.setdefault(label, []).append(interval.net_return)
    entries = _completed_entry_regimes(run, labels)
    names = sorted(set(returns) | set(entries))
    report = {
        name: {
            "intervals": len(returns.get(name, [])),
            "completed_entries": entries.get(name, 0),
            "net_return": aggregate_return(returns[name]) if returns.get(name) else 0.0,
        }
        for name in names
    }
    eligible = [
        item
        for item in report.values()
        if item["intervals"] >= 45 and item["completed_entries"] >= 5
    ]
    return report, len(eligible) >= 3 and all(item["net_return"] > 0.0 for item in eligible)


def evaluate_development(
    market: DevelopmentMarket,
    preregistration: Mapping[str, Any],
    *,
    bootstrap_rng: Any | None = None,
) -> dict[str, Any]:
    """Evaluate the frozen 2025 development stage without touching 2026 values."""

    if market.holdout_values_read:
        raise RelativeValueError("holdout values were read")
    if tuple(TRIALS) != TRIAL_ORDER or len(TRIAL_ORDER) != 7:
        raise RelativeValueError("frozen trial mapping changed")
    runs = {
        name: build_period_run(market, trial, DEVELOPMENT_START, DEVELOPMENT_END)
        for name, trial in TRIALS.items()
    }
    intervals = {name: _account_run(run, BASE_COST_BPS) for name, run in runs.items()}
    summaries = {name: _summary(values) for name, values in intervals.items()}
    primary_run = runs[PRIMARY.name]
    primary_intervals = intervals[PRIMARY.name]
    primary = summaries[PRIMARY.name]
    doubled = _summary(_account_run(primary_run, DOUBLED_COST_BPS))
    delayed_run = build_period_run(
        market,
        PRIMARY,
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        execution_delay_sessions=1,
    )
    delayed = _summary(_account_run(delayed_run, BASE_COST_BPS))

    folds: list[dict[str, Any]] = []
    for start, end in DEVELOPMENT_FOLDS:
        fold_run = build_period_run(market, PRIMARY, start, end)
        fold_intervals = _account_run(fold_run, BASE_COST_BPS)
        summary = _summary(fold_intervals)
        summary.update(
            {
                "start_utc": start.isoformat(),
                "end_exclusive_utc": end.isoformat(),
                "completed_entries": sum(completed_holds(fold_run.fills).values()),
            }
        )
        folds.append(summary)
    eligible_folds = [fold for fold in folds if fold["intervals"] >= 60]
    positive_folds = sum(fold["net_return"] > 0.0 for fold in eligible_folds)

    cash_run = _benchmark_calendar_run(
        market, DEVELOPMENT_START, DEVELOPMENT_END, exposed_when_active=False
    )
    buy_hold_run = _benchmark_calendar_run(
        market, DEVELOPMENT_START, DEVELOPMENT_END, exposed_when_active=True
    )
    cash_summary = _summary(_account_run(cash_run, 0.0))
    benchmarks = {
        "cash_zero_return": cash_summary,
        "BTCUSDT_buy_and_hold": _summary(
            _segmented_buy_and_hold(buy_hold_run, {"BTCUSDT": 1.0, "ETHUSDT": 0.0})
        ),
        "ETHUSDT_buy_and_hold": _summary(
            _segmented_buy_and_hold(buy_hold_run, {"BTCUSDT": 0.0, "ETHUSDT": 1.0})
        ),
        "equal_weight_BTC_ETH_buy_and_hold": _summary(
            _segmented_buy_and_hold(buy_hold_run, {"BTCUSDT": 0.5, "ETHUSDT": 0.5})
        ),
        "raw_60_session_relative_strength_rotation": summaries[
            "raw_60_session_relative_strength_rotation"
        ],
    }
    equal_weight = benchmarks["equal_weight_BTC_ETH_buy_and_hold"]
    raw = benchmarks["raw_60_session_relative_strength_rotation"]
    baseline_superiority = bool(
        primary["annualized_sharpe"] is not None
        and equal_weight["annualized_sharpe"] is not None
        and raw["annualized_sharpe"] is not None
        and primary["net_return"] > equal_weight["net_return"]
        and primary["net_return"] > raw["net_return"]
        and primary["annualized_sharpe"] > equal_weight["annualized_sharpe"]
        and primary["annualized_sharpe"] > raw["annualized_sharpe"]
        and primary["maximum_drawdown"] < equal_weight["maximum_drawdown"]
    )

    endpoints = {
        name: {(item.start, item.end): item.net_return for item in values}
        for name, values in intervals.items()
    }
    common = sorted(set.intersection(*(set(values) for values in endpoints.values())))
    alternatives = [[endpoints[name][key] for key in common] for name in TRIAL_ORDER]
    bootstrap_report = bootstrap(_returns(primary_intervals), rng=bootstrap_rng)
    dsr = deflated_sharpe(alternatives[0], alternatives)
    pbo_value = pbo(alternatives)
    holds = completed_holds(primary_run.fills)
    contributions = {
        symbol: sum(
            item.gross_pnl[symbol] - item.cost_attribution[symbol]
            for item in primary_intervals
        )
        for symbol in SYMBOLS
    }
    exceptional = concentration(primary_intervals)
    regimes, regime_pass = _regime_report(primary_run, primary_intervals)
    metrics = {
        "aggregate_net_return_gt": primary["net_return"],
        "annualized_sharpe_gte": primary["annualized_sharpe"],
        "maximum_drawdown_lte": primary["maximum_drawdown"],
        "fold_count": len(eligible_folds),
        "positive_folds_minimum": positive_folds,
        "doubled_cost_aggregate_net_return_gt": doubled["net_return"],
        "additional_delay_aggregate_net_return_gt": delayed["net_return"],
        "positive_parameter_neighbors_minimum": sum(
            summaries[name]["net_return"] > 0.0 for name in PARAMETER_NEIGHBORS
        ),
        "parameter_neighbor_count": len(PARAMETER_NEIGHBORS),
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": bootstrap_report["lower_95"],
        "deflated_sharpe_probability_gte": dsr,
        "probability_of_backtest_overfitting_lte": pbo_value,
        "baseline_superiority": baseline_superiority,
        "completed_entries_total_minimum": sum(holds.values()),
        "completed_holds_each_asset_minimum": min(holds.values()),
        "asset_net_contribution_each_gt": min(contributions.values()),
        "exceptional_profit_gate": "pass" if exceptional["pass"] else "fail",
        "regime_gate": "pass" if regime_pass else "fail",
        "no_material_leakage": True,
    }
    gates = preregistration.get("development_gates_all_required")
    if not isinstance(gates, Mapping):
        raise RelativeValueError("frozen development gates are missing")
    checks = gate_checks(metrics, gates)
    all_pass = len(checks) == len(gates) and all(checks.values())
    return {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-relative-value-rotation-v1",
        "stage": "DEVELOPMENT",
        "classification": "DEVELOPMENT_GO" if all_pass else "HISTORICAL_NO_GO",
        "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "all_development_gates_pass": all_pass,
        "gate_checks": checks,
        "metrics": metrics,
        "folds": folds,
        "primary": primary,
        "variants": summaries,
        "benchmarks": benchmarks,
        "doubled_cost": doubled,
        "additional_delay": delayed,
        "completed_holds": holds,
        "asset_net_contributions": contributions,
        "bootstrap": bootstrap_report,
        "deflated_sharpe_probability": dsr,
        "probability_of_backtest_overfitting": pbo_value,
        "multiplicity_aligned_interval_count": len(common),
        "regimes": regimes,
        "portfolio_concentration": exceptional,
        "quarantine_liquidation_intervals": sum(
            item.quarantine_liquidation for item in primary_intervals
        ),
        "source_partition_count": market.source_partition_count,
        "holdout_values_read": False,
        "holdout_opened": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
    }
