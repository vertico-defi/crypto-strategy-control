"""Development-only evaluator for the frozen BTC/ETH mean-reversion experiment."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from strategy_control.mean_reversion import (
    PARAMETER_NEIGHBORS,
    TRIAL_ORDER,
    VARIANTS,
    MeanReversionConfig,
    MeanReversionError,
    account_portfolio,
    aggregate_net_return,
    asset_state_machine,
    atomic_portfolio_fills,
    bootstrap,
    completed_entries,
    concentration,
    deflated_sharpe,
    forced_terminal_cash,
    gate_checks,
    max_drawdown,
    pbo,
    regimes,
)
from strategy_control.trend import DailyBar, Fill, IntervalResult, buy_and_hold, daily_sharpe
from strategy_control.trend_pipeline import DevelopmentMarket

SYMBOLS = ("BTCUSDT", "ETHUSDT")
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


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise MeanReversionError("period boundaries must be timezone-aware UTC")
    return value.astimezone(UTC)


def _incomplete(day: DailyBar) -> DailyBar:
    return DailyBar(
        session=day.session,
        available_at=day.available_at,
        open=day.open,
        high=day.high,
        low=day.low,
        close=day.close,
        complete=False,
    )


def _prepared_days(
    market: DevelopmentMarket, end: datetime
) -> tuple[dict[str, list[DailyBar]], list[datetime]]:
    """Treat any required asynchronous/missing fill as a causal gap for both assets."""

    days = {symbol: list(market.days[symbol]) for symbol in SYMBOLS}
    sessions = [day.session for day in days[SYMBOLS[0]]]
    if not sessions or any(
        [day.session for day in days[symbol]] != sessions for symbol in SYMBOLS[1:]
    ):
        raise MeanReversionError("asset daily sessions are not aligned")
    prepared = {symbol: list(days[symbol]) for symbol in SYMBOLS}
    for index, session in enumerate(sessions):
        entries = [market.causal_fills[symbol].get(session) for symbol in SYMBOLS]
        earliest_expected = session + timedelta(days=1, minutes=1)
        required_inside_period = earliest_expected < end
        synchronized = bool(
            all(entry is not None for entry in entries)
            and len({entry[0] for entry in entries if entry is not None}) == 1
        )
        complete = all(days[symbol][index].complete for symbol in SYMBOLS)
        if not complete or (required_inside_period and not synchronized):
            for symbol in SYMBOLS:
                prepared[symbol][index] = _incomplete(prepared[symbol][index])
    return prepared, sessions


def build_period_fills(
    market: DevelopmentMarket,
    config: MeanReversionConfig,
    start: datetime,
    end: datetime,
    *,
    execution_delay_sessions: int = 0,
    standalone_symbol: str | None = None,
) -> list[Fill]:
    """Build independent-from-cash, synchronized, terminally liquidated period fills."""

    start, end = _utc(start), _utc(end)
    if end <= start or end > DEVELOPMENT_END:
        raise MeanReversionError("development period crossed the frozen boundary")
    if standalone_symbol is not None and standalone_symbol not in SYMBOLS:
        raise MeanReversionError("unsupported standalone asset")
    prepared, sessions = _prepared_days(market, end)
    decisions = {
        symbol: asset_state_machine(
            prepared[symbol],
            config,
            execution_delay_sessions=execution_delay_sessions,
            recovery_sessions=150,
            decision_start=start,
        )
        for symbol in SYMBOLS
    }
    timestamps: list[datetime] = []
    prices: list[Mapping[str, float]] = []
    targets: list[Mapping[str, float]] = []
    for index, session in enumerate(sessions):
        if index + 1 >= len(sessions):
            continue
        entries = [market.causal_fills[symbol].get(session) for symbol in SYMBOLS]
        if any(entry is None for entry in entries):
            continue
        resolved = [entry for entry in entries if entry is not None]
        fill_timestamps = {entry[0] for entry in resolved}
        if len(fill_timestamps) != 1:
            continue
        timestamp = resolved[0][0]
        if timestamp < start or timestamp >= end:
            continue
        if standalone_symbol is None:
            timestamps.append(timestamp)
            prices.append(
                {symbol: resolved[position][1] for position, symbol in enumerate(SYMBOLS)}
            )
            targets.append(
                {
                    symbol: 0.5 if decisions[symbol][index + 1].actual_long else 0.0
                    for symbol in SYMBOLS
                }
            )
        else:
            position = SYMBOLS.index(standalone_symbol)
            timestamps.append(timestamp)
            prices.append({standalone_symbol: resolved[position][1]})
            targets.append(
                {
                    standalone_symbol: (
                        1.0 if decisions[standalone_symbol][index + 1].actual_long else 0.0
                    )
                }
            )
    if len(timestamps) < 2:
        raise MeanReversionError("insufficient synchronized fills for evaluation")
    if standalone_symbol is None:
        fills = atomic_portfolio_fills(timestamps, prices, targets)
    else:
        fills = [
            Fill(timestamp, price, target)
            for timestamp, price, target in zip(timestamps, prices, targets, strict=True)
        ]
    return forced_terminal_cash(fills)


def _returns(intervals: Sequence[IntervalResult]) -> list[float]:
    return [interval.net_return for interval in intervals]


def _period_summary(intervals: Sequence[IntervalResult]) -> dict[str, Any]:
    values = _returns(intervals)
    if not values:
        raise MeanReversionError("empty period result")
    sharpe = daily_sharpe(values)
    return {
        "intervals": len(values),
        "net_return": aggregate_net_return(values),
        "annualized_sharpe": sharpe * math.sqrt(365) if math.isfinite(sharpe) else None,
        "maximum_drawdown": max_drawdown(values),
        "turnover": sum(interval.turnover for interval in intervals),
        "cost": sum(interval.cost for interval in intervals),
    }


def _regime_report(
    market: DevelopmentMarket, fills: Sequence[Fill], intervals: Sequence[IntervalResult]
) -> tuple[dict[str, dict[str, Any]], bool]:
    labels = regimes(market.days["BTCUSDT"])
    regime_by_fill: dict[datetime, str] = {}
    for index, day in enumerate(market.days["BTCUSDT"]):
        entry = market.causal_fills["BTCUSDT"].get(day.session)
        if entry is not None and labels[index] is not None:
            regime_by_fill[entry[0]] = str(labels[index])
    returns_by_regime: dict[str, list[float]] = {}
    for interval in intervals:
        label = regime_by_fill.get(interval.start)
        if label is not None:
            returns_by_regime.setdefault(label, []).append(interval.net_return)
    entries_by_regime: dict[str, int] = {}
    prior = {symbol: 0.0 for symbol in SYMBOLS}
    for fill in fills:
        label = regime_by_fill.get(fill.timestamp)
        if label is not None:
            for symbol in SYMBOLS:
                if prior[symbol] == 0.0 and fill.targets[symbol] > 0.0:
                    entries_by_regime[label] = entries_by_regime.get(label, 0) + 1
        prior = dict(fill.targets)
    names = sorted(set(returns_by_regime) | set(entries_by_regime))
    report = {
        name: {
            "intervals": len(returns_by_regime.get(name, [])),
            "completed_entries": entries_by_regime.get(name, 0),
            "net_return": (
                aggregate_net_return(returns_by_regime[name])
                if returns_by_regime.get(name)
                else 0.0
            ),
        }
        for name in names
    }
    eligible = [
        item
        for item in report.values()
        if item["intervals"] >= 45 and item["completed_entries"] >= 5
    ]
    passed = len(eligible) >= 3 and all(
        item["net_return"] > 0.0 and item["net_return"] >= -0.05 for item in eligible
    )
    return report, passed


def evaluate_development(
    market: DevelopmentMarket,
    preregistration: Mapping[str, Any],
    *,
    bootstrap_rng: Any | None = None,
) -> dict[str, Any]:
    """Evaluate the single frozen 2025 development stage and never touch 2026 values."""

    if market.holdout_values_read:
        raise MeanReversionError("holdout values were read")
    if tuple(VARIANTS) != TRIAL_ORDER or len(TRIAL_ORDER) != 7:
        raise MeanReversionError("frozen trial mapping changed")
    fills_by_variant = {
        name: build_period_fills(market, config, DEVELOPMENT_START, DEVELOPMENT_END)
        for name, config in VARIANTS.items()
    }
    intervals_by_variant = {
        name: account_portfolio(fills, BASE_COST_BPS)
        for name, fills in fills_by_variant.items()
    }
    variant_summaries = {
        name: _period_summary(intervals) for name, intervals in intervals_by_variant.items()
    }
    primary_fills = fills_by_variant[TRIAL_ORDER[0]]
    primary_intervals = intervals_by_variant[TRIAL_ORDER[0]]
    primary = variant_summaries[TRIAL_ORDER[0]]
    doubled = _period_summary(account_portfolio(primary_fills, DOUBLED_COST_BPS))
    delayed_fills = build_period_fills(
        market,
        VARIANTS[TRIAL_ORDER[0]],
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        execution_delay_sessions=1,
    )
    delayed = _period_summary(account_portfolio(delayed_fills, BASE_COST_BPS))

    fold_reports: list[dict[str, Any]] = []
    for start, end in DEVELOPMENT_FOLDS:
        fills = build_period_fills(market, VARIANTS[TRIAL_ORDER[0]], start, end)
        intervals = account_portfolio(fills, BASE_COST_BPS)
        summary = _period_summary(intervals)
        summary.update(
            {
                "start_utc": start.isoformat(),
                "end_exclusive_utc": end.isoformat(),
                "completed_entries": sum(completed_entries(fills).values()),
            }
        )
        fold_reports.append(summary)
    eligible_folds = [fold for fold in fold_reports if fold["intervals"] >= 60]
    positive_folds = sum(fold["net_return"] > 0.0 for fold in eligible_folds)

    standalone: dict[str, dict[str, Any]] = {}
    standalone_concentration: dict[str, dict[str, object]] = {}
    for symbol in SYMBOLS:
        fills = build_period_fills(
            market,
            VARIANTS[TRIAL_ORDER[0]],
            DEVELOPMENT_START,
            DEVELOPMENT_END,
            standalone_symbol=symbol,
        )
        intervals = account_portfolio(fills, BASE_COST_BPS)
        standalone[symbol] = _period_summary(intervals)
        standalone[symbol]["completed_entries"] = completed_entries(fills)[symbol]
        standalone_concentration[symbol] = concentration(intervals)

    benchmarks = {
        "cash_zero_return": {
            "intervals": len(primary_intervals),
            "net_return": 0.0,
            "annualized_sharpe": None,
            "maximum_drawdown": 0.0,
        },
        "BTCUSDT_buy_and_hold": _period_summary(
            buy_and_hold(primary_fills, {"BTCUSDT": 1.0, "ETHUSDT": 0.0}, BASE_COST_BPS)
        ),
        "ETHUSDT_buy_and_hold": _period_summary(
            buy_and_hold(primary_fills, {"BTCUSDT": 0.0, "ETHUSDT": 1.0}, BASE_COST_BPS)
        ),
        "equal_weight_BTC_ETH_buy_and_hold": _period_summary(
            buy_and_hold(primary_fills, {"BTCUSDT": 0.5, "ETHUSDT": 0.5}, BASE_COST_BPS)
        ),
        "raw_three_session_drawdown_baseline": variant_summaries[
            "raw_three_session_drawdown_baseline"
        ],
    }
    equal_weight = benchmarks["equal_weight_BTC_ETH_buy_and_hold"]
    raw_baseline = benchmarks["raw_three_session_drawdown_baseline"]
    baseline_superiority = bool(
        primary["annualized_sharpe"] is not None
        and equal_weight["annualized_sharpe"] is not None
        and raw_baseline["annualized_sharpe"] is not None
        and primary["annualized_sharpe"] > equal_weight["annualized_sharpe"]
        and primary["annualized_sharpe"] > raw_baseline["annualized_sharpe"]
        and primary["maximum_drawdown"] < equal_weight["maximum_drawdown"]
        and primary["maximum_drawdown"] < raw_baseline["maximum_drawdown"]
    )

    by_variant = {
        name: {(item.start, item.end): item.net_return for item in intervals}
        for name, intervals in intervals_by_variant.items()
    }
    common = set.intersection(*(set(values) for values in by_variant.values()))
    ordered_common = sorted(common)
    alternative_returns = [
        [by_variant[name][interval] for interval in ordered_common] for name in TRIAL_ORDER
    ]
    primary_aligned = alternative_returns[0]
    bootstrap_report = bootstrap(_returns(primary_intervals), rng=bootstrap_rng)
    dsr = deflated_sharpe(primary_aligned, alternative_returns)
    pbo_value = pbo(alternative_returns)
    portfolio_concentration = concentration(primary_intervals)
    all_concentration_pass = bool(
        portfolio_concentration["pass"]
        and all(value["pass"] for value in standalone_concentration.values())
    )
    regime_report, regime_pass = _regime_report(market, primary_fills, primary_intervals)
    entry_counts = completed_entries(primary_fills)
    neighbor_positive = sum(
        variant_summaries[name]["net_return"] > 0.0 for name in PARAMETER_NEIGHBORS
    )
    metrics = {
        "aggregate_net_return_gt": primary["net_return"],
        "annualized_sharpe_gte": primary["annualized_sharpe"],
        "positive_folds_minimum": positive_folds,
        "fold_count": len(eligible_folds),
        "maximum_drawdown_lte": primary["maximum_drawdown"],
        "doubled_cost_aggregate_net_return_gt": doubled["net_return"],
        "additional_delay_aggregate_net_return_gt": delayed["net_return"],
        "positive_parameter_neighbors_minimum": neighbor_positive,
        "parameter_neighbor_count": len(PARAMETER_NEIGHBORS),
        "asset_standalone_net_return_each_gt": min(
            standalone[symbol]["net_return"] for symbol in SYMBOLS
        ),
        "completed_entries_total_minimum": sum(entry_counts.values()),
        "completed_entries_each_asset_minimum": min(entry_counts.values()),
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": bootstrap_report["lower_95"],
        "deflated_sharpe_probability_gte": dsr,
        "probability_of_backtest_overfitting_lte": pbo_value,
        "regime_gate": "pass" if regime_pass else "fail",
        "exceptional_trade_gate": "pass" if all_concentration_pass else "fail",
        "baseline_superiority": baseline_superiority,
        "no_material_leakage": True,
    }
    gates = preregistration.get("development_gates_all_required")
    if not isinstance(gates, Mapping):
        raise MeanReversionError("frozen development gates are missing")
    checks = gate_checks(metrics, gates)
    all_pass = len(checks) == len(gates) and all(checks.values())
    return {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-long-only-mean-reversion-v1",
        "stage": "DEVELOPMENT",
        "classification": "DEVELOPMENT_GO" if all_pass else "HISTORICAL_NO_GO",
        "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "all_development_gates_pass": all_pass,
        "gate_checks": checks,
        "metrics": metrics,
        "folds": fold_reports,
        "primary": primary,
        "variants": variant_summaries,
        "benchmarks": benchmarks,
        "doubled_cost": doubled,
        "additional_delay": delayed,
        "completed_entries": entry_counts,
        "standalone_assets": standalone,
        "bootstrap": bootstrap_report,
        "deflated_sharpe_probability": dsr,
        "probability_of_backtest_overfitting": pbo_value,
        "multiplicity_aligned_interval_count": len(ordered_common),
        "regimes": regime_report,
        "portfolio_concentration": portfolio_concentration,
        "standalone_concentration": standalone_concentration,
        "source_partition_count": market.source_partition_count,
        "holdout_values_read": False,
        "holdout_opened": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
    }
