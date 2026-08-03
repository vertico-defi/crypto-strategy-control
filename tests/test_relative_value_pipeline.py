from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.relative_value import CASH, PRIMARY, RelativeValueError
from strategy_control.relative_value_pipeline import (
    DEVELOPMENT_END,
    DEVELOPMENT_START,
    _account_run,
    _benchmark_calendar_run,
    _segmented_buy_and_hold,
    build_period_run,
    evaluate_development,
)
from strategy_control.trend import DailyBar
from strategy_control.trend_pipeline import DevelopmentMarket


def _market(count: int = 731) -> DevelopmentMarket:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    btc_values = [100.0]
    eth_values = [100.0]
    for index in range(1, count):
        phase = (index // 45) % 2
        btc_change = 0.004 if phase == 0 else -0.002
        eth_change = -0.002 if phase == 0 else 0.004
        btc_values.append(btc_values[-1] * (1 + btc_change))
        eth_values.append(eth_values[-1] * (1 + eth_change))

    def rows(values: list[float]) -> list[DailyBar]:
        return [
            DailyBar(
                start + timedelta(days=index),
                start + timedelta(days=index + 1),
                value,
                value,
                value,
                value,
                True,
            )
            for index, value in enumerate(values)
        ]

    days = {"BTCUSDT": rows(btc_values), "ETHUSDT": rows(eth_values)}
    fills = {
        symbol: {
            day.session: (
                day.session + timedelta(days=1, minutes=1),
                float(day.open),
            )
            for day in symbol_days
        }
        for symbol, symbol_days in days.items()
    }
    return DevelopmentMarket(days=days, causal_fills=fills, source_partition_count=36)


def _replace_day(
    market: DevelopmentMarket, symbol: str, session: datetime, *, complete: bool
) -> DevelopmentMarket:
    days = {name: list(values) for name, values in market.days.items()}
    index = next(i for i, day in enumerate(days[symbol]) if day.session == session)
    day = days[symbol][index]
    days[symbol][index] = DailyBar(
        day.session, day.available_at, day.open, day.high, day.low, day.close, complete
    )
    return DevelopmentMarket(
        days=days,
        causal_fills=market.causal_fills,
        source_partition_count=market.source_partition_count,
    )


def _gates() -> dict[str, object]:
    return {
        "aggregate_net_return_gt": 0.0,
        "annualized_sharpe_gte": 0.75,
        "maximum_drawdown_lte": 0.2,
        "fold_count": 4,
        "positive_folds_minimum": 3,
        "doubled_cost_aggregate_net_return_gt": 0.0,
        "additional_delay_aggregate_net_return_gt": 0.0,
        "positive_parameter_neighbors_minimum": 3,
        "parameter_neighbor_count": 4,
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": 0.0,
        "deflated_sharpe_probability_gte": 0.95,
        "probability_of_backtest_overfitting_lte": 0.2,
        "baseline_superiority": "frozen comparison",
        "completed_entries_total_minimum": 24,
        "completed_holds_each_asset_minimum": 8,
        "asset_net_contribution_each_gt": 0.0,
        "exceptional_profit_gate": "pass",
        "regime_gate": "pass",
        "no_material_leakage": True,
    }


def test_base_and_delayed_clocks_start_from_cash_and_terminally_liquidate() -> None:
    market = _market()
    base = build_period_run(market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END)
    delayed = build_period_run(
        market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END, execution_delay_sessions=1
    )
    first_base = next(fill.timestamp for fill in base.fills if any(fill.targets.values()))
    first_delayed = next(fill.timestamp for fill in delayed.fills if any(fill.targets.values()))
    assert first_delayed == first_base + timedelta(days=1)
    assert base.fills[0].targets == {"BTCUSDT": 0.0, "ETHUSDT": 0.0}
    assert base.fills[-1].targets == {"BTCUSDT": 0.0, "ETHUSDT": 0.0}
    assert all(DEVELOPMENT_START <= fill.timestamp < DEVELOPMENT_END for fill in base.fills)


def test_fold_prefix_isolation_ignores_a_post_end_gap() -> None:
    market = _market()
    end = datetime(2025, 4, 1, tzinfo=UTC)
    original = build_period_run(market, PRIMARY, DEVELOPMENT_START, end)
    changed = _replace_day(
        market,
        "ETHUSDT",
        datetime(2025, 4, 2, tzinfo=UTC),
        complete=False,
    )
    isolated = build_period_run(changed, PRIMARY, DEVELOPMENT_START, end)
    assert isolated == original
    assert _account_run(isolated, 14.0) == _account_run(original, 14.0)


def test_exposed_gap_retains_priced_liquidation_and_never_bridges_recovery() -> None:
    market = _market()
    gap_session = datetime(2025, 1, 3, tzinfo=UTC)
    broken = _replace_day(market, "ETHUSDT", gap_session, complete=False)
    run = build_period_run(broken, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END)
    intervals = _account_run(run, 14.0)
    quarantined = [item for item in intervals if item.quarantine_liquidation]
    assert len(quarantined) == 1
    assert quarantined[0].end == gap_session + timedelta(days=1, minutes=1)
    assert quarantined[0].net_return != 0.0
    assert run.fills[-1].targets == {"BTCUSDT": 0.0, "ETHUSDT": 0.0}
    assert all(
        intervals[index].segment == intervals[index - 1].segment
        or intervals[index].start > intervals[index - 1].end
        for index in range(1, len(intervals))
    )
    buy_hold_run = _benchmark_calendar_run(
        broken, DEVELOPMENT_START, DEVELOPMENT_END, exposed_when_active=True
    )
    buy_hold = _segmented_buy_and_hold(
        buy_hold_run, {"BTCUSDT": 1.0, "ETHUSDT": 0.0}
    )
    assert sum(item.quarantine_liquidation for item in buy_hold) == 1
    cash_run = _benchmark_calendar_run(
        broken, DEVELOPMENT_START, DEVELOPMENT_END, exposed_when_active=False
    )
    assert not any(cash_run.quarantine_liquidations)


def test_unpriceable_exposed_gap_is_data_integrity_failure() -> None:
    market = _market()
    clean = build_period_run(market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END)
    entry_index = next(
        index for index, fill in enumerate(clean.fills) if any(fill.targets.values())
    )
    entry_session = clean.signal_sessions[entry_index]
    mutable_fills = {
        symbol: dict(entries) for symbol, entries in market.causal_fills.items()
    }
    for symbol in mutable_fills:
        mutable_fills[symbol] = {
            session: value
            for session, value in mutable_fills[symbol].items()
            if session <= entry_session
        }
    broken = DevelopmentMarket(
        days=market.days,
        causal_fills=mutable_fills,
        source_partition_count=36,
    )
    with pytest.raises(RelativeValueError, match="DATA_INTEGRITY_FAILURE"):
        build_period_run(broken, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END)


def test_development_boundary_and_report_remain_holdout_closed_and_deterministic() -> None:
    class FakePCG:
        def __init__(self) -> None:
            self.state = 17

        def integers(self, high: int) -> int:
            self.state = (self.state * 6364136223846793005 + 1) % (2**64)
            return self.state % high

        def random(self) -> float:
            self.state = (self.state * 6364136223846793005 + 1) % (2**64)
            return self.state / 2**64

    with pytest.raises(RelativeValueError, match="development"):
        build_period_run(
            _market(), PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END + timedelta(days=1)
        )
    result = evaluate_development(
        _market(), {"development_gates_all_required": _gates()}, bootstrap_rng=FakePCG()
    )
    assert result["classification"] in {"DEVELOPMENT_GO", "HISTORICAL_NO_GO"}
    assert result["holdout_values_read"] is False
    assert result["holdout_opened"] is False
    assert result["candidate_promoted"] is False
    assert result["capital_permitted"] == 0
    assert len(result["folds"]) == 4
    assert len(result["variants"]) == 7
    assert result["multiplicity_aligned_interval_count"] >= 8
    assert CASH == "CASH"
