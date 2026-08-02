from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.trend import DailyBar, TrendError
from strategy_control.trend_pipeline import (
    DEVELOPMENT_END,
    DevelopmentMarket,
    _development_partitions,
    build_period_fills,
    evaluate_development,
)


def _market(count: int = 200) -> tuple[DevelopmentMarket, dict[str, list[float | None]]]:
    start = datetime(2024, 7, 1, tzinfo=UTC)
    days = [
        DailyBar(
            start + timedelta(days=index),
            start + timedelta(days=index + 1),
            100 + index,
            101 + index,
            99 + index,
            100 + index,
            True,
        )
        for index in range(count)
    ]
    causal = {
        day.session: (day.session + timedelta(days=1, minutes=1), day.open)
        for day in days
    }
    market = DevelopmentMarket(
        days={"BTCUSDT": days, "ETHUSDT": days},
        causal_fills={"BTCUSDT": causal, "ETHUSDT": causal},
        source_partition_count=36,
    )
    targets = {symbol: [0.25] * count for symbol in ("BTCUSDT", "ETHUSDT")}
    return market, targets


def test_development_partition_router_excludes_holdout() -> None:
    partitions = [
        {
            "relative_path": f"canonical/symbol=BTCUSDT/year=2025/month={month:02d}/x",
            "verification_scope": "HASH_AND_SCHEMA_METADATA_ONLY",
        }
        for month in range(1, 13)
    ] + [
        {
            "relative_path": f"canonical/symbol=ETHUSDT/year=2025/month={month:02d}/x",
            "verification_scope": "HASH_AND_SCHEMA_METADATA_ONLY",
        }
        for month in range(1, 13)
    ] + [
        {
            "relative_path": f"canonical/{index}",
            "verification_scope": "HASH_AND_SCHEMA_METADATA_ONLY",
        }
        for index in range(12)
    ] + [
        {
            "relative_path": f"canonical/symbol=BTCUSDT/year=2026/month={month:02d}/x",
            "verification_scope": "BYTE_HASH_ONLY_NO_PARQUET_PARSE",
        }
        for month in range(1, 7)
    ]
    selected = _development_partitions({"partitions": partitions})
    assert len(selected) == 36
    assert all("year=2026" not in path for path in selected)


def test_period_fills_force_terminal_cash_and_refuse_holdout_boundary() -> None:
    market, targets = _market()
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2025, 1, 20, tzinfo=UTC)
    fills = build_period_fills(market, targets, start, end)
    assert fills and fills[-1].targets == {"BTCUSDT": 0.0, "ETHUSDT": 0.0}
    assert all(start <= fill.timestamp < end for fill in fills)
    with pytest.raises(TrendError, match="boundary"):
        build_period_fills(market, targets, start, DEVELOPMENT_END + timedelta(days=1))


def test_quarantined_target_inside_active_period_fails_closed() -> None:
    market, targets = _market()
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2025, 1, 20, tzinfo=UTC)
    sessions = [day.session for day in market.days["BTCUSDT"]]
    inside = next(index for index, session in enumerate(sessions) if session == start)
    targets["BTCUSDT"][inside + 2] = None
    with pytest.raises(TrendError, match="quarantined"):
        build_period_fills(market, targets, start, end)


def test_full_development_evaluator_is_bounded_and_holdout_closed() -> None:
    class FakePCG:
        def __init__(self) -> None:
            self.state = 7

        def integers(self, high: int) -> int:
            self.state = (self.state * 6364136223846793005 + 1) % (2**64)
            return self.state % high

        def random(self) -> float:
            self.state = (self.state * 6364136223846793005 + 1) % (2**64)
            return self.state / 2**64

    market, _ = _market(549)
    gates = {
        "aggregate_net_return_gt": 0.0,
        "positive_folds_minimum": 3,
        "fold_count": 4,
        "annualized_sharpe_gte": 0.75,
        "maximum_drawdown_lte": 0.2,
        "doubled_cost_aggregate_net_return_gt": 0.0,
        "additional_delay_aggregate_net_return_gt": 0.0,
        "asset_standalone_net_return_each_gt": 0.0,
        "positive_parameter_neighbors_minimum": 3,
        "parameter_neighbor_count": 4,
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": 0.0,
        "deflated_sharpe_probability_gte": 0.95,
        "probability_of_backtest_overfitting_lte": 0.2,
        "baseline_superiority": "frozen_rule",
        "regime_gate": "pass",
        "exceptional_trade_gate": "pass",
        "no_material_leakage": True,
    }
    result = evaluate_development(
        market,
        {"development_gates_all_required": gates},
        bootstrap_rng=FakePCG(),
    )
    assert result["stage"] == "DEVELOPMENT"
    assert result["holdout_values_read"] is False
    assert result["holdout_opened"] is False
    assert result["source_partition_count"] == 36
    assert len(result["folds"]) == 4
