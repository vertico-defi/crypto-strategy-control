from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.mean_reversion import MeanReversionConfig, MeanReversionError
from strategy_control.mean_reversion_pipeline import (
    DEVELOPMENT_END,
    DEVELOPMENT_START,
    build_period_fills,
    evaluate_development,
)
from strategy_control.trend import DailyBar
from strategy_control.trend_pipeline import DevelopmentMarket, _development_partitions


def _market(count: int = 549) -> DevelopmentMarket:
    start = datetime(2024, 7, 1, tzinfo=UTC)
    values = [100.0]
    for index in range(1, count):
        if index % 37 == 0:
            change = -0.08
        elif index % 37 == 1:
            change = 0.045
        else:
            change = 0.003 if index % 2 else -0.002
        values.append(values[-1] * (1.0 + change))
    days = [
        DailyBar(
            session=start + timedelta(days=index),
            available_at=start + timedelta(days=index + 1),
            open=value,
            high=value,
            low=value,
            close=value,
            complete=True,
        )
        for index, value in enumerate(values)
    ]
    causal = {
        day.session: (day.session + timedelta(days=1, minutes=1), day.open) for day in days
    }
    return DevelopmentMarket(
        days={"BTCUSDT": days, "ETHUSDT": days},
        causal_fills={"BTCUSDT": dict(causal), "ETHUSDT": dict(causal)},
        source_partition_count=36,
    )


def _gates() -> dict[str, object]:
    return {
        "aggregate_net_return_gt": 0.0,
        "annualized_sharpe_gte": 0.75,
        "positive_folds_minimum": 3,
        "fold_count": 4,
        "maximum_drawdown_lte": 0.2,
        "doubled_cost_aggregate_net_return_gt": 0.0,
        "additional_delay_aggregate_net_return_gt": 0.0,
        "positive_parameter_neighbors_minimum": 3,
        "parameter_neighbor_count": 4,
        "asset_standalone_net_return_each_gt": 0.0,
        "completed_entries_total_minimum": 24,
        "completed_entries_each_asset_minimum": 10,
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": 0.0,
        "deflated_sharpe_probability_gte": 0.95,
        "probability_of_backtest_overfitting_lte": 0.2,
        "regime_gate": "pass",
        "exceptional_trade_gate": "pass",
        "baseline_superiority": "frozen comparison",
        "no_material_leakage": True,
    }


def test_shared_partition_router_never_selects_a_2026_holdout() -> None:
    partitions = [
        {
            "relative_path": f"canonical/symbol={symbol}/year=2025/month={month:02d}/x",
            "verification_scope": "HASH_AND_SCHEMA_METADATA_ONLY",
        }
        for symbol in ("BTCUSDT", "ETHUSDT")
        for month in range(1, 13)
    ] + [
        {
            "relative_path": f"canonical/development-extra-{index}",
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


def test_period_starts_from_cash_and_liquidates_before_half_open_end() -> None:
    market = _market()
    end = DEVELOPMENT_START + timedelta(days=20)
    fills = build_period_fills(market, MeanReversionConfig(entry_z=-999), DEVELOPMENT_START, end)
    assert fills[0].targets == {"BTCUSDT": 0.0, "ETHUSDT": 0.0}
    assert fills[-1].targets == {"BTCUSDT": 0.0, "ETHUSDT": 0.0}
    assert all(DEVELOPMENT_START <= fill.timestamp < end for fill in fills)


def test_missing_synchronized_fill_with_risky_exposure_fails_closed() -> None:
    market = _market()
    missing_session = DEVELOPMENT_START + timedelta(days=5)
    mutable = dict(market.causal_fills["ETHUSDT"])
    mutable.pop(missing_session)
    broken = DevelopmentMarket(
        days=market.days,
        causal_fills={"BTCUSDT": market.causal_fills["BTCUSDT"], "ETHUSDT": mutable},
        source_partition_count=36,
    )
    with pytest.raises(MeanReversionError, match="quarantine"):
        build_period_fills(
            broken,
            MeanReversionConfig(entry_z=999, exit_z=1000),
            DEVELOPMENT_START,
            DEVELOPMENT_START + timedelta(days=20),
        )


def test_development_boundary_refuses_any_2026_period() -> None:
    with pytest.raises(MeanReversionError, match="boundary"):
        build_period_fills(
            _market(),
            MeanReversionConfig(),
            DEVELOPMENT_START,
            DEVELOPMENT_END + timedelta(days=1),
        )


def test_full_development_report_is_deterministic_and_holdout_closed() -> None:
    class FakePCG:
        def __init__(self) -> None:
            self.state = 11

        def integers(self, high: int) -> int:
            self.state = (self.state * 6364136223846793005 + 1) % (2**64)
            return self.state % high

        def random(self) -> float:
            self.state = (self.state * 6364136223846793005 + 1) % (2**64)
            return self.state / 2**64

    result = evaluate_development(
        _market(), {"development_gates_all_required": _gates()}, bootstrap_rng=FakePCG()
    )
    assert result["stage"] == "DEVELOPMENT"
    assert result["classification"] in {"DEVELOPMENT_GO", "HISTORICAL_NO_GO"}
    assert result["holdout_values_read"] is False
    assert result["holdout_opened"] is False
    assert result["candidate_promoted"] is False
    assert result["capital_permitted"] == 0
    assert len(result["folds"]) == 4
    assert len(result["variants"]) == 7
    assert result["multiplicity_aligned_interval_count"] >= 8
