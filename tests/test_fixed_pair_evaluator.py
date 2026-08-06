from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from strategy_control.fixed_pair_evaluator import (
    BoundaryRowIndex,
    CashLedger,
    ExactExecutionOracle,
    MissingExecutionRow,
    PortfolioRebalance,
    build_sessions,
    rebalance,
    terminal_liquidation,
)
from strategy_control.fixed_pair_evaluator.evidence import (
    StageMarker,
    require_independent_sources,
    validate_stage_sequence,
)
from strategy_control.fixed_pair_evaluator.loader import (
    DataIdentityError,
    DevelopmentManifest,
    HoldoutGuard,
)
from strategy_control.fixed_pair_evaluator.session import (
    RecoveryState,
    Row,
    SessionInvariantError,
    expected_gaps,
)


def t(minutes: int) -> datetime:
    return datetime(2025, 1, 1, tzinfo=UTC) + timedelta(minutes=minutes)


def rows() -> list[Row]:
    return [
        Row("BTCUSDT", t(0), 100),
        Row("BTCUSDT", t(1), 101),
        Row("ETHUSDT", t(0), 10),
        Row("ETHUSDT", t(1), 11),
    ]


def test_boundary_index_is_strict_and_exact() -> None:
    index = BoundaryRowIndex.build(rows(), boundary=t(1))
    assert len(index.rows) == 2
    assert index.exact("BTCUSDT", t(0)) is not None
    assert index.exact("BTCUSDT", t(1)) is None


def test_duplicate_and_nonmonotonic_rows_fail_closed() -> None:
    with pytest.raises(SessionInvariantError, match="duplicate"):
        BoundaryRowIndex.build([*rows(), Row("BTCUSDT", t(0), 100)], boundary=t(2))
    with pytest.raises(SessionInvariantError, match="nonmonotonic"):
        BoundaryRowIndex.build(
            [Row("BTCUSDT", t(1), 101), Row("BTCUSDT", t(0), 100)], boundary=t(2)
        )


def test_missing_execution_row_never_forward_scans() -> None:
    index = BoundaryRowIndex.build(rows(), boundary=t(2))
    with pytest.raises(MissingExecutionRow):
        ExactExecutionOracle(index).lookup("BTCUSDT", t(2))


def test_synchronization_and_quarantine_are_explicit() -> None:
    index = BoundaryRowIndex.build(rows(), boundary=t(2))
    sessions = build_sessions(
        index,
        timestamps=(t(0), t(1), t(2)),
        assets=("BTCUSDT", "ETHUSDT"),
        quarantine=frozenset({t(1)}),
    )
    assert [s.complete for s in sessions] == [True, True, False]
    assert [s.eligible for s in sessions] == [True, False, False]


def test_availability_boundary_and_immutable_rows() -> None:
    delayed = Row("BTCUSDT", t(0), 100, available=t(2))
    index = BoundaryRowIndex.build([delayed], boundary=t(2))
    assert index.exact("BTCUSDT", t(0)) is None
    with pytest.raises(TypeError):
        index.by_asset_time[("X", t(0))] = delayed  # type: ignore[index]


def test_turnover_rebalance_and_terminal_liquidation() -> None:
    ledger = CashLedger(1000.0, {})
    next_ledger = rebalance(
        ledger,
        PortfolioRebalance({"BTCUSDT": 100.0}, {"BTCUSDT": 0.5}, 14.0),
        equity=1000.0,
    )
    assert next_ledger.cash < 500.0
    terminal = terminal_liquidation(next_ledger, {"BTCUSDT": 110.0}, 14.0)
    assert terminal.units == {}
    assert terminal.cash > 0


def test_relative_value_half_l1_rotation_turnover_is_one() -> None:
    ledger = CashLedger(1000.0, {"BTCUSDT": 10.0})
    plan = PortfolioRebalance(
        {"BTCUSDT": 100.0, "ETHUSDT": 100.0},
        {"ETHUSDT": 1.0},
        14.0,
        "half_l1_including_cash",
    )
    result = rebalance(ledger, plan, equity=1000.0)
    assert result.cash == 0.0
    assert result.units["ETHUSDT"] < 10.0


def test_stage_order_and_holdout_guard_fail_closed() -> None:
    markers = tuple(StageMarker.complete(stage, {"stage": stage}) for stage in (
        "identity_verified", "representative_rows_materialized", "production_trace_emitted",
        "independent_reference_reconciled", "development_evaluator_complete"
    ))
    validate_stage_sequence(markers)
    manifest = DevelopmentManifest(Path("/tmp/data"), ("2025/file.parquet",), "commit", "hash")
    with pytest.raises(DataIdentityError):
        manifest.resolve_development("2026/file.parquet")
    with pytest.raises(DataIdentityError):
        HoldoutGuard().reject(Path("/tmp/2026/file.parquet"))
    require_independent_sources("production", "reference")
    with pytest.raises(ValueError):
        require_independent_sources("same", "same")


def test_gap_and_recovery_require_150_complete_sessions() -> None:
    assert expected_gaps((t(0), t(1), t(3))) == ((t(1), t(3)),)
    index = BoundaryRowIndex.build(rows(), boundary=t(2))
    sessions = build_sessions(index, timestamps=(t(0), t(1)), assets=("BTCUSDT", "ETHUSDT"))
    state = RecoveryState()
    for session in sessions:
        state = state.observe(session)
    assert not state.recovered
