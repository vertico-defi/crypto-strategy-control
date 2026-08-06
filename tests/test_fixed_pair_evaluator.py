from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.fixed_pair_evaluator import (
    BoundaryRowIndex,
    ExactExecutionOracle,
    MissingExecutionRow,
    build_sessions,
)
from strategy_control.fixed_pair_evaluator.session import Row, SessionInvariantError


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
