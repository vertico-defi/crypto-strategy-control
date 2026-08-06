from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.mean_reversion_v5_cleanroom import (
    ASSETS,
    CleanRoomInvariantError,
    CleanRow,
    build_sessions,
    evaluate,
)


def _fixture_rows() -> tuple[tuple[CleanRow, ...], tuple[datetime, ...]]:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    timestamps = tuple(start + timedelta(minutes=i) for i in range(30))
    rows = []
    for asset, base in ((ASSETS[0], 100.0), (ASSETS[1], 200.0)):
        for i, timestamp in enumerate(timestamps):
            # A deterministic valid panel with a bounded dip and recovery.
            close = base * (1.0 - (0.02 if 18 <= i <= 21 else 0.0) + i * 0.0001)
            rows.append(CleanRow(asset, timestamp, close))
    return tuple(rows), timestamps


def test_complete_fixture_evaluates_to_cash() -> None:
    rows, timestamps = _fixture_rows()
    result = evaluate(build_sessions(rows, timestamps))
    assert result.terminal_cash is True
    assert result.terminal_equity == pytest.approx(1.017716465564334)
    assert result.net_return == pytest.approx(0.01771646556433404)
    assert result.costs == pytest.approx(0.002825810492524234)
    assert len(result.decisions) == 60
    assert len(result.fills) == 4
    assert len(result.interval_returns) == 29
    assert all(fill.execution_timestamp > fill.target_timestamp for fill in result.fills)


def test_duplicate_rows_fail_closed() -> None:
    rows, timestamps = _fixture_rows()
    with pytest.raises(CleanRoomInvariantError, match="duplicate"):
        build_sessions((*rows, rows[0]), timestamps)


def test_missing_next_session_execution_fails_closed() -> None:
    rows, timestamps = _fixture_rows()
    incomplete = tuple(
        row for row in rows if not (row.asset == ASSETS[1] and row.timestamp == timestamps[22])
    )
    with pytest.raises(CleanRoomInvariantError):
        evaluate(build_sessions(incomplete, timestamps))


def test_incomplete_session_is_quarantined() -> None:
    rows, timestamps = _fixture_rows()
    missing = {timestamps[10]}
    retained = tuple(row for row in rows if row.timestamp not in missing)
    sessions = build_sessions(retained, timestamps)
    assert sessions[10].complete is False
    assert sessions[10].quarantine is True
