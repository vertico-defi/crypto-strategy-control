from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.mean_reversion_v5_cleanroom import ASSETS, CleanRow, build_sessions
from strategy_control.relative_value_v5_cleanroom import (
    RelativeCleanRoomError,
    decide,
    evaluate_fixture,
)


def _sessions(count: int = 140):
    start = datetime(2025, 1, 1, tzinfo=UTC)
    timestamps = tuple(start + timedelta(days=i) for i in range(count))
    rows = []
    for asset, base in ((ASSETS[0], 100.0), (ASSETS[1], 200.0)):
        for i, timestamp in enumerate(timestamps):
            close = base * (1.0 + i * (0.001 if asset == ASSETS[0] else 0.0002))
            rows.append(CleanRow(asset, timestamp, close))
    return build_sessions(tuple(rows), timestamps)


def test_relative_fixture_is_causal_and_terminal_cash() -> None:
    result = evaluate_fixture(_sessions())
    assert result.terminal_cash is True
    assert all(fill.execution_timestamp > fill.target_timestamp for fill in result.fills)


def test_incomplete_synchronized_session_fails_closed_when_exposed() -> None:
    sessions = list(_sessions())
    sessions[130] = type(sessions[130])(sessions[130].timestamp, sessions[130].rows, False, True)
    with pytest.raises(RelativeCleanRoomError):
        evaluate_fixture(tuple(sessions))


def test_warmup_prevents_early_decision() -> None:
    assert decide(_sessions(20), 19, "CASH") is None
