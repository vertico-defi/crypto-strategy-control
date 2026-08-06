from datetime import UTC, datetime, timedelta

from strategy_control.mean_reversion_v5_cleanroom import ASSETS, CleanRow, build_sessions, evaluate
from strategy_control.mean_reversion_v5_reference import calculate


def _fixture_rows() -> tuple[tuple[CleanRow, ...], tuple[datetime, ...]]:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    timestamps = tuple(start + timedelta(minutes=i) for i in range(30))
    rows = []
    for asset, base in ((ASSETS[0], 100.0), (ASSETS[1], 200.0)):
        for i, timestamp in enumerate(timestamps):
            close = base * (1.0 - (0.02 if 18 <= i <= 21 else 0.0) + i * 0.0001)
            rows.append(CleanRow(asset, timestamp, close))
    return tuple(rows), timestamps


def test_reference_reconciles_fixture() -> None:
    rows, timestamps = _fixture_rows()
    sessions = build_sessions(rows, timestamps)
    production = evaluate(sessions)
    reference = calculate(sessions)
    assert production.terminal_equity == reference.terminal_equity
    assert production.net_return == reference.net_return
    assert production.costs == reference.costs
    assert len(production.decisions) == len(reference.decisions)
    assert len(production.fills) == len(reference.fills)
    assert production.interval_returns == reference.interval_returns
