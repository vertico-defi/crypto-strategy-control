import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from strategy_control.calendar_pipeline import (
    DEVELOPMENT_END,
    CalendarPipelineError,
    MinuteRecord,
    TrialRun,
    causal_interval_valid,
    development_partitions,
    evaluate_development,
    exact_hour_vector,
    fifth_valid_vector,
    fold_source_prefix,
    open_development_partition,
)
from strategy_control.calendar_seasonality import TRIALS, CalendarIntegrityError


def stamp(hour: int, minute: int = 0) -> datetime:
    return datetime(2025, 1, 1, hour, minute, tzinfo=UTC)


def row(symbol: str, when: datetime, *, available_delay: int = 0) -> MinuteRecord:
    event = when + timedelta(minutes=1)
    return MinuteRecord(
        symbol,
        when,
        event,
        event + timedelta(minutes=available_delay),
        10,
        11,
        9,
        10,
    )


def contract() -> dict[str, object]:
    return json.loads(
        Path("experiments/btc-eth-vol-targeted-trend-v1/DATA_CONTRACT.json").read_text()
    )


def test_loader_rejects_2026_before_any_opener_call() -> None:
    opened: list[str] = []
    with pytest.raises(CalendarPipelineError):
        open_development_partition("x/year=2026/x.parquet", opened.append)
    assert opened == []
    assert len(development_partitions(contract())) == 36


def test_unequal_availability_and_missing_hour_are_invalid() -> None:
    records = [
        row("BTCUSDT", stamp(1)),
        row("ETHUSDT", stamp(1), available_delay=1),
        row("BTCUSDT", stamp(2)),
        row("ETHUSDT", stamp(2)),
    ]
    # Availability can differ, but both components remain explicit and no later
    # boundary can stand in for a missing exact-hour observation.
    assert exact_hour_vector(records, stamp(1)).timestamp == stamp(1)
    assert not causal_interval_valid(records, stamp(1), stamp(1, 1))
    with pytest.raises(CalendarIntegrityError):
        exact_hour_vector(records, stamp(4))


def test_fifth_event_delay_and_timeout() -> None:
    records = [
        row(asset, stamp(3, minute)) for minute in range(1, 6) for asset in ("BTCUSDT", "ETHUSDT")
    ]
    assert fifth_valid_vector(records, stamp(3)).timestamp == stamp(3, 5)
    with pytest.raises(CalendarPipelineError):
        fifth_valid_vector(records[:-2], stamp(3))


def test_development_end_and_fold_prefix_are_half_open() -> None:
    before = row("BTCUSDT", DEVELOPMENT_END - timedelta(minutes=2))
    boundary_available = row("BTCUSDT", DEVELOPMENT_END - timedelta(minutes=1))
    at_end = row("BTCUSDT", DEVELOPMENT_END)
    assert fold_source_prefix((before, boundary_available, at_end), DEVELOPMENT_END) == (before,)


def test_frozen_trial_order_and_fail_closed_missing_statistics() -> None:
    calls: list[str] = []

    def evaluator(trial, records, start, end):  # type: ignore[no-untyped-def]
        calls.append(trial.name)
        return TrialRun(trial.name, {stamp(0): 0.0}, {})

    result = evaluate_development((), evaluator)
    assert calls == [trial.name for trial in TRIALS]
    assert result.status == "HISTORICAL_NO_GO"
    assert result.performance_claim_permitted is False
    assert "missing immutable 28-record DSR prior registry" in result.failures
    assert "missing frozen gate evaluator" in result.failures
