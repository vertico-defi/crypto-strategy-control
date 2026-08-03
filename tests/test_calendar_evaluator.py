import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from strategy_control.calendar_evaluator import (
    ASSETS,
    CalendarEvaluationError,
    FrozenSchedule,
    build_market,
    execute_trial,
    prior_daily_sharpes,
)
from strategy_control.calendar_pipeline import MinuteRecord
from strategy_control.calendar_seasonality import (
    TRIALS,
    cscv_pbo,
    deflated_sharpe_probability,
    schedule_for_interval,
)


def minute_row(asset: str, stamp: datetime, price: float) -> MinuteRecord:
    event = stamp + timedelta(minutes=1)
    return MinuteRecord(asset, stamp, event, event, price, price, price, price)


def complete_minutes(start: datetime, end: datetime) -> tuple[MinuteRecord, ...]:
    rows: list[MinuteRecord] = []
    stamp = start
    count = 0
    while stamp < end:
        for index, asset in enumerate(ASSETS):
            rows.append(minute_row(asset, stamp, 100.0 + count * 0.001 + index))
        stamp += timedelta(minutes=1)
        count += 1
    return tuple(rows)


def active_schedules(start: datetime, end: datetime) -> dict[datetime, FrozenSchedule]:
    result: dict[datetime, FrozenSchedule] = {}
    hour = start
    while hour < end:
        refresh = schedule_for_interval(hour)
        result[refresh] = FrozenSchedule(
            refresh,
            TRIALS[0].name,
            {asset: (True,) * 48 for asset in ASSETS},
        )
        hour += timedelta(hours=1)
    return result


def test_build_market_rejects_invalid_and_duplicate_rows() -> None:
    stamp = datetime(2025, 1, 1, tzinfo=UTC)
    valid = minute_row(ASSETS[0], stamp, 100.0)
    with pytest.raises(CalendarEvaluationError, match="duplicate"):
        build_market((valid, valid))
    invalid = MinuteRecord(ASSETS[1], stamp, stamp, stamp, 1.0, 1.0, 1.0, 1.0)
    with pytest.raises(CalendarEvaluationError, match="invalid"):
        build_market((invalid,))


def test_base_and_delayed_paths_are_atomic_and_terminally_cash() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=2)
    market = build_market(complete_minutes(start, end), source_partition_count=36)
    schedules = active_schedules(start, end)
    base = execute_trial(
        market,
        TRIALS[0],
        start,
        end,
        schedules=schedules,
        labels={},
    )
    delayed = execute_trial(
        market,
        TRIALS[0],
        start,
        end,
        schedules=schedules,
        delay_minutes=5,
        labels={},
    )
    assert base.final_cash and delayed.final_cash
    assert base.counters.entries == base.counters.episodes == 1
    assert delayed.counters.entries == delayed.counters.episodes == 1
    assert len(base.daily_returns) == len(delayed.daily_returns) == 2
    assert base.asset_net[0] + base.asset_net[1] == pytest.approx(base.net_return)


def test_missing_boundary_liquidates_exposure_and_never_restarts_wealth() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=5)
    records = list(complete_minutes(start, end))
    missing = start + timedelta(hours=2)
    records = [row for row in records if row.open_timestamp != missing]
    market = build_market(tuple(records))
    run = execute_trial(
        market,
        TRIALS[0],
        start,
        end,
        schedules=active_schedules(start, end),
        labels={},
    )
    assert run.quarantine_liquidations == 1
    assert run.final_cash
    assert run.counters.entries == run.counters.episodes == 1


def test_prior_dsr_registry_is_hash_bound_and_frequency_compatible() -> None:
    values = prior_daily_sharpes(Path("experiments"))
    assert len(values) == 21
    assert all(math.isfinite(value) for value in values)


def test_frozen_dsr_and_pbo_degeneracies_fail_closed() -> None:
    values = [math.sin(index) * 0.01 + 0.001 for index in range(365)]
    records = [index * 0.001 for index in range(28)]
    probability = deflated_sharpe_probability(values, records)
    assert 0.0 <= probability <= 1.0
    assert deflated_sharpe_probability([0.0] * 365, records) == 0.0
    assert cscv_pbo([[0.0] * 16] * 7) == 1.0
