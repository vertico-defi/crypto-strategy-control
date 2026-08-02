from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.mean_reversion import (
    PARAMETER_NEIGHBORS,
    PRIMARY,
    TRIAL_ORDER,
    VARIANTS,
    AssetDecision,
    MeanReversionConfig,
    MeanReversionError,
    asset_state_machine,
    atomic_portfolio_fills,
    bootstrap_seed,
    completed_entries,
    deflated_sharpe,
    forced_terminal_cash,
    pbo,
    standardized_shocks,
)
from strategy_control.trend import DailyBar


def _days(count: int, *, broken: int | None = None) -> list[DailyBar]:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    # Nonconstant returns provide finite sample volatility after recovery.
    values = [100.0]
    for index in range(1, count):
        values.append(values[-1] * (1.0 + (0.003 if index % 2 else -0.002)))
    return [
        DailyBar(
            start + timedelta(days=index),
            start + timedelta(days=index + 1),
            value,
            value,
            value,
            value,
            index != broken,
        )
        for index, value in enumerate(values)
    ]


def test_standardized_shock_needs_complete_nonzero_volatility_window() -> None:
    days = _days(30)
    assert standardized_shocks(days)[19] is None
    assert standardized_shocks(days)[20] is not None
    days[15] = DailyBar(days[15].session, days[15].available_at, 1, 1, 1, 1, False)
    assert standardized_shocks(days)[20] is None


def test_quarantine_fails_closed_when_risky_or_pending() -> None:
    days = _days(160, broken=155)
    # A forced tiny threshold creates an entry before the broken session.
    with pytest.raises(MeanReversionError, match="quarantine"):
        asset_state_machine(
            days, MeanReversionConfig(entry_z=999, exit_z=1000), recovery_sessions=20
        )


def test_quarantine_cancels_unfilled_cash_entry_and_date_gap_resets_recovery() -> None:
    days = _days(40, broken=21)
    rows = asset_state_machine(
        days,
        MeanReversionConfig(entry_z=999, exit_z=1000),
        execution_delay_sessions=1,
        recovery_sessions=20,
    )
    assert rows[20].pending and not rows[20].actual_long
    assert not rows[21].pending and not rows[21].actual_long
    gap_days = _days(40)
    shifted = gap_days[22]
    gap_days[22] = DailyBar(
        shifted.session + timedelta(days=1),
        shifted.available_at + timedelta(days=1),
        shifted.open,
        shifted.high,
        shifted.low,
        shifted.close,
        True,
    )
    rows = asset_state_machine(gap_days, recovery_sessions=20)
    assert rows[22].valid_input is False


def test_recovery_preserves_cash_and_rebuilds_inputs() -> None:
    days = _days(60, broken=20)
    result = asset_state_machine(days, recovery_sessions=10)
    assert all(not row.actual_long and not row.pending for row in result[20:30])
    assert result[30].valid_input is False  # volatility window includes only post-gap data


def test_fold_decision_start_uses_warmup_but_never_carries_pre_fold_state() -> None:
    days = _days(220)
    start = days[180].session
    rows = asset_state_machine(
        days,
        MeanReversionConfig(entry_z=999, exit_z=1000),
        recovery_sessions=150,
        decision_start=start,
    )
    assert all(not row.actual_long and not row.pending for row in rows[:180])
    assert rows[180].pending and not rows[180].actual_long


def test_pending_order_blocks_supersession_and_actual_state_changes_only_on_execution() -> None:
    days = _days(50)
    rows = asset_state_machine(
        days,
        MeanReversionConfig(entry_z=999, exit_z=1000),
        execution_delay_sessions=1,
        recovery_sessions=20,
    )
    first = next(index for index, row in enumerate(rows) if row.pending)
    assert rows[first].actual_long is False
    assert rows[first + 1].actual_long is False
    assert rows[first + 2].actual_long is True


def test_holding_clock_schedules_exit_after_exact_exposed_intervals() -> None:
    days = _days(50)
    config = MeanReversionConfig(entry_z=999, exit_z=1000, maximum_holding_intervals=3)
    rows = asset_state_machine(days, config, recovery_sessions=20)
    entered = next(i for i, row in enumerate(rows) if row.actual_long)
    # Cash is queued early enough to execute at fill j+3, after three intervals.
    assert rows[entered + 2].desired_long is False
    assert rows[entered + 3].actual_long is False


def test_delayed_holding_clock_still_exits_after_exact_five_intervals() -> None:
    rows = asset_state_machine(
        _days(60),
        MeanReversionConfig(entry_z=999, exit_z=1000, maximum_holding_intervals=5),
        execution_delay_sessions=1,
        recovery_sessions=20,
    )
    entered = next(i for i, row in enumerate(rows) if row.actual_long)
    assert all(rows[index].actual_long for index in range(entered, entered + 5))
    assert rows[entered + 5].actual_long is False


def test_atomic_accounting_rejects_invalid_weights_or_asynchronous_assets_and_terminal_cash(
) -> None:
    time = datetime(2025, 1, 1, tzinfo=UTC)
    with pytest.raises(MeanReversionError, match="exactly zero or one half"):
        atomic_portfolio_fills(
            [time],
            [{"BTCUSDT": 1.0, "ETHUSDT": 1.0}],
            [{"BTCUSDT": 1.0, "ETHUSDT": 0.5}],
        )
    with pytest.raises(MeanReversionError, match="non-synchronised"):
        atomic_portfolio_fills(
            [time],
            [{"BTCUSDT": 1.0, "ETHUSDT": 1.0}],
            [{"BTCUSDT": 0.5, "DOGEUSDT": 0.0}],
        )
    fills = atomic_portfolio_fills(
        [time],
        [{"BTCUSDT": 1.0, "ETHUSDT": 1.0}],
        [{"BTCUSDT": 0.5, "ETHUSDT": 0.5}],
    )
    assert forced_terminal_cash(fills)[-1].targets == {"BTCUSDT": 0.0, "ETHUSDT": 0.0}


def test_completed_entries_requires_a_costed_return_to_cash() -> None:
    time = datetime(2025, 1, 1, tzinfo=UTC)
    fills = atomic_portfolio_fills(
        [time + timedelta(days=i) for i in range(3)],
        [{"BTCUSDT": 1.0, "ETHUSDT": 1.0}] * 3,
        [
            {"BTCUSDT": 0.0, "ETHUSDT": 0.0},
            {"BTCUSDT": 0.5, "ETHUSDT": 0.0},
            {"BTCUSDT": 0.0, "ETHUSDT": 0.0},
        ],
    )
    assert completed_entries(fills) == {"BTCUSDT": 1, "ETHUSDT": 0}


def test_declared_variant_mapping_and_statistical_degeneracies_fail_closed() -> None:
    assert tuple(VARIANTS) == TRIAL_ORDER
    assert len(TRIAL_ORDER) == 7
    assert len(PARAMETER_NEIGHBORS) == 4
    assert VARIANTS[TRIAL_ORDER[0]] == PRIMARY
    assert pbo([[0.0] * 7] * 7) == 1.0
    assert deflated_sharpe([0.0] * 3, [[0.0] * 3] * 7) == 0.0
    expected_seed = int.from_bytes(
        hashlib.sha256(b"btc-eth-long-only-mean-reversion-v1").digest()[:8], "big"
    )
    assert bootstrap_seed() == expected_seed


def test_asset_decision_is_immutable() -> None:
    row = AssetDecision(False, False, False, None, False)
    with pytest.raises(FrozenInstanceError):
        row.actual_long = True  # type: ignore[misc]
