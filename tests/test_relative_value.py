from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.relative_value import (
    CASH,
    PARAMETER_NEIGHBORS,
    PRIMARY,
    TRIAL_ORDER,
    TRIALS,
    RelativeValueError,
    RotationDecision,
    Score,
    atomic_fills,
    bootstrap_seed,
    completed_holds,
    concentration,
    decide,
    deflated_sharpe,
    fold_prefix_indices,
    gate_checks,
    pbo,
    quarantine_action,
    regime_history,
    rotation_state_machine,
    score_at,
    self_financing_with_attribution,
)
from strategy_control.trend import DailyBar


def _days(count: int, *, broken: int | None = None, drift: float = 0.001) -> list[DailyBar]:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    prices = [100.0]
    for index in range(1, count):
        prices.append(prices[-1] * (1 + drift + (0.001 if index % 2 else -0.0005)))
    return [
        DailyBar(start + timedelta(days=i), start + timedelta(days=i + 1), p, p, p, p, i != broken)
        for i, p in enumerate(prices)
    ]


def test_scores_are_causal_complete_and_each_trial_is_declared() -> None:
    days = _days(200)
    assert score_at(days, 119) is None
    base = score_at(days, 120)
    assert base is not None
    # Mutating a future close cannot alter the cutoff score.
    future = list(days)
    row = future[150]
    future[150] = DailyBar(
        row.session, row.available_at, row.open, row.high, row.low, row.close * 9, True
    )
    assert score_at(future, 120) == base
    assert tuple(TRIALS) == TRIAL_ORDER and len(TRIAL_ORDER) == 7 and len(PARAMETER_NEIGHBORS) == 4
    assert score_at(days, 179, TRIALS["long_60_120_180_horizons"]) is None
    assert score_at(days, 181, TRIALS["long_60_120_180_horizons"]) is not None


def test_ties_cash_filter_and_every_override() -> None:
    up = Score((0.1, 0.1, 0.1), 1.0)
    down = Score((-0.1, -0.1, -0.1), 0.2)
    tie = Score((0.1, 0.1, 0.1), 1.0)
    assert decide(up, tie, CASH, PRIMARY) == CASH
    assert decide(up, tie, "ETHUSDT", PRIMARY) == "ETHUSDT"
    assert decide(down, up, CASH, PRIMARY) == "ETHUSDT"
    assert (
        decide(
            Score((-0.1,), 1.0),
            Score((-0.2,), 0.3),
            CASH,
            TRIALS["raw_60_session_relative_strength_rotation"],
        )
        == CASH
    )
    assert decide(tie, tie, CASH, TRIALS["always_in_higher_score_no_cash_filter"]) == "BTCUSDT"
    assert TRIALS["wide_0_50_rotation_gap"].gap == 0.5
    assert TRIALS["raw_unadjusted_20_60_120"].risk_adjusted is False
    assert TRIALS["short_10_30_60_horizons"].horizons == (10, 30, 60)


def test_pending_is_immutable_same_session_fill_can_queue_and_cash_gap_cancels() -> None:
    btc, eth = _days(180, drift=0.004), _days(180, drift=-0.003)
    rows = rotation_state_machine(btc, eth, recovery_sessions=150, execution_delay_sessions=1)
    queued = next(i for i, row in enumerate(rows) if row.pending is not None)
    assert rows[queued].actual == CASH
    assert rows[queued + 1].pending is not None  # countdown, no supersession
    assert rows[queued + 2].actual in {"BTCUSDT", "ETHUSDT"}
    broken_btc, broken_eth = (
        _days(180, broken=150, drift=0.004),
        _days(180, broken=150, drift=-0.003),
    )
    # A pending cash entry is cancelled and creates a new segment, never a fake return.
    rows = rotation_state_machine(
        broken_btc, broken_eth, recovery_sessions=150, execution_delay_sessions=1
    )
    assert rows[150].quarantined and rows[150].actual == CASH and rows[150].pending is None


def test_exposed_quarantine_and_fold_warmup_fail_closed() -> None:
    btc, eth = _days(180, broken=170, drift=0.004), _days(180, broken=170, drift=-0.003)
    with pytest.raises(RelativeValueError, match="exposed quarantine"):
        rotation_state_machine(btc, eth, recovery_sessions=150)
    days = _days(180)
    start = days[160].session
    rows = rotation_state_machine(
        days, _days(180, drift=-0.003), recovery_sessions=150, decision_start=start
    )
    assert all(row.actual == CASH and row.pending is None for row in rows[:160])
    assert quarantine_action(CASH, "BTCUSDT").cancel_pending
    assert quarantine_action("BTCUSDT", "ETHUSDT").requires_priced_liquidation
    sessions = [start + timedelta(days=i) for i in range(3)]
    assert fold_prefix_indices(sessions, start, start + timedelta(days=2)) == (0, 1)


def test_atomic_three_weight_accounting_turnover_cost_attribution_and_segment_boundary() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    fills = atomic_fills(
        [start + timedelta(days=i) for i in range(4)],
        [
            {"BTCUSDT": 100.0, "ETHUSDT": 100.0},
            {"BTCUSDT": 110.0, "ETHUSDT": 100.0},
            {"BTCUSDT": 110.0, "ETHUSDT": 110.0},
            {"BTCUSDT": 110.0, "ETHUSDT": 110.0},
        ],
        ["BTCUSDT", "ETHUSDT", CASH, CASH],
    )
    intervals = self_financing_with_attribution(fills)
    # The first observed interval includes the initial cash-to-BTC entry and the
    # atomic BTC-to-ETH rotation at its endpoint.
    assert intervals[0].turnover == pytest.approx(2.0)
    assert (
        intervals[0].cost_attribution["BTCUSDT"] > 0
        and intervals[0].cost_attribution["ETHUSDT"] > 0
    )
    assert intervals[-2].turnover == pytest.approx(1.0)
    with pytest.raises(RelativeValueError, match="bridge quarantine"):
        self_financing_with_attribution(fills, segments=[0, 0, 1, 1])

    flat = atomic_fills(
        [start, start + timedelta(days=1)],
        [{"BTCUSDT": 100.0, "ETHUSDT": 100.0}] * 2,
        ["BTCUSDT", CASH],
    )
    [flat_interval] = self_financing_with_attribution(flat)
    assert flat_interval.net_return == pytest.approx((1 - 0.0014) ** 2 - 1)
    assert flat_interval.turnover == pytest.approx(2.0)
    assert flat_interval.cost_attribution["BTCUSDT"] == pytest.approx(flat_interval.cost)

    segmented = atomic_fills(
        [start + timedelta(days=i) for i in range(4)],
        [{"BTCUSDT": 100.0, "ETHUSDT": 100.0}] * 4,
        ["BTCUSDT", CASH, "BTCUSDT", CASH],
    )
    segmented_intervals = self_financing_with_attribution(
        segmented, segments=[0, 0, 1, 1]
    )
    assert [item.turnover for item in segmented_intervals] == pytest.approx([2.0, 2.0])
    assert segmented_intervals[1].start == start + timedelta(days=2)


def test_completed_holds_regime_reset_current_exclusion_statistics_and_gates() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    fills = atomic_fills(
        [start + timedelta(days=i) for i in range(4)],
        [{"BTCUSDT": 1.0, "ETHUSDT": 1.0}] * 4,
        [CASH, "BTCUSDT", "ETHUSDT", CASH],
    )
    assert completed_holds(fills) == {"BTCUSDT": 1, "ETHUSDT": 1}
    days = _days(190, broken=170)
    labels = regime_history(days)
    assert labels[119] is None and labels[170] is None and all(x is None for x in labels[171:])
    assert deflated_sharpe([0.0, 0.0], [[0.0, 0.0]] * 7) == 0.0
    assert pbo([[0.0] * 7] * 7) == 1.0
    assert bootstrap_seed() == int.from_bytes(
        hashlib.sha256(b"btc-eth-relative-value-rotation-v1").digest()[:8], "big"
    )
    checks = gate_checks(
        {"aggregate_net_return_gt": 0.1, "fold_count": 4, "no_material_leakage": True},
        {
            "aggregate_net_return_gt": 0.0,
            "fold_count": 4,
            "no_material_leakage": True,
            "unknown": True,
        },
    )
    assert checks == {
        "aggregate_net_return_gt": True,
        "fold_count": True,
        "no_material_leakage": True,
        "unknown": False,
    }
    assert concentration([])["pass"] is False


def test_target_validation_and_immutable_decision() -> None:
    now = datetime(2025, 1, 1, tzinfo=UTC)
    with pytest.raises(RelativeValueError):
        atomic_fills([now], [{"BTCUSDT": 1.0, "ETHUSDT": float("nan")}], [CASH])
    decision = RotationDecision(CASH, CASH, None, False, 0, False)
    with pytest.raises(FrozenInstanceError):
        decision.actual = "BTCUSDT"  # type: ignore[misc]
