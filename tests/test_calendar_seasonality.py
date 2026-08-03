from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.calendar_seasonality import (
    ASSETS,
    TRIALS,
    CalendarIntegrityError,
    CellEstimate,
    Counters,
    H,
    JointVector,
    Observation,
    PendingTarget,
    Portfolio,
    Quarantine,
    bootstrap_seed,
    bucket_at,
    deadline_for,
    decision_is_timely,
    deduplicate_prospective,
    dsr_degenerate_probability,
    estimate_cell,
    exact_joint_vector,
    fold_prefix,
    holm_active,
    hypothesis_order,
    joint_targets,
    pbo_degenerate,
    rebalance,
    schedule_for_interval,
    terminal_target_time,
    trim_observations,
)


def dt(day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2025, 1, day, hour, minute, tzinfo=UTC)


def obs(value: float, day: int) -> Observation:
    point = dt(day, 1)
    return Observation(value, point, point.replace(hour=0))


def test_bucket_order_hypotheses_and_monday_clocks() -> None:
    monday = dt(6)
    assert bucket_at(monday) == 0 and bucket_at(dt(4, 23)) == 47
    assert len(hypothesis_order()) == 96 and hypothesis_order()[0] == (ASSETS[0], 0)
    assert schedule_for_interval(monday) == datetime(2024, 12, 30, tzinfo=UTC)
    assert schedule_for_interval(monday + timedelta(hours=1)) == monday
    assert deadline_for(monday) == dt(5, 23, 59)
    assert decision_is_timely(monday, dt(5, 23, 59))
    assert not decision_is_timely(monday, monday)


def test_trials_are_complete_and_immutable_shape() -> None:
    assert len(TRIALS) == 7
    assert [x.lookback_weeks for x in TRIALS] == [26, 26, 13, 39, 26, 26, 26]
    assert TRIALS[1].holm_alpha is None and TRIALS[1].buckets == 24


def test_trim_ties_and_cluster_fail_closed() -> None:
    values = (obs(-1, 1), obs(-1, 2), obs(0, 3), obs(1, 4), obs(1, 5)) * 4
    # Duplicate endpoints are integrity failures, never arbitrary tie breaking.
    with pytest.raises(CalendarIntegrityError):
        trim_observations(values, 0.05)
    rows = tuple(Observation(v, dt(i + 1, 1), dt(i + 1)) for i, v in enumerate(range(20)))
    kept = trim_observations(rows, 0.05)
    assert kept[0].value == 1 and kept[-1].value == 18
    estimate = estimate_cell(
        rows, trim_fraction=0, minimum=20, minimum_weeks=21, cdf=lambda x, d: 0.5
    )
    assert estimate.p_value == 1
    assert estimate_cell(rows, trim_fraction=0, minimum=20, minimum_weeks=20, cdf=None).p_value == 1


def test_cr1_and_holm_ties_include_ineligible_cells() -> None:
    rows = tuple(obs(0.01 + i * 0.001, i + 1) for i in range(20))
    estimated = estimate_cell(
        rows, trim_fraction=0, minimum=20, minimum_weeks=20, cdf=lambda x, d: 0.9
    )
    assert (
        estimated.eligible and estimated.standard_error is not None and estimated.standard_error > 0
    )
    cells = [CellEstimate(H + 0.1, 1.0, 0.0001, (), True) for _ in range(2)]
    cells += [CellEstimate(None, None, 1.0, (), False) for _ in range(94)]
    result = holm_active(cells, 0.05)
    assert result[:2] == (True, True) and not any(result[2:])
    tied = [CellEstimate(H + 0.1, 1.0, 0.00053, (), True) for _ in range(95)]
    tied.insert(0, CellEstimate(H + 0.1, 1.0, 0.0001, (), True))
    # The tied second rank fails .05/95; all later cells must remain rejected.
    assert holm_active(tied, 0.05) == (True,) + (False,) * 95


def test_joint_vector_requires_exact_boundary_no_later_substitute() -> None:
    late = JointVector(dt(1, 0, 1), 100, 200)
    with pytest.raises(CalendarIntegrityError):
        exact_joint_vector((late,), dt(1))
    assert exact_joint_vector((JointVector(dt(1), 100, 200),), dt(1)).btc == 100


def test_delay_pending_fifth_event_timeout_and_recovery() -> None:
    pending = PendingTarget((1.0, 0.0, 0.0), dt(1))
    for minute in range(1, 5):
        assert pending is not None
        pending = pending.event(JointVector(dt(1, 0, minute), 100, 200))
    assert pending is not None
    assert pending.event(JointVector(dt(1, 0, 5), 100, 200)) is None
    assert PendingTarget((1.0, 0.0, 0.0), dt(1), 4).timed_out(dt(1, 1))
    state = Quarantine(False).trigger()
    for _ in range(60):
        state = state.valid_minute()
    assert state.may_resume(dt(1, 1), True)


def test_self_financing_drift_swap_costs_and_attribution() -> None:
    start = JointVector(dt(1), 100, 100)
    portfolio = rebalance(Portfolio(), (0.5, 0.5, 0.0), start, cell=0)
    assert portfolio.wealth == pytest.approx(1 - 0.0014)
    # A same target must still trade drift at the next mark.
    drifted = rebalance(portfolio, (0.5, 0.5, 0.0), JointVector(dt(1, 1), 110, 100), cell=0)
    assert drifted.wealth < portfolio.wealth * 1.1
    swapped = rebalance(drifted, (0.0, 1.0, 0.0), JointVector(dt(1, 2), 110, 105), cell=1)
    cash = rebalance(swapped, (0.0, 0.0, 1.0), JointVector(dt(1, 3), 110, 105), cell=1)
    assert sum(cash.asset_net) == pytest.approx(cash.wealth - 1, abs=1e-12)
    assert sum(cash.cell_net) == pytest.approx(cash.wealth - 1, abs=1e-12)


def test_quarantine_counters_terminal_and_prefix() -> None:
    q = Quarantine(True, 12, (1.0, 0.0, 0.0)).trigger()
    assert q.pending is None and q.consecutive_valid == 0 and q.exposed
    old = (0.0, 0.0, 1.0)
    new = (0.5, 0.5, 0.0)
    counters = (
        Counters().completed_fill(old, new, dt(6, 1)).completed_fill(new, (0.0, 0.0, 1.0), dt(6, 2))
    )
    assert counters.entries == counters.episodes == 1 and counters.asset_entries == (1, 1)
    assert terminal_target_time(dt(2)) == dt(1, 23)
    assert fold_prefix((dt(1), dt(2)), dt(2)) == (dt(1),)


def test_seeds_stats_degeneracies_and_prospective_deduplication() -> None:
    assert bootstrap_seed(7) == 4873322929811045809
    assert bootstrap_seed(28) != bootstrap_seed(91)
    with pytest.raises(CalendarIntegrityError):
        bootstrap_seed(8)
    assert dsr_degenerate_probability([0.0] * 30, [0.0] * 28) == 0
    assert pbo_degenerate([[0.0] * 8] * 7) == 1
    hour = dt(1)
    assert deduplicate_prospective(((hour, (1.0, 0.0, 0.0)), (hour, (0.0, 1.0, 0.0)))) == (
        (hour, (1.0, 0.0, 0.0)),
    )
    assert joint_targets(True, True) == (0.5, 0.5, 0.0)
