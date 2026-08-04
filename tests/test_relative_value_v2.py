from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.relative_value_v2 import (
    CASH,
    SYMBOLS,
    CanonicalVector,
    MinuteRow,
    Observation,
    RelativeValueV2Error,
    canonical_vector_after,
    common_endpoint_panel,
    information_cutoff,
    pbo_rankable_sharpe,
    phase2_dsr_degenerate,
    primitive_dsr_valid,
    run_clock,
    terminal_vector,
)


def _at(minute: int) -> datetime:
    return datetime(2025, 1, 1, tzinfo=UTC) + timedelta(minutes=minute)


def _vector(minute: int) -> CanonicalVector:
    return CanonicalVector(
        _at(minute),
        (
            MinuteRow(SYMBOLS[0], _at(minute), 100, f"b{minute}"),
            MinuteRow(SYMBOLS[1], _at(minute), 50, f"e{minute}"),
        ),
    )


def test_full_required_lookback_information_cutoff_uses_both_assets_maximum_availability() -> None:
    observations = (
        Observation(SYMBOLS[0], _at(1), _at(4), 1),
        Observation(SYMBOLS[1], _at(3), _at(8), 2),
    )
    assert information_cutoff(observations) == _at(8)
    with pytest.raises(RelativeValueV2Error):
        information_cutoff(observations[:1])


def test_unequal_cross_asset_cutoffs_select_earliest_exact_synchronized_vector_after_maximum() -> (
    None
):
    rows = [
        MinuteRow(SYMBOLS[0], _at(9), 1, "b9"),
        MinuteRow(SYMBOLS[1], _at(10), 1, "e10"),
        MinuteRow(SYMBOLS[0], _at(11), 1, "b11"),
        MinuteRow(SYMBOLS[1], _at(11), 1, "e11"),
    ]
    assert canonical_vector_after(_at(8), rows).timestamp == _at(11)
    with pytest.raises(RelativeValueV2Error):
        canonical_vector_after(_at(8), rows[:2])


def test_duplicate_nonmonotonic_malformed_or_boundary_mismatched_rows_fail_closed() -> None:
    rows = [MinuteRow(SYMBOLS[0], _at(10), 1, "b"), MinuteRow(SYMBOLS[0], _at(10), 1, "b2")]
    with pytest.raises(RelativeValueV2Error):
        canonical_vector_after(_at(1), rows)
    with pytest.raises(RelativeValueV2Error):
        MinuteRow(SYMBOLS[0], _at(1), float("nan"), "bad")


def test_base_delayed_clocks_are_independent_pending_is_immutable_and_terminal_cash_exact() -> None:
    vectors = (_vector(1), _vector(2), _vector(3))
    decisions = (("s1", "BTCUSDT"), ("s2", "ETHUSDT"), ("s3", "BTCUSDT"))
    base, delayed = (
        run_clock(decisions, vectors, delayed=False),
        run_clock(decisions, vectors, delayed=True),
    )
    assert base[0].actual_after == "BTCUSDT" and base[-1].actual_after == CASH
    assert delayed[0].actual_after == CASH and delayed[0].pending_after == "BTCUSDT"
    assert delayed[1].actual_before == CASH and delayed[1].actual_after == "BTCUSDT"
    assert delayed[-1].actual_after == CASH and delayed[-1].pending_after is None


def test_actual_last_canonical_vector_strictly_inside_boundary_is_terminal_fill() -> None:
    vectors = (_vector(1), _vector(3), _vector(5))
    assert terminal_vector(vectors, _at(5)).timestamp == _at(3)


def test_common_DSR_PBO_panel_uses_exact_current_trial_endpoint_intersection_and_degeneracies() -> (
    None
):
    from strategy_control.relative_value_v2 import TRIAL_ORDER

    panel = {name: {_at(1): 0.01, _at(2): 0.02} for name in TRIAL_ORDER}
    panel[TRIAL_ORDER[-1]].pop(_at(2))
    assert common_endpoint_panel(panel) == (_at(1),)
    assert primitive_dsr_valid((0.01, 0.02, 0.03))
    assert not primitive_dsr_valid((0.01, 0.02))
    registry = [0.1] * 35 + [None] * 21
    assert phase2_dsr_degenerate([0.01] * 30, registry, slots=56)
    assert pbo_rankable_sharpe((0.0, 0.0)) == -float("inf")
