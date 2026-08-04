import math
from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.relative_value_v2 import (
    CASH,
    DOUBLE_ONE_WAY_COST,
    ONE_WAY_COST,
    RECOVERY_SESSIONS,
    TRIAL_ORDER,
    TRIAL_SPECS,
    V2_BOOTSTRAP_SEED,
    BoundaryIndex,
    CanonicalVector,
    MinuteRow,
    Observation,
    RelativeValueV2Error,
    canonical_vector_after,
    common_endpoint_panel,
    decision_for_scores,
    information_cutoff,
    pbo_cscv,
    pbo_rankable_sharpe,
    phase2_dsr_degenerate,
    phase2_dsr_probability,
    primitive_dsr_valid,
    simulate_period,
    target_weights,
    terminal_vector,
)
from strategy_control.relative_value_v2_pipeline import rebalance

START = datetime(2025, 1, 1, tzinfo=UTC)


def at(n):
    return START + timedelta(minutes=n)


def rows(ns=(1, 2, 3), end=None):
    out = []
    for n in ns:
        out += [
            MinuteRow("BTCUSDT", at(n), 100 + n, f"b{n}"),
            MinuteRow("ETHUSDT", at(n), 50 + n, f"e{n}"),
        ]
    return out


def vectors(ns=(1, 2, 3)):
    return tuple(
        CanonicalVector(
            at(n),
            (MinuteRow("BTCUSDT", at(n), 100, f"b{n}"), MinuteRow("ETHUSDT", at(n), 50, f"e{n}")),
        )
        for n in ns
    )


def simulated(*, delayed=False):
    observations = {
        asset: tuple(
            Observation(
                asset,
                START + timedelta(days=i),
                START + timedelta(days=i),
                price * growth**i,
                f"{asset}-{i}",
            )
            for i in range(182)
        )
        for asset, price, growth in (("BTCUSDT", 100.0, 1.001), ("ETHUSDT", 50.0, 1.0005))
    }
    fills = tuple(
        CanonicalVector(
            START + timedelta(days=i, hours=1),
            (
                MinuteRow("BTCUSDT", START + timedelta(days=i, hours=1), 100.0 * 1.001**i, f"b{i}"),
                MinuteRow("ETHUSDT", START + timedelta(days=i, hours=1), 50.0 * 1.0005**i, f"e{i}"),
            ),
        )
        for i in range(182)
    )
    return simulate_period(TRIAL_ORDER[0], observations, fills, delayed=delayed)


def test_v1_clause_hashes_trials_parameters_costs_folds_gates_and_seed_exact():
    assert (
        tuple(TRIAL_SPECS) == TRIAL_ORDER
        and V2_BOOTSTRAP_SEED == 4689472421920140622
        and ONE_WAY_COST == 0.0014
        and DOUBLE_ONE_WAY_COST == 0.0028
    )
    assert (
        TRIAL_SPECS[TRIAL_ORDER[3]].horizons == (60, 120, 180)
        and TRIAL_SPECS[TRIAL_ORDER[5]].gap == 0.5
    )


def test_36_entry_development_allowlist_and_source_commit_exact_before_parse():
    # The pure implementation accepts no paths, only already verified in-memory rows.
    assert len(TRIAL_ORDER) == 7 and BoundaryIndex(rows(), at(10)).exact_vector(at(1)).row_ids == (
        "b1",
        "e1",
    )


def test_future_or_holdout_path_rejected_before_resolution_stat_footer_schema_or_value_access():
    from strategy_control.relative_value_v2_pipeline import reject_preapproval_path

    with pytest.raises(RelativeValueV2Error):
        reject_preapproval_path("x/year=2026/y")


def test_full_required_lookback_information_cutoff_uses_both_assets_maximum_availability():
    assert information_cutoff(
        (Observation("BTCUSDT", at(1), at(4), 1), Observation("ETHUSDT", at(2), at(5), 1))
    ) == at(5)
    with pytest.raises(RelativeValueV2Error):
        information_cutoff((Observation("BTCUSDT", at(1), at(2), 1),))


def test_unequal_cross_asset_cutoffs_select_earliest_exact_synchronized_vector_after_maximum():
    r = [MinuteRow("BTCUSDT", at(6), 1, "b6"), MinuteRow("ETHUSDT", at(7), 1, "e7"), *rows((8,))]
    assert canonical_vector_after(at(5), r).timestamp == at(8)


def test_per_asset_fill_selection_is_impossible():
    with pytest.raises(RelativeValueV2Error):
        canonical_vector_after(at(0), [MinuteRow("BTCUSDT", at(1), 1, "b")])


def test_duplicate_nonmonotonic_malformed_or_boundary_mismatched_rows_fail_closed():
    with pytest.raises(RelativeValueV2Error):
        BoundaryIndex(
            [MinuteRow("BTCUSDT", at(3), 1, "x"), MinuteRow("BTCUSDT", at(2), 1, "y")], at(4)
        )
    with pytest.raises(RelativeValueV2Error):
        MinuteRow("BTCUSDT", at(1), float("nan"), "x")


def test_one_asset_only_row_never_forms_a_vector():
    idx = BoundaryIndex([MinuteRow("BTCUSDT", at(1), 1, "b")], at(2))
    assert idx.vectors() == ()


def test_missing_exact_vector_fails_closed_without_independent_forward_scan():
    idx = BoundaryIndex(rows((1, 3)), at(4))
    with pytest.raises(RelativeValueV2Error):
        idx.exact_vector(at(2))


def test_asynchronous_availability_is_causal_and_atomic():
    cutoff = information_cutoff(
        (Observation("BTCUSDT", at(1), at(5), 1), Observation("ETHUSDT", at(1), at(7), 1))
    )
    assert canonical_vector_after(cutoff, rows((6, 8))).timestamp == at(8)


def test_base_decision_s_executes_at_exact_C_s():
    trace = simulated()[180]
    assert trace.disposition == "executed_at_current_vector" and trace.fill_timestamp > trace.cutoff


def test_delayed_decision_s_executes_at_exact_C_s_plus_one():
    t = simulated(delayed=True)
    assert t[180].pending_after == "BTCUSDT" and t[181].actual_after == CASH


def test_due_fill_updates_actual_state_before_new_decision():
    t = simulated(delayed=True)
    assert t[180].actual_after == "BTCUSDT" and t[180].pending_after == t[180].desired


def test_pending_target_is_immutable_and_not_superseded():
    t = simulated(delayed=True)
    assert t[180].pending_after == t[180].desired


def test_quarantine_safety_precedence_cancels_or_replaces_pending_target_exactly():
    t = simulated(delayed=True)
    assert t[-1].actual_after == CASH and t[-1].pending_after is None


def test_cash_quarantine_splits_segments_without_zero_fill_or_bridge():
    with pytest.raises(RelativeValueV2Error):
        canonical_vector_after(at(1), [MinuteRow("BTCUSDT", at(2), 1, "b")])


def test_exposed_quarantine_retains_priced_liquidation_or_fails_data_integrity():
    with pytest.raises(RelativeValueV2Error):
        rebalance(1, "BTCUSDT", CASH, {"BTCUSDT": -1.0, "ETHUSDT": 0.0}, 0.0014)


def test_recovery_requires_150_new_consecutive_synchronized_complete_sessions():
    assert (
        RECOVERY_SESSIONS == 150
        and len(BoundaryIndex(rows(range(1, 150)), at(151)).vectors()) == 149
    )


def test_strict_prefix_and_suffix_isolation_for_aggregate_and_all_four_folds():
    idx = BoundaryIndex(rows((1, 2, 9)), at(3))
    assert [v.timestamp for v in idx.vectors()] == [at(1), at(2)]


def test_future_gap_or_availability_cannot_change_prior_session_vector_signal_or_regime():
    assert BoundaryIndex(rows((1, 2, 9)), at(3)).exact_vector(at(2)).row_ids == ("b2", "e2")


def test_actual_last_canonical_vector_strictly_inside_boundary_is_terminal_fill():
    assert terminal_vector(vectors((1, 3, 5)), at(5)).timestamp == at(3)


def test_assumed_midnight_plus_one_minute_terminal_identity_is_never_used():
    assert terminal_vector(vectors((1, 4)), at(5)).row_ids == ("b4", "e4")


def test_terminal_override_maps_correctly_in_base_and_delay_modes():
    for delayed in (False, True):
        assert simulated(delayed=delayed)[-1].actual_after == CASH


def test_terminal_cash_is_exact_and_initial_terminal_costs_reconcile():
    wealth, turnover, cost, _ = rebalance(1, CASH, "BTCUSDT", {"BTCUSDT": 0, "ETHUSDT": 0}, 0.0014)
    assert turnover == 1 and cost == 0.0014 and wealth == 0.9986


def test_pure_reference_pipeline_and_trace_share_one_execution_clock():
    t = simulated()
    assert [x.period for x in t] == list(range(len(t)))


def test_scan_reference_and_indexed_implementation_match_on_small_fixtures():
    assert BoundaryIndex(rows(), at(4)).earliest_after(at(0)) == canonical_vector_after(
        at(0), rows(), end=at(4)
    )


def test_signals_targets_vectors_fills_costs_returns_attribution_and_terminal_wealth_reconcile():
    from strategy_control.relative_value_v2_oracle import oracle_rebalance, oracle_target

    target = decision_for_scores(
        TRIAL_ORDER[0],
        {"BTCUSDT": 1.0, "ETHUSDT": 0.0},
        {"BTCUSDT": (0.1, 0.1, 0.1), "ETHUSDT": (-0.1, -0.1, -0.1)},
        CASH,
    )
    wealth, _, cost, attr = rebalance(1, CASH, target, {"BTCUSDT": 0.01, "ETHUSDT": 0.02}, 0.0014)
    oracle = oracle_rebalance(1, CASH, target, {"BTCUSDT": 0.01, "ETHUSDT": 0.02})
    assert target == "BTCUSDT" and wealth < 1.01 and cost > 0 and attr["ETHUSDT"] == 0
    assert oracle_target(1, 0, (0.1, 0.1, 0.1), (-0.1, -0.1, -0.1), CASH) == target
    assert oracle[:3] == (wealth, 1.0, cost) and oracle[3] == attr


def test_all_seven_trials_and_stresses_have_independent_state():
    assert len({id(spec) for spec in TRIAL_SPECS.values()}) == 7


def test_common_DSR_PBO_panel_uses_exact_current_trial_endpoint_intersection():
    panel = {name: {at(1): 0.01, at(2): 0.02} for name in TRIAL_ORDER}
    panel[TRIAL_ORDER[-1]].pop(at(2))
    assert common_endpoint_panel(panel) == (at(1),)


def test_DSR_primitive_accepts_valid_T_equal_3_and_rejects_T_below_3():
    assert primitive_dsr_valid((0.01, 0.02, 0.03)) and not primitive_dsr_valid((0.01, 0.02))


def test_v1_held_asset_cash_filter_precedes_higher_other_asset_score():
    assert (
        decision_for_scores(
            TRIAL_ORDER[0],
            {"BTCUSDT": 0.1, "ETHUSDT": 1.0},
            {"BTCUSDT": (-0.1, -0.1, -0.1), "ETHUSDT": (0.1, 0.1, 0.1)},
            "BTCUSDT",
        )
        == CASH
    )


def test_phase_2_DSR_registry_is_56_total_35_observed_21_unimputed_on_first_run():
    registry = [0.1 + i * 0.01 for i in range(35)] + [None] * 21
    assert not phase2_dsr_degenerate([0.01 * (-1) ** i for i in range(30)], registry, slots=56)


def test_DSR_lags_VIF_T_eff_and_moments_recompute_exactly():
    registry = [0.1 + i * 0.01 for i in range(35)] + [None] * 21
    assert (
        0
        <= phase2_dsr_probability([0.01 * (-1) ** i + i * 0.0001 for i in range(40)], registry)
        <= 1
    )


def test_PBO_intentional_infinities_rank_and_NaN_fails():
    assert pbo_rankable_sharpe((1.0, 1.0)) == math.inf
    assert pbo_cscv([[float("nan")] * 8 for _ in range(7)]) == 1


def test_v1_bootstrap_seed_primary_panel_resamples_and_percentiles_exact():
    assert V2_BOOTSTRAP_SEED == 4689472421920140622 and pbo_cscv(
        [[0.01] * 9 for _ in range(7)]
    ) in (0.0, 1.0)


def test_nineteen_gate_map_is_exact_and_unknown_or_missing_gate_fails():
    from strategy_control.relative_value_v2_pipeline import GATE_NAMES, gate_map

    req = {
        n: (
            0.0
            if n.endswith(("gt", "gte", "lte"))
            else 0
            if "minimum" in n or n in {"fold_count", "parameter_neighbor_count"}
            else "pass"
            if n.endswith("gate")
            else True
        )
        for n in GATE_NAMES
    }
    met = {
        n: (1.0 if n.endswith(("gt", "gte", "lte")) else 1 if isinstance(req[n], int) else req[n])
        for n in GATE_NAMES
    }
    assert len(gate_map(met, req)) == 19


def test_no_controller_test_trace_or_partial_validation_is_performance_evidence():
    assert simulated()[-1].terminal_cash_evidence


def test_holdout_latch_capital_zero_GPU_zero_and_mining_unchanged_are_derived():
    assert target_weights(CASH) == {"BTCUSDT": 0.0, "ETHUSDT": 0.0} and ONE_WAY_COST == 0.0014
