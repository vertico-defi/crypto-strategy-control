"""Synthetic deterministic tests for the frozen v2 pure contract only."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta, timezone

import pytest

import strategy_control.mean_reversion_v2 as mean_reversion_v2_module
from strategy_control.mean_reversion_v2 import (
    ASSETS,
    BOOTSTRAP_SEED,
    DEVELOPMENT_FOLDS,
    DOUBLED_ONE_WAY_COST_BPS,
    GATE_NAMES,
    GATE_REQUIREMENTS,
    ONE_WAY_COST_BPS,
    TRIAL_ORDER,
    TRIALS,
    AccountedInterval,
    AccountingFill,
    AccountingResult,
    Clock,
    Decision,
    Disposition,
    Fill,
    MeanReversionV2Error,
    PanelReturn,
    Target,
    accounting_equity_path,
    aggregate_gates,
    annualized_sharpe,
    canonical_hash,
    causal_gap_segments,
    common_panel,
    comparator_panel,
    compounded_equity,
    derive_integrity_evidence,
    dsr_probability,
    exact_signal,
    guard_development_relative_path,
    maximum_drawdown,
    multiplicity_counts,
    pbo,
    reconcile_accounting,
    reconcile_target_outcomes,
    reconcile_trace,
    regime_labels,
    require_predeclared_terminal_fill,
    self_financing,
    stationary_bootstrap,
    strict_prefix,
    target_identity,
    trace_hashes,
)


def dt(day: int, minute: int = 0) -> datetime:
    return datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=day, minutes=minute)


def identified_target(
    *,
    asset: str = "BTCUSDT",
    decision_session: datetime | None = None,
    fill_index: int = 1,
    fill_time: datetime | None = None,
    desired_weight: float = 0.5,
) -> Target:
    decision = decision_session or dt(0)
    execution = fill_time or dt(1)
    draft = Target("", asset, decision, fill_index, execution, desired_weight)
    return Target(
        target_identity(draft),
        asset,
        decision,
        fill_index,
        execution,
        desired_weight,
    )


def passing_metrics() -> dict[str, object]:
    values: dict[str, object] = {name: 1.0 for name in GATE_NAMES}
    values.update(
        {
            "annualized_sharpe_gte": 0.75,
            "positive_folds_minimum": 3,
            "fold_count": 4,
            "maximum_drawdown_lte": 0.1,
            "positive_parameter_neighbors_minimum": 3,
            "parameter_neighbor_count": 4,
            "completed_entries_total_minimum": 24,
            "completed_entries_each_asset_minimum": 10,
            "deflated_sharpe_probability_gte": 0.95,
            "probability_of_backtest_overfitting_lte": 0.2,
            "regime_gate": "pass",
            "exceptional_trade_gate": "pass",
            "baseline_superiority": True,
            "no_material_leakage": True,
        }
    )
    return values


def test_v1_signal_parameters_trials_costs_folds_gates_and_seed_are_exact() -> None:
    assert ASSETS == ("BTCUSDT", "ETHUSDT")
    assert BOOTSTRAP_SEED == 4480959964820476661
    assert tuple(
        (
            trial.name,
            trial.horizon,
            trial.volatility_lookback,
            trial.entry,
            trial.exit,
            trial.maximum_holding_intervals,
            trial.raw,
        )
        for trial in TRIALS
    ) == (
        ("primary_standardized_shock", 3, 20, -1.5, -0.25, 5, False),
        ("raw_three_session_drawdown_baseline", 3, None, -0.05, 0.0, 5, True),
        ("shorter_two_session_shock", 2, 20, -1.5, -0.25, 4, False),
        ("longer_five_session_shock", 5, 20, -1.5, -0.25, 7, False),
        ("shallower_entry", 3, 20, -1.25, -0.25, 5, False),
        ("deeper_entry", 3, 20, -1.75, -0.25, 5, False),
        ("slower_volatility_estimator", 3, 40, -1.5, -0.25, 5, False),
    )
    assert tuple(trial.name for trial in TRIALS) == TRIAL_ORDER
    assert (ONE_WAY_COST_BPS, DOUBLED_ONE_WAY_COST_BPS) == (14.0, 28.0)
    assert (
        (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC)),
        (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
        (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
        (datetime(2025, 10, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
    ) == DEVELOPMENT_FOLDS
    assert tuple(GATE_REQUIREMENTS) == GATE_NAMES
    assert dict(GATE_REQUIREMENTS) == {
        "aggregate_net_return_gt": 0.0,
        "annualized_sharpe_gte": 0.75,
        "positive_folds_minimum": 3,
        "fold_count": 4,
        "maximum_drawdown_lte": 0.2,
        "doubled_cost_aggregate_net_return_gt": 0.0,
        "additional_delay_aggregate_net_return_gt": 0.0,
        "positive_parameter_neighbors_minimum": 3,
        "parameter_neighbor_count": 4,
        "asset_standalone_net_return_each_gt": 0.0,
        "completed_entries_total_minimum": 24,
        "completed_entries_each_asset_minimum": 10,
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": 0.0,
        "deflated_sharpe_probability_gte": 0.95,
        "probability_of_backtest_overfitting_lte": 0.2,
        "regime_gate": "pass",
        "exceptional_trade_gate": "pass",
        "baseline_superiority": (
            "primary Sharpe strictly above both equal-weight buy-and-hold and "
            "raw-drawdown-baseline Sharpes, and primary maximum drawdown strictly "
            "below both comparators' maximum drawdowns"
        ),
        "no_material_leakage": True,
    }


def test_production_identity_and_real_row_obligations_are_explicitly_deferred() -> None:
    deferred = {
        "source_contract_and_36_entry_allowlist_hash_mismatch_rejected_before_resolution",
        "source_commit_byte_count_and_sha256_verified_before_parse",
        "verified_buffer_parse_uses_list_valued_pyarrow_columns",
        "representative_real_rows_materialize_from_all_allowlisted_files",
    }
    assert len(deferred) == 4


def test_future_or_holdout_path_rejected_before_stat_footer_schema_or_value_access() -> None:
    with pytest.raises(MeanReversionV2Error, match="before access"):
        guard_development_relative_path(
            "canonical/venue=binance/symbol=BTCUSDT/year=2026/month=01/x.parquet"
        )
    with pytest.raises(MeanReversionV2Error, match="before access"):
        guard_development_relative_path("../x.parquet")
    assert guard_development_relative_path("canonical/year=2025/month=01/x.parquet")


def test_expected_grid_detects_known_missing_minutes_and_jointly_absent_session_causally() -> None:
    assert causal_gap_segments([(dt(0), True), (dt(1), False)]) == [None, None]


def test_duplicate_nonmonotonic_invalid_or_asynchronous_rows_fail_closed() -> None:
    with pytest.raises(MeanReversionV2Error, match="nonmonotonic"):
        causal_gap_segments([(dt(1), True), (dt(0), True)])
    non_utc = dt(0).astimezone(timezone(timedelta(hours=1)))
    with pytest.raises(MeanReversionV2Error, match="must be UTC"):
        causal_gap_segments([(non_utc, True)])
    assert causal_gap_segments([(dt(0), True), (dt(1, 1), True)]) == [None, None]


def test_missing_exact_ordinary_fill_never_scans_forward() -> None:
    clock = Clock()
    _, target = clock.decide("BTCUSDT", dt(0), dt(1), 0, -2.0)
    assert target is not None
    with pytest.raises(MeanReversionV2Error, match="forward scan prohibited"):
        clock.apply_fill(dt(2), 100.0, 1)


def test_cash_quarantine_splits_segments_and_never_bridges_return() -> None:
    labels = causal_gap_segments([(dt(index), index != 2) for index in range(153)])
    assert labels[1] is None
    assert labels[2] is None
    assert labels[-1] == 1
    intervals = {
        name: [
            PanelReturn(0, 1, 0.01, True, True, False),
            PanelReturn(1, 3, 0.25, True, False, False),
        ]
        for name in TRIAL_ORDER
    }
    assert common_panel(intervals)[TRIAL_ORDER[0]] == [0.01]


def test_risky_or_pending_exit_quarantine_is_data_integrity_failure() -> None:
    clock = Clock()
    clock.actual = 0.5
    with pytest.raises(MeanReversionV2Error, match="DATA_INTEGRITY_FAILURE"):
        clock.quarantine()
    pending_exit = Clock()
    pending_exit.actual = 0.5
    pending_exit.pending = identified_target(desired_weight=0.0)
    with pytest.raises(MeanReversionV2Error, match="DATA_INTEGRITY_FAILURE"):
        pending_exit.quarantine()


def test_recovery_requires_150_new_contiguous_joint_sessions() -> None:
    labels = causal_gap_segments([(dt(index), True) for index in range(150)])
    assert labels[:149] == [None] * 149
    assert labels[149] == 0


def test_regime_history_and_expanding_median_reset_at_gap() -> None:
    closes = [100.0 * 1.001**index for index in range(400)]
    segments = [0] * 220 + [None] + [1] * 179
    labels = regime_labels(closes, segments)
    assert any(label is not None for label in labels[:220])
    assert labels[220] is None
    assert labels[221:399].count(None) == 178


def test_future_gap_cannot_change_prior_regime_label() -> None:
    closes = [100.0 * 1.001**index for index in range(240)]
    prefix = regime_labels(closes[:220], [0] * 220)
    with_gap = regime_labels(closes, [0] * 220 + [None] + [1] * 19)
    assert prefix == with_gap[:220]


def test_prior_target_fill_updates_actual_state_before_later_decision() -> None:
    clock = Clock()
    _, entry = clock.decide("BTCUSDT", dt(0), dt(1), 0, -2.0)
    assert entry is not None
    assert clock.apply_fill(dt(1), 100.0, 0) is not None
    decision, _ = clock.decide("BTCUSDT", dt(1), dt(2), 1, -2.0)
    assert decision.actual_before == 0.5
    with pytest.raises(MeanReversionV2Error, match="identity changed"):
        clock.decide("ETHUSDT", dt(2), dt(3), 2, -2.0)


def test_executed_fill_does_not_suppress_valid_completed_session_decision() -> None:
    clock = Clock()
    _, entry = clock.decide("BTCUSDT", dt(0), dt(1), 0, -2.0)
    assert entry is not None
    clock.apply_fill(dt(1), 100.0, 0)
    decision, exit_target = clock.decide("BTCUSDT", dt(1), dt(2), 1, 0.0)
    assert decision.pending is False
    assert exit_target is not None


def test_still_pending_target_suppresses_new_decision() -> None:
    clock = Clock(delay=1)
    clock.decide(
        "BTCUSDT",
        dt(0),
        dt(1),
        0,
        -2.0,
        delayed_fill_time=dt(3),
    )
    assert clock.apply_fill(dt(1), 100.0, 0) is None
    decision, target = clock.decide("BTCUSDT", dt(1), dt(2), 1, 0.0)
    assert decision.pending is True
    assert target is None


def test_base_target_executes_at_exact_B_s_and_delay_at_exact_B_s_plus_one_session() -> None:
    base = Clock()
    delayed = Clock(delay=1)
    _, base_target = base.decide("BTCUSDT", dt(0), dt(1), 5, -2.0)
    _, delayed_target = delayed.decide(
        "BTCUSDT",
        dt(0),
        dt(1),
        5,
        -2.0,
        delayed_fill_time=dt(4),
    )
    assert base_target is not None and delayed_target is not None
    assert (base_target.fill_index, base_target.fill_time) == (5, dt(1))
    assert (delayed_target.fill_index, delayed_target.fill_time) == (6, dt(4))
    assert base.apply_fill(dt(1), 100.0, 5) is not None
    assert delayed.apply_fill(dt(1), 100.0, 5) is None
    assert delayed.apply_fill(dt(4), 100.0, 6) is not None


def test_entry_at_j_forced_exit_executes_at_j_plus_5() -> None:
    clock = Clock()
    _, entry = clock.decide("BTCUSDT", dt(0), dt(1), 10, -2.0)
    assert entry is not None
    clock.apply_fill(dt(1), 100.0, 10)
    for offset in range(1, 5):
        _, target = clock.decide("BTCUSDT", dt(offset), dt(offset + 1), 10 + offset, -2.0)
        assert target is None
    _, exit_target = clock.decide("BTCUSDT", dt(5), dt(6), 15, -2.0)
    assert exit_target is not None
    assert exit_target.fill_index == 15
    assert clock.apply_fill(dt(6), 100.0, 15) is not None
    assert clock.actual == 0.0


def test_recovery_exit_can_execute_before_j_plus_5() -> None:
    clock = Clock()
    _, entry = clock.decide("BTCUSDT", dt(0), dt(1), 10, -2.0)
    assert entry is not None
    clock.apply_fill(dt(1), 100.0, 10)
    _, exit_target = clock.decide("BTCUSDT", dt(1), dt(2), 11, 0.0)
    assert exit_target is not None
    assert exit_target.fill_index == 11
    clock.apply_fill(dt(2), 100.0, 11)
    assert clock.actual == 0.0


def test_raw_drawdown_baseline_exits_on_daily_not_three_session_return() -> None:
    clock = Clock(TRIALS[1])
    _, entry = clock.decide("BTCUSDT", dt(0), dt(1), 10, -0.06)
    assert entry is not None
    clock.apply_fill(dt(1), 100.0, 10)
    _, no_exit = clock.decide(
        "BTCUSDT",
        dt(1),
        dt(2),
        11,
        0.02,
        raw_daily_return=-0.01,
    )
    assert no_exit is None
    _, exit_target = clock.decide(
        "BTCUSDT",
        dt(2),
        dt(3),
        12,
        -0.02,
        raw_daily_return=0.01,
    )
    assert exit_target is not None


def test_every_target_has_exactly_one_fill_cancel_or_terminal_disposition() -> None:
    target = identified_target()
    fill = Fill(target.target_id, "BTCUSDT", 1, dt(1), 100.0, 0.5)
    disposition = Disposition(target.target_id, "fill", dt(1))
    assert reconcile_target_outcomes([target], [fill], [disposition])
    assert not reconcile_target_outcomes([target], [], [disposition])
    wrong_index = Fill(target.target_id, "BTCUSDT", 2, dt(1), 100.0, 0.5)
    assert not reconcile_target_outcomes([target], [wrong_index], [disposition])


def test_decision_target_fill_and_return_trace_hashes_reconcile() -> None:
    decision = Decision("BTCUSDT", dt(0), 0.0, False, -2.0, 0.5)
    target = identified_target()
    fill = Fill(target.target_id, "BTCUSDT", 1, dt(1), 100.0, 0.5)
    disposition = Disposition(target.target_id, "fill", dt(1))
    values = {
        "inputs": ["i"],
        "decisions": [decision],
        "targets": [target],
        "fills": [fill],
        "dispositions": [disposition],
        "costs": [0.001],
        "returns": [0.01],
    }
    expected = trace_hashes(**values)
    assert reconcile_trace(**values, expected_hashes=expected)
    assert not reconcile_trace(**{**values, "returns": [math.nan]}, expected_hashes=expected)


def test_strict_fold_prefix_blocks_every_post_end_row_gap_and_availability_event() -> None:
    rows = [(dt(0), 1), (dt(2), "future")]
    assert strict_prefix(rows, dt(1)) == [(dt(0), 1)]
    assert strict_prefix([*rows, (dt(3), "later")], dt(1)) == [(dt(0), 1)]
    assert strict_prefix([*rows, (dt(0), "malformed future suffix")], dt(1)) == [(dt(0), 1)]


def test_each_fold_and_aggregate_path_start_independently_in_cash() -> None:
    paths = [Clock(), Clock(delay=1), *(Clock(trial) for trial in TRIALS)]
    assert len({id(path) for path in paths}) == len(paths)
    assert all(path.actual == 0.0 and path.pending is None for path in paths)


def test_no_return_interval_crosses_fold_or_quarantine_boundary() -> None:
    panel = {
        name: [
            PanelReturn(0, 1, 0.0, True, True, True),
            PanelReturn(1, 3, 0.1, True, False, False),
        ]
        for name in TRIAL_ORDER
    }
    assert common_panel(panel)[TRIAL_ORDER[0]] == [0.0]


def test_forced_terminal_fill_is_predeclared_inside_half_open_boundary_and_costed() -> None:
    eligible = [dt(0), dt(1), dt(2)]
    assert require_predeclared_terminal_fill(eligible, dt(2), dt(3)) == dt(2)
    with pytest.raises(MeanReversionV2Error, match="predeclared terminal"):
        require_predeclared_terminal_fill(eligible, dt(1), dt(3))
    fills = [
        AccountingFill(
            dt(0),
            {"BTCUSDT": 100.0, "ETHUSDT": 100.0},
            {"BTCUSDT": 0.5, "ETHUSDT": 0.0},
        ),
        AccountingFill(
            dt(2),
            {"BTCUSDT": 110.0, "ETHUSDT": 100.0},
            {"BTCUSDT": 0.0, "ETHUSDT": 0.0},
        ),
    ]
    result = self_financing(fills)
    assert result.terminal_cash
    assert result.intervals[-1].cost > 0.0


def test_base_doubled_delay_variant_baseline_fold_and_standalone_states_are_independent() -> None:
    clocks = [Clock(trial) for trial in TRIALS]
    clocks.extend([Clock(), Clock(delay=1), Clock()])
    assert len({id(clock) for clock in clocks}) == len(clocks)


def test_common_panel_requires_consecutive_valid_endpoints_and_genuine_cash_zero() -> None:
    panel = {
        name: [
            PanelReturn(0, 1, 0.0, True, True, True),
            PanelReturn(1, 2, 0.1, True, True, False),
            PanelReturn(2, 3, 0.0, True, True, False),
        ]
        for name in TRIAL_ORDER
    }
    assert common_panel(panel)[TRIAL_ORDER[0]] == [0.0, 0.1]
    duplicate = {name: [*rows, rows[1]] for name, rows in panel.items()}
    with pytest.raises(MeanReversionV2Error, match="duplicate valid panel endpoint"):
        common_panel(duplicate)


def test_baseline_sharpe_and_drawdown_recomputed_on_identical_panels() -> None:
    primary = [
        PanelReturn(0, 1, 0.01, True, True, False),
        PanelReturn(1, 2, 0.02, True, True, False),
    ]
    baseline = [
        PanelReturn(0, 1, -0.01, True, True, False),
        PanelReturn(1, 2, 0.01, True, True, False),
        PanelReturn(2, 3, 0.5, True, True, False),
    ]
    primary_common, baseline_common = comparator_panel(primary, baseline)
    assert (primary_common, baseline_common) == ([0.01, 0.02], [-0.01, 0.01])
    assert annualized_sharpe(primary_common) > annualized_sharpe(baseline_common)
    assert maximum_drawdown(compounded_equity(primary_common)) == 0.0
    assert maximum_drawdown(compounded_equity(baseline_common)) == pytest.approx(0.01)


def test_independent_units_costs_returns_and_terminal_wealth_reconcile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fills = [
        AccountingFill(
            dt(0),
            {"BTCUSDT": 100.0, "ETHUSDT": 100.0},
            {"BTCUSDT": 0.5, "ETHUSDT": 0.0},
        ),
        AccountingFill(
            dt(1),
            {"BTCUSDT": 110.0, "ETHUSDT": 100.0},
            {"BTCUSDT": 0.0, "ETHUSDT": 0.0},
        ),
    ]
    result = self_financing(fills)
    assert len(result.intervals) == 1
    assert result.terminal_cash
    assert maximum_drawdown(accounting_equity_path(result)) >= result.initial_cost
    monkeypatch.setattr(
        mean_reversion_v2_module,
        "self_financing",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("shared evaluator path")),
    )
    assert reconcile_accounting(fills, result)
    broken_interval = AccountedInterval(
        **{**result.intervals[0].__dict__, "cost": result.intervals[0].cost + 0.1}
    )
    broken = AccountingResult((broken_interval,), result.initial_cost, result.terminal_equity, True)
    assert not reconcile_accounting(fills, broken)


def test_49_attempt_DSR_registry_has_35_observed_and_14_unimputed_slots() -> None:
    assert multiplicity_counts() == {"N": 49, "observed": 35, "unimputed": 14}
    assert multiplicity_counts(2) == {"N": 56, "observed": 42, "unimputed": 14}
    assert multiplicity_counts(3) == {"N": 63, "observed": 49, "unimputed": 14}
    assert multiplicity_counts(1, holdout=True) == {
        "N": 56,
        "observed": 42,
        "unimputed": 14,
    }


def test_v1_bootstrap_seed_and_linear_percentiles_are_exact() -> None:
    assert BOOTSTRAP_SEED == 4480959964820476661
    lower_a, upper_a = stationary_bootstrap([0.01, -0.01, 0.02], resamples=20)
    lower_b, upper_b = stationary_bootstrap([0.01, -0.01, 0.02], resamples=20)
    assert (lower_a, upper_a) == (lower_b, upper_b)
    assert lower_a == 0.006666666666666667
    assert upper_a == 0.008416666666666663


def test_PBO_has_exactly_seven_current_trials_eight_blocks_and_70_splits() -> None:
    matrix = [[0.01 if (index + trial) % 2 else -0.01 for index in range(16)] for trial in range(7)]
    assert pbo(matrix) == 1.0
    assert pbo(matrix[:6]) == 1.0
    assert pbo([[0.0] * 8 for _ in range(7)]) == 1.0


def test_DSR_autocorrelation_and_dynamic_registry_degeneracies_fail_closed() -> None:
    primary = [0.01 if index % 2 else -0.005 for index in range(80)]
    assert dsr_probability([0.01] * 30, [0.1] * 35, N=49) == 0.0
    assert dsr_probability(primary, [0.001 * index for index in range(35)], N=56) == 0.0
    value = dsr_probability(primary, [0.001 * index for index in range(42)], N=56)
    assert value == pytest.approx(0.996461190447893, abs=1e-15)


def test_gate_map_has_exactly_19_named_gates_and_unknown_or_missing_gate_fails() -> None:
    report = aggregate_gates(passing_metrics())
    assert len(report) == 19
    assert all(report.values())
    assert not any(aggregate_gates({**passing_metrics(), "unknown": 1}).values())
    assert not aggregate_gates({**passing_metrics(), "annualized_sharpe_gte": math.nan})[
        "annualized_sharpe_gte"
    ]
    assert not aggregate_gates({**passing_metrics(), "fold_count": 4.0})["fold_count"]
    assert not aggregate_gates({**passing_metrics(), "fold_count": 5})["fold_count"]


def test_no_material_leakage_terminal_cash_and_holdout_closure_are_derived_not_hardcoded() -> None:
    assert derive_integrity_evidence(
        input_identity_pass=True,
        trace_reconciliation_pass=True,
        terminal_cash=True,
        strict_prefix_pass=True,
        holdout_closed=True,
        gate_names=GATE_NAMES,
    )
    assert not derive_integrity_evidence(
        input_identity_pass=True,
        trace_reconciliation_pass=True,
        terminal_cash=False,
        strict_prefix_pass=True,
        holdout_closed=True,
        gate_names=GATE_NAMES,
    )


def test_future_close_cannot_change_prior_signal() -> None:
    closes = [100.0 + index for index in range(30)]
    before = exact_signal(closes, 25)
    closes[-1] = math.nan
    assert exact_signal(closes, 25) == before


def test_canonical_hash_rejects_nonfinite_and_normalizes_key_order() -> None:
    assert canonical_hash({"a": 1, "b": 2}) == canonical_hash({"b": 2, "a": 1})
    with pytest.raises(MeanReversionV2Error, match="nonfinite"):
        canonical_hash({"value": math.nan})
    with pytest.raises(MeanReversionV2Error, match="keys must be strings"):
        canonical_hash({1: "ambiguous"})
