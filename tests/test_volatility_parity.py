from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from strategy_control.volatility_parity import (
    BASE_COST_RATE,
    CASH,
    EXPERIMENT_ID,
    PRIMARY,
    SYMBOLS,
    TRIALS,
    MinuteBar,
    Session,
    Trial,
    VolatilityParityError,
    arm_holdout_latch,
    bounded_inverse_volatility,
    canonical_vector,
    contiguous_return_window,
    create_holdout_latch,
    cscv_pbo,
    deduplicate_prospective_keys,
    deflated_sharpe,
    delayed_pending,
    event_drawdown,
    exceptional_profit,
    execute_trade,
    expected_whole_minute_open,
    initial_account,
    mark_account,
    materialize_target,
    order_lifecycle,
    prospective_decision_key,
    rebalance_minima,
    reconcile_contributions,
    recovery_eligible,
    regime_gate,
    regime_labels,
    stationary_bootstrap,
    trade_account,
    validate_covariance,
)


def _matrix(length: int, eth_scale: float = 2.0) -> tuple[tuple[float, float], ...]:
    return tuple(
        (
            (1 if index % 2 else -1) * 0.01,
            (1 if index % 2 else -1) * 0.01 * eth_scale,
        )
        for index in range(length)
    )


def _session(
    start: datetime,
    *,
    close: float = 100.0,
    complete: bool = True,
    available_delay: timedelta = timedelta(0),
    rows: tuple[MinuteBar, ...] = (),
) -> Session:
    return Session(
        start=start,
        available_timestamp=start + timedelta(days=1) + available_delay,
        open=close,
        high=close,
        low=close,
        close=close,
        complete=complete,
        rows=rows,
    )


def _target(start: datetime) -> object:
    return materialize_target(PRIMARY, _session(start), _matrix(60), input_ids=("fixed",))


def test_raw_inverse_volatility_ERC_before_clipping() -> None:
    estimate = bounded_inverse_volatility(_matrix(60, 3.0))
    assert estimate.raw[SYMBOLS[0]] == pytest.approx(0.75)
    assert estimate.raw_component_risks[0] == pytest.approx(estimate.raw_component_risks[1])
    assert sum(estimate.raw_component_risks) == pytest.approx(estimate.raw_portfolio_risk)


def test_clipped_weights_not_asserted_ERC() -> None:
    estimate = bounded_inverse_volatility(_matrix(60, 4.0), Trial("synthetic", 60, 0.2, 0.6))
    assert estimate.bounded[SYMBOLS[0]] == 0.6
    assert estimate.bounded_component_risks[0] != pytest.approx(estimate.bounded_component_risks[1])


def test_perfect_anticorrelation_zero_variance_fails() -> None:
    matrix = tuple((value, -value) for value, _ in _matrix(60))
    with pytest.raises(VolatilityParityError, match="variance degeneracy"):
        bounded_inverse_volatility(matrix)


def test_covariance_symmetry_eigenvalue_and_scale_tolerances() -> None:
    assert validate_covariance(((1.0, 0.5), (0.5, 1.0)), (1.0, 1.0))[0][1] == 0.5
    with pytest.raises(VolatilityParityError, match="asymmetric"):
        validate_covariance(((1.0, 0.5), (0.4, 1.0)), (1.0, 1.0))
    with pytest.raises(VolatilityParityError, match="positive semidefinite"):
        validate_covariance(((1.0, 2.0), (2.0, 1.0)), (1.0, 1.0))
    with pytest.raises(VolatilityParityError, match="variance and sigma"):
        validate_covariance(((1.0, 0.0), (0.0, 1.0)), (1.1, 1.0))


def test_trial_specific_lookback_matrix() -> None:
    for trial in TRIALS:
        required = 60 if trial.equal_weight else trial.lookback
        bounded_inverse_volatility(_matrix(required), trial)
        with pytest.raises(VolatilityParityError, match="exact trial lookback"):
            bounded_inverse_volatility(_matrix(required - 1), trial)
    returns: list[tuple[float, float] | None] = [None, *_matrix(90)]
    assert len(contiguous_return_window(returns, 90, 90)) == 90
    returns[40] = None
    with pytest.raises(VolatilityParityError, match="not contiguous"):
        contiguous_return_window(returns, 90, 90)


def test_scheduled_numerical_failure_terminal() -> None:
    zero = tuple((0.0, 0.0) for _ in range(60))
    with pytest.raises(VolatilityParityError, match="scale or marginal"):
        bounded_inverse_volatility(zero)
    # No function returns a prior target, cash substitute, or skip sentinel.


def test_information_time_includes_event_and_availability() -> None:
    start = datetime(2025, 1, 5, tzinfo=UTC)
    late = MinuteBar(
        start + timedelta(hours=12),
        start + timedelta(days=1, seconds=17),
        1,
        1,
        1,
        1,
        "late",
    )
    estimator = _session(start - timedelta(days=1), rows=(late,))
    target = materialize_target(
        PRIMARY,
        _session(start),
        _matrix(60),
        estimator_sessions=(estimator,),
    )
    assert target.information_time == late.available_timestamp


def test_target_hash_materialized_at_information_time() -> None:
    start = datetime(2025, 1, 5, tzinfo=UTC)
    first = materialize_target(PRIMARY, _session(start), _matrix(60), input_ids=("a",))
    same = materialize_target(PRIMARY, _session(start), _matrix(60), input_ids=("a",))
    changed = materialize_target(PRIMARY, _session(start), _matrix(60), input_ids=("b",))
    assert first.canonical_hash == same.canonical_hash
    assert first.canonical_hash != changed.canonical_hash
    assert len(bytes.fromhex(first.canonical_hash)) == 32


def test_expected_whole_minute_open_strictly_after_information_time() -> None:
    timestamp = datetime(2025, 1, 6, 0, 0, 0, 1, tzinfo=UTC)
    assert expected_whole_minute_open(timestamp) == datetime(2025, 1, 6, 0, 1, tzinfo=UTC)
    whole = datetime(2025, 1, 6, tzinfo=UTC)
    assert expected_whole_minute_open(whole) == whole + timedelta(minutes=1)


def test_missing_exact_base_vector_no_later_substitute() -> None:
    expected = datetime(2025, 1, 6, 0, 1, tzinfo=UTC)
    later = expected + timedelta(minutes=1)
    bar = MinuteBar(later + timedelta(minutes=1), later + timedelta(minutes=1), 2, 2, 2, 2)
    with pytest.raises(VolatilityParityError, match="missing exact"):
        canonical_vector({later: bar}, {later: bar}, expected)


def test_additional_delay_exact_next_session_and_no_supersession() -> None:
    start = datetime(2025, 1, 5, tzinfo=UTC)
    target = _target(start)
    pending = delayed_pending(target, start + timedelta(days=2))
    before = order_lifecycle(
        pending,
        quarantined=False,
        terminal=False,
        exposed=False,
        exact_vector_available=True,
        session=start + timedelta(days=1),
    )
    assert before.pending is pending and not before.executed_pending
    fill = order_lifecycle(
        before.pending,
        quarantined=False,
        terminal=False,
        exposed=False,
        exact_vector_available=True,
        session=start + timedelta(days=2),
    )
    assert fill.executed_pending and fill.pending is None


def test_same_timestamp_event_order_and_pending_cancellation() -> None:
    start = datetime(2025, 1, 5, tzinfo=UTC)
    pending = delayed_pending(_target(start), start + timedelta(days=2))
    event = order_lifecycle(
        pending,
        quarantined=True,
        terminal=False,
        exposed=True,
        exact_vector_available=True,
        session=start + timedelta(days=2),
    )
    assert event.pending is None and event.safety_liquidation
    assert event.event_order == (
        "integrity_detection",
        "mark_existing_units",
        "safety_liquidation",
        "record_state",
    )
    terminal = order_lifecycle(
        pending,
        quarantined=False,
        terminal=True,
        exposed=True,
        exact_vector_available=True,
        session=start + timedelta(days=2),
    )
    assert terminal.pending is None and terminal.terminal


def test_reported_turnover_vs_costed_risky_fraction() -> None:
    prior = {SYMBOLS[0]: 0.5, SYMBOLS[1]: 0.5, CASH: 0.0}
    target = {SYMBOLS[0]: 0.6, SYMBOLS[1]: 0.4, CASH: 0.0}
    trade = execute_trade(100, prior, target, BASE_COST_RATE, datetime(2025, 1, 1, tzinfo=UTC))
    assert trade.turnover == pytest.approx(0.1)
    assert trade.gross_risky_fraction == pytest.approx(0.2)


def test_cash_entry_exit_and_risky_rotation_costs() -> None:
    cash = {SYMBOLS[0]: 0.0, SYMBOLS[1]: 0.0, CASH: 1.0}
    risky = {SYMBOLS[0]: 0.5, SYMBOLS[1]: 0.5, CASH: 0.0}
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    entry = execute_trade(100, cash, risky, BASE_COST_RATE, timestamp)
    exit_trade = execute_trade(100, risky, cash, BASE_COST_RATE, timestamp)
    rotation = execute_trade(
        100,
        risky,
        {SYMBOLS[0]: 0.6, SYMBOLS[1]: 0.4, CASH: 0.0},
        BASE_COST_RATE,
        timestamp,
    )
    assert entry.gross_risky_fraction == exit_trade.gross_risky_fraction == 1
    assert entry.cost == exit_trade.cost == pytest.approx(0.14)
    assert rotation.cost == pytest.approx(0.028)


def test_asset_contribution_currency_reconciliation() -> None:
    prices = {SYMBOLS[0]: 100.0, SYMBOLS[1]: 50.0}
    target = {SYMBOLS[0]: 0.5, SYMBOLS[1]: 0.5, CASH: 0.0}
    account, _ = trade_account(
        initial_account(100.0), prices, target, BASE_COST_RATE, datetime(2025, 1, 1, tzinfo=UTC)
    )
    marked, wealth = mark_account(account, {SYMBOLS[0]: 110.0, SYMBOLS[1]: 55.0})
    terminal, _ = trade_account(
        marked,
        {SYMBOLS[0]: 110.0, SYMBOLS[1]: 55.0},
        {SYMBOLS[0]: 0.0, SYMBOLS[1]: 0.0, CASH: 1.0},
        BASE_COST_RATE,
        datetime(2025, 1, 2, tzinfo=UTC),
    )
    assert wealth > 100
    reconcile_contributions(
        100.0,
        terminal.cash,
        terminal.contributions[SYMBOLS[0]],
        terminal.contributions[SYMBOLS[1]],
    )
    with pytest.raises(VolatilityParityError, match="reconciliation"):
        reconcile_contributions(100.0, terminal.cash, 0.0, 0.0)


def test_stationary_bootstrap_restart_and_big_endian_seeds() -> None:
    values = [0.01 if index % 3 else -0.005 for index in range(80)]
    first = stationary_bootstrap(values, 10, resamples=50)
    second = stationary_bootstrap(values, 10, resamples=50)
    expected = int.from_bytes(
        hashlib.sha256(f"{EXPERIMENT_ID}|stationary-bootstrap|10".encode()).digest()[:8],
        "big",
        signed=False,
    )
    assert first == second and first["seed"] == expected


def test_DSR_35_attempt_registry_no_calendar_imputation() -> None:
    primary = [0.01 * math.sin(index) + 0.001 for index in range(365)]
    result = deflated_sharpe(primary, [index / 100 for index in range(21)], [0.1] * 7)
    assert result["N"] == 35
    assert result["sigma_SR"] > 0
    missing_prior = deflated_sharpe(primary, [0.1] * 20, [0.1] * 7)
    assert missing_prior["probability"] == 0


def test_DSR_Bartlett_T_eff_and_degeneracies() -> None:
    primary = [0.01 * math.sin(index * 1.7) + 0.001 for index in range(365)]
    result = deflated_sharpe(
        primary,
        [index / 100 for index in range(21)],
        [0.01 * index for index in range(7)],
    )
    assert result["VIF"] >= 1 and result["T_eff"] <= len(primary)
    assert deflated_sharpe([0.0] * 365, [0.1] * 21, [0.2] * 7)["probability"] == 0


def test_PBO_common_matrix_ties_infinities_and_degeneracies() -> None:
    values = [[0.01 * math.sin(index + trial) for index in range(80)] for trial in range(7)]
    assert 0 <= cscv_pbo(values) <= 1
    ties = [[0.01] * 80 for _ in range(7)]
    assert cscv_pbo(ties) == 1
    assert cscv_pbo(values[:6]) == 1
    corrupted = [*values]
    corrupted[0] = [*corrupted[0][:-1], math.nan]
    assert cscv_pbo(corrupted) == 1


def test_regime_prior_only_median_gap_reset_assignment_and_rebalance_minimum() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    sessions = [
        _session(
            start + timedelta(days=index),
            close=100 + index + 2 * math.sin(index),
        )
        for index in range(310)
    ]
    labels = regime_labels(sessions)
    assert labels[0] is None and any(label is not None for label in labels[250:])
    sessions[260] = _session(sessions[260].start, complete=False)
    reset = regime_labels(sessions)
    assert reset[260] is None and all(label is None for label in reset[261:])
    passed = regime_gate(
        {name: [0.001] * 45 for name in ("up_high", "up_low", "down_low")},
        {name: 5 for name in ("up_high", "up_low", "down_low")},
    )
    assert passed["pass"] is True


def test_event_level_drawdown_path() -> None:
    assert event_drawdown([100.0, 110.0, 80.0, 120.0]) == pytest.approx(1 - 80 / 110)
    with pytest.raises(VolatilityParityError):
        event_drawdown([100.0, math.nan])


def test_exceptional_profit_currency_denominator() -> None:
    assert exceptional_profit([4.0, 3.0, 2.0, 1.0])["largest"] == pytest.approx(0.4)
    assert exceptional_profit([-1.0, 0.0])["pass"] is False
    assert exceptional_profit([math.nan])["pass"] is False


def test_development_rebalance_minima() -> None:
    assert rebalance_minima(40, [8, 8, 8, 8])
    assert not rebalance_minima(39, [8, 8, 8, 8])
    assert rebalance_minima(20, [8, 8], holdout=True)


def test_holdout_authorization_and_irreversible_fsynced_latch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "HOLDOUT_ACCESS_LATCH.json"
    hashes = {"source": "a" * 64, "audit": "b" * 64}
    calls = 0
    original_fsync = __import__("os").fsync

    def counted_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        original_fsync(descriptor)

    monkeypatch.setattr("strategy_control.volatility_parity.os.fsync", counted_fsync)
    create_holdout_latch(path, {"invocation_id": "one", "authorization_hashes": hashes})
    arm_holdout_latch(path, hashes, first_access_at_utc="2026-08-03T00:00:00Z")
    stored = json.loads(path.read_text())
    assert stored["accessed"] is True and calls >= 4
    with pytest.raises(VolatilityParityError, match="already armed"):
        arm_holdout_latch(path, hashes)
    with pytest.raises(VolatilityParityError, match="pre-existing"):
        create_holdout_latch(path, {"invocation_id": "two", "authorization_hashes": hashes})


def test_prospective_Sunday_key_deduplication_and_postfreeze_warmup() -> None:
    session_end = datetime(2027, 1, 4, tzinfo=UTC)
    key = prospective_decision_key(session_end)
    assert deduplicate_prospective_keys((key, key, key + "-next")) == (key, key + "-next")
    assert key.startswith(EXPERIMENT_ID)


def test_recovery_requires_exact_150_completed_joint_sessions() -> None:
    start = datetime(2024, 8, 13, tzinfo=UTC)
    sessions = [_session(start + timedelta(days=index)) for index in range(150)]
    eligibility = recovery_eligible(sessions)
    assert not any(eligibility[:149]) and eligibility[149]
    sessions[100] = _session(sessions[100].start, complete=False)
    assert not recovery_eligible(sessions)[-1]
