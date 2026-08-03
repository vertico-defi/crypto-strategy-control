from __future__ import annotations

import hashlib
import itertools
import math
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from strategy_control.volatility_managed import (
    BASE_COST,
    EXPERIMENT_ID,
    FILL_KEYS,
    FIXED_LATCH_PATH,
    TARGET_KEYS,
    PathState,
    VolatilityManagedError,
    assign_regime_currency_pnl,
    baseline_all_six,
    cancel_pending,
    canonical_hash,
    common_consecutive_endpoints,
    consecutive_returns,
    deflated_sharpe,
    event_drawdown,
    exact_vector,
    expected_grid,
    fold_state,
    fresh_path,
    holdout_minima,
    joint_grid_status,
    make_fill,
    make_target,
    pbo,
    prospective_keys,
    reconcile_contributions,
    regime_labels,
    safety_vector,
    session_manifest,
    sleeve_scalar,
    stationary_bootstrap,
    strict_prefix,
    trade_metrics,
    validate_allowlist,
    validate_latch,
    validate_latch_creation,
    validate_registry,
    verify_opened_bytes,
)


def dt(day: int = 1, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2025, 1, day, hour, minute, tzinfo=UTC)


def allowlist() -> list[dict[str, Any]]:
    months = [
        f"{year}-{month:02d}"
        for year, start, stop in ((2024, 7, 13), (2025, 1, 13))
        for month in range(start, stop)
    ]
    return [
        {
            "bytes": index + 1,
            "month": month,
            "relative_path": f"canonical/{symbol}/{month}",
            "sha256": hashlib.sha256(f"{symbol}|{month}".encode()).hexdigest(),
            "symbol": symbol,
        }
        for symbol in ("BTCUSDT", "ETHUSDT")
        for index, month in enumerate(months)
    ]


def target_record(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {key: 0 for key in TARGET_KEYS}
    values.update(
        {
            "schema_version": "1",
            "experiment_id": EXPERIMENT_ID,
            "path_kind": "base",
            "trial_or_benchmark": "primary",
            "boundary_start": "a",
            "boundary_end": "b",
            "decision_session_end": "c",
            "I_s": "d",
            "B_s": "e",
            "lookback": 60,
            "target_volatility": 0.4,
            "sigma_hat": 0.5,
            "cap_state": "uncapped",
            "risky_scalar": 0.8,
            "weights_BTC_ETH_cash": [0.4, 0.4, 0.2],
            "ordered_source_record_ids": [],
            "ordered_source_record_hashes": [],
            "session_input_manifest_sha256": "a" * 64,
            "status": "materialized",
        }
    )
    values.update(changes)
    return values


def fill_record(parent: str, **changes: object) -> dict[str, object]:
    values: dict[str, object] = {key: 0 for key in FILL_KEYS}
    values.update(
        {
            "schema_version": "1",
            "experiment_id": EXPERIMENT_ID,
            "path_kind": "base",
            "trial_or_benchmark": "primary",
            "boundary_start": "a",
            "boundary_end": "b",
            "decision_session_end": "c",
            "B_s": "e",
            "execution_event_timestamp": "f",
            "execution_available_timestamp": "f",
            "BTC_row_sha256": "a" * 64,
            "ETH_row_sha256": "b" * 64,
            "execution_vector_sha256": "c" * 64,
            "parent_target_sha256": parent,
            "pretrade_weights_BTC_ETH_cash": [0, 0, 1],
            "target_weights_BTC_ETH_cash": [0.4, 0.4, 0.2],
            "cost_rate": BASE_COST,
            "currency_cost": 0.1,
            "turnover": 0.4,
            "gross_risky_trade": 0.8,
            "status": "filled",
            "cancellation_reason": None,
        }
    )
    values.update(changes)
    return values


def manifest_row(identifier: str) -> dict[str, str]:
    return {
        "relative_path": "canonical/a",
        "file_sha256": "a" * 64,
        "row_identifier": identifier,
        "event_timestamp": f"event-{identifier}",
        "available_timestamp": f"available-{identifier}",
        "row_hash": hashlib.sha256(identifier.encode()).hexdigest(),
    }


def bootstrap_reference(
    values: list[float], block: int, resamples: int
) -> tuple[int, float, float]:
    seed = int.from_bytes(
        hashlib.sha256(f"{EXPERIMENT_ID}|stationary-bootstrap|{block}".encode()).digest()[:8],
        "big",
    )
    rng = random.Random(seed)
    means: list[float] = []
    for _ in range(resamples):
        index = rng.randrange(len(values))
        sample = [values[index]]
        for _ in range(1, len(values)):
            index = (
                rng.randrange(len(values))
                if rng.random() < 1 / block
                else (index + 1) % len(values)
            )
            sample.append(values[index])
        means.append(sum(sample) / len(sample))
    means.sort()

    def percentile(q: float) -> float:
        rank = q * (len(means) - 1)
        lower, upper = math.floor(rank), math.ceil(rank)
        return means[lower] + (means[upper] - means[lower]) * (rank - lower)

    return seed, percentile(0.025), percentile(0.975)


def test_source_commit_and_allowlist_hash_mismatch_rejected_before_path_resolution() -> None:
    entries = allowlist()
    expected_hash = canonical_hash(entries)
    path_resolved = False
    with pytest.raises(VolatilityManagedError):
        validate_allowlist("wrong", "frozen", entries, expected_hash)
    assert path_resolved is False
    with pytest.raises(VolatilityManagedError):
        validate_allowlist("frozen", "frozen", entries, "0" * 64)
    validate_allowlist("frozen", "frozen", entries, expected_hash)


def test_opened_byte_count_and_sha256_reverified_before_parse() -> None:
    raw = b"harmless-synthetic-bytes"
    verify_opened_bytes(raw, len(raw), hashlib.sha256(raw).hexdigest())
    with pytest.raises(VolatilityManagedError):
        verify_opened_bytes(raw, len(raw) + 1, hashlib.sha256(raw).hexdigest())
    with pytest.raises(VolatilityManagedError):
        verify_opened_bytes(raw, len(raw), "0" * 64)


def test_session_input_manifest_hash_canonical_and_ordered() -> None:
    rows = [manifest_row("1"), manifest_row("2")]
    reordered_keys = [dict(reversed(list(row.items()))) for row in rows]
    assert session_manifest(rows) == session_manifest(reordered_keys)
    assert session_manifest(rows) != session_manifest(list(reversed(rows)))


def test_future_rows_cannot_change_prior_target_or_fold() -> None:
    end = dt(3)
    prefix = [(dt(1), 1), (dt(2), 2)]
    first = strict_prefix(prefix, end)
    second = strict_prefix([*prefix, (dt(3), 999), (dt(4), -999)], end)
    assert first == second
    assert make_target(target_record()).sha256 == make_target(target_record()).sha256
    assert fold_state(end, [dt(1), dt(2)]) == fold_state(end, [dt(1), dt(2)])


def test_target_hash_distinguishes_trial_benchmark_boundary_and_weights() -> None:
    base = make_target(target_record()).sha256
    variants = {
        make_target(target_record(trial_or_benchmark="weekly_benchmark")).sha256,
        make_target(target_record(boundary_end="other")).sha256,
        make_target(target_record(weights_BTC_ETH_cash=[0.3, 0.3, 0.4])).sha256,
    }
    assert base not in variants and len(variants) == 3


def test_fill_rejects_parent_target_hash_mismatch() -> None:
    target = make_target(target_record())
    fill = make_fill(fill_record(target.sha256), target)
    assert fill.record["parent_target_sha256"] == target.sha256
    with pytest.raises(VolatilityManagedError):
        make_fill(fill_record("0" * 64), target)


def test_expected_grid_detects_jointly_absent_session_causally() -> None:
    boundary = dt(2)
    grid = expected_grid(dt())
    assert len(grid) == 1440 and grid[0] == dt(1, 0, 1) and grid[-1] == boundary
    status = joint_grid_status(dt(), (), (), boundary)
    assert not status.complete and len(status.triggers) == 2880
    assert {event.detected_at for event in status.triggers} == {boundary}


def test_duplicate_nonmonotonic_invalid_asynchronous_rows_trigger_quarantine() -> None:
    grid = list(expected_grid(dt()))
    btc = grid.copy()
    btc[5] = btc[4]
    eth = grid.copy()
    eth[9] = eth[9] + timedelta(minutes=1)
    valid = [True] * len(grid)
    valid[20] = False
    available = [value + timedelta(seconds=5) for value in btc]
    status = joint_grid_status(
        dt(),
        btc,
        eth,
        dt(2),
        btc_available=available,
        btc_valid=valid,
    )
    names = {event.trigger for event in status.triggers}
    assert not status.complete
    assert {"BTCUSDT_duplicate", "BTCUSDT_nonmonotonic", "BTCUSDT_invalid"} <= names
    assert "asynchronous_joint_row" in names


def test_missing_exact_B_s_never_scans_for_ordinary_fill() -> None:
    with pytest.raises(VolatilityManagedError, match="never scan"):
        exact_vector(dt(1, 0, 1), {dt(1, 0, 2): 100}, {dt(1, 0, 2): 10})


def test_exposed_quarantine_uses_earliest_later_valid_safety_vector() -> None:
    trigger = dt(1, 0, 1)
    later = safety_vector(
        trigger,
        [
            (dt(1, 0, 1), dt(1, 0, 3), 1, 1),
            (dt(1, 0, 2), dt(1, 0, 1), 2, 2),
            (dt(1, 0, 3), dt(1, 0, 4), 3, 4),
            (dt(1, 0, 4), dt(1, 0, 5), 5, 6),
        ],
    )
    assert later == (dt(1, 0, 3), dt(1, 0, 4), 3.0, 4.0)


def test_unpriceable_exposure_before_terminal_fails_data_integrity() -> None:
    with pytest.raises(VolatilityManagedError, match="unpriceable"):
        safety_vector(dt(1, 0, 5), [(dt(1, 0, 4), dt(1, 0, 5), 1, 1)])


def test_scheduled_zero_or_nonfinite_volatility_is_terminal_numerical_failure() -> None:
    with pytest.raises(VolatilityManagedError, match="volatility"):
        sleeve_scalar([(0.0, 0.0)] * 60)
    invalid = [(0.01, 0.01)] * 59 + [(math.nan, 0.01)]
    with pytest.raises(VolatilityManagedError):
        sleeve_scalar(invalid)


def test_base_doubled_delay_fold_trial_benchmark_paths_are_state_independent() -> None:
    paths = [fresh_path() for _ in range(12)]
    assert len({id(path) for path in paths}) == 12
    assert all(path == PathState() for path in paths)


def test_delay_preserves_exact_target_hash_and_is_cancelled_by_quarantine() -> None:
    target = make_target(target_record())
    pending = PathState(pending_target_hash=target.sha256)
    assert pending.pending_target_hash == target.sha256
    cancelled = cancel_pending(pending, quarantine=True)
    assert cancelled.pending_target_hash is None and cancelled.quarantined


def test_fold_starts_cash_with_warmup_only_and_no_prior_target_or_pnl() -> None:
    state = fold_state(dt(3), [dt(1), dt(2)])
    assert state.wealth == 1 and state.weights == (0, 0, 1)
    assert state.pending_target_hash is None and state.pnl == (0, 0)
    with pytest.raises(VolatilityManagedError):
        fold_state(dt(3), [dt(3)])


def test_daily_panel_requires_exact_seven_trial_intersection_and_consecutive_days() -> None:
    panels = [{dt(1): 1.0, dt(2): 1.1, dt(3): 1.21} for _ in range(7)]
    panels[-1].pop(dt(2))
    endpoints = common_consecutive_endpoints(panels)
    assert endpoints == (dt(1), dt(3)) and consecutive_returns(panels[0], endpoints) == ()
    with pytest.raises(VolatilityManagedError):
        common_consecutive_endpoints(panels[:6])


def test_genuine_cash_zero_distinguished_from_unavailable_span() -> None:
    cash = {dt(1): 1.0, dt(2): 1.0, dt(3): 1.0}
    complete = common_consecutive_endpoints([cash] * 7)
    assert consecutive_returns(cash, complete) == (0.0, 0.0)
    unavailable = [dict(cash) for _ in range(7)]
    unavailable[-1].pop(dt(2))
    assert consecutive_returns(cash, common_consecutive_endpoints(unavailable)) == ()


def test_baseline_all_six_conditions_strict_and_ties_fail() -> None:
    primary = {"net": 0.10, "sharpe": 1.0, "drawdown": 0.10}
    weekly = {"net": 0.05, "sharpe": 0.8, "drawdown": 0.20}
    buy_hold = {"net": 0.04, "sharpe": 0.7, "drawdown": 0.30}
    assert baseline_all_six(primary, weekly, buy_hold)
    for key in ("net", "sharpe", "drawdown"):
        tied = dict(weekly)
        tied[key] = primary[key]
        assert not baseline_all_six(primary, tied, buy_hold)


def test_minute_drawdown_retains_marks_costs_safety_and_terminal_events() -> None:
    event_wealth = [1.0, 1.2, 1.19, 0.6, 0.59, 0.8]
    assert event_drawdown(event_wealth) == pytest.approx(1 - 0.59 / 1.2)


def test_terminal_vector_exact_missing_vector_fails_without_fallback() -> None:
    terminal = dt(1, 23, 59)
    with pytest.raises(VolatilityManagedError):
        exact_vector(terminal, {dt(1, 23, 58): 1}, {dt(1, 23, 58): 1})


def test_costed_gross_risky_trade_and_reported_turnover_are_distinct() -> None:
    metrics = trade_metrics((0.5, 0.5, 0), (0.6, 0.4, 0), 100, BASE_COST)
    assert metrics["gross_risky_trade"] == pytest.approx(0.2)
    assert metrics["turnover"] == pytest.approx(0.1)
    assert metrics["currency_cost"] == pytest.approx(100 * BASE_COST * 0.2)
    with pytest.raises(VolatilityManagedError):
        trade_metrics((math.nan, 0.5, 0.5), (0.6, 0.4, 0), 100, BASE_COST)


def test_asset_contribution_reconciles_currency_wealth() -> None:
    reconcile_contributions(1.0, 1.1, 0.04, 0.06)
    with pytest.raises(VolatilityManagedError):
        reconcile_contributions(1.0, 1.1, 0.04, 0.05)


def test_42_attempt_DSR_registry_has_35_observed_and_7_unimputed_slots() -> None:
    observed = [index / 100 for index in range(35)]
    assert validate_registry(observed[:28], observed[28:], calendar_slots=7) == tuple(observed)
    with pytest.raises(VolatilityManagedError):
        validate_registry(observed[:28], [*observed[28:], 0.0], calendar_slots=7)
    with pytest.raises(VolatilityManagedError):
        validate_registry(observed[:28], observed[28:], calendar_slots=6)


def test_DSR_probability_zero_on_every_declared_degeneracy() -> None:
    registry = [index / 100 for index in range(35)]
    assert deflated_sharpe([0.0] * 40, registry) == 0.0
    assert deflated_sharpe([0.01, -0.01], registry) == 0.0
    assert deflated_sharpe([0.01, -0.01] * 40, registry[:-1]) == 0.0
    assert deflated_sharpe([0.01, -0.01] * 40, [0.1] * 35) == 0.0
    assert deflated_sharpe([math.nan] * 40, registry) == 0.0


def test_PBO_all_70_splits_rank_over_8_and_fail_closed() -> None:
    assert len(list(itertools.combinations(range(8), 4))) == 70
    matrix = [[0.01 * math.sin(index + trial) for index in range(83)] for trial in range(7)]
    estimate = pbo(matrix)
    assert 0.0 <= estimate <= 1.0
    assert pbo(matrix[:6]) == 1.0
    assert pbo([row[:7] for row in matrix]) == 1.0
    malformed = [row.copy() for row in matrix]
    malformed[0][0] = math.nan
    assert pbo(malformed) == 1.0


def test_bootstrap_all_three_exact_seeds_restart_and_linear_percentiles() -> None:
    values = [0.01 if index % 3 else -0.004 for index in range(83)]
    for block in (10, 20, 40):
        expected_seed, expected_lower, expected_upper = bootstrap_reference(values, block, 200)
        actual = stationary_bootstrap(values, block, 200)
        assert actual == {
            "seed": expected_seed,
            "lower": expected_lower,
            "upper": expected_upper,
        }


def test_regime_prior_only_equalities_gap_reset_and_cost_assignment() -> None:
    closes = [100.0] * 250
    labels = regime_labels(closes)
    assert labels[-1] == "down_low"
    changed_future = regime_labels(closes[:200] + [200.0] * 50)
    assert changed_future[:200] == labels[:200]
    reset = regime_labels([*closes[:200], None, *closes[201:]])
    assert all(label is None for label in reset[200:])
    totals = assign_regime_currency_pnl(
        ["down_low", "down_low"], [1.0, 2.0], [0.1, 0.2], [-0.3, 0.4]
    )
    assert totals == {"down_low": pytest.approx(2.8)}


def test_holdout_minima_are_150_days_20_total_8_per_fold_2_of_2() -> None:
    assert holdout_minima(holdout=True) == {
        "days": 150,
        "total_rebalances": 20,
        "per_fold": 8,
        "positive_folds": 2,
    }


def test_fixed_latch_preexistence_mismatch_or_post_arm_error_is_terminal() -> None:
    authorization = {"source": "a" * 64}
    latch = {
        "schema_version": "1.0",
        "experiment_id": EXPERIMENT_ID,
        "authorization_hashes": authorization,
        "accessed": True,
        "first_access_at_utc": "2026-01-01T00:00:00Z",
    }
    validate_latch_creation(path_exists=False, path=FIXED_LATCH_PATH)
    validate_latch(latch, authorization)
    with pytest.raises(VolatilityManagedError):
        validate_latch_creation(path_exists=True)
    with pytest.raises(VolatilityManagedError):
        validate_latch(latch, {"source": "b" * 64})
    with pytest.raises(VolatilityManagedError):
        validate_latch(latch, authorization, post_arm_error=True)


def test_prospective_new_sunday_counts_even_if_weights_repeat_but_duplicate_key_does_not() -> None:
    first = datetime(2025, 1, 5, tzinfo=UTC)
    second = datetime(2025, 1, 12, tzinfo=UTC)
    keys = prospective_keys([first, first, second])
    assert len(keys) == 2 and keys[0] != keys[1]
