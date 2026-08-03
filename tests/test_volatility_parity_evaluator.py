from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from strategy_control.volatility_parity import (
    BASE_COST_RATE,
    CASH,
    PRIMARY,
    SYMBOLS,
    TRIALS,
    Session,
    canonical_hash,
    paired_returns,
)
from strategy_control.volatility_parity_evaluator import (
    OBSERVATION_START,
    DevelopmentMarket,
    JointVector,
    PlannedFill,
    _planned_trial_fills,
    build_trial_targets,
    evaluate_development,
    simulate_path,
)
from strategy_control.volatility_parity_pipeline import DEVELOPMENT_END, DEVELOPMENT_START

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT = ROOT / "experiments" / "btc-eth-causal-volatility-parity-rebalancing-v1"


def _synthetic_market() -> DevelopmentMarket:
    count = (DEVELOPMENT_END - OBSERVATION_START).days
    btc_close = 100.0
    eth_close = 50.0
    sessions: dict[str, list[Session]] = {symbol: [] for symbol in SYMBOLS}
    vectors: dict[datetime, JointVector] = {}
    for index in range(count):
        start = OBSERVATION_START + timedelta(days=index)
        common = 0.001 + 0.004 * math.sin(index / 5)
        btc_close *= math.exp(common + 0.0008 * math.cos(index / 11))
        eth_close *= math.exp(1.3 * common + 0.0006 * math.sin(index / 13))
        closes = {SYMBOLS[0]: btc_close, SYMBOLS[1]: eth_close}
        for symbol in SYMBOLS:
            sessions[symbol].append(
                Session(
                    start=start,
                    available_timestamp=start + timedelta(days=1),
                    open=closes[symbol],
                    high=closes[symbol],
                    low=closes[symbol],
                    close=closes[symbol],
                    complete=True,
                    input_hash=canonical_hash({"synthetic": symbol, "session": start.isoformat()}),
                )
            )
        endpoint = start + timedelta(hours=23, minutes=59)
        next_open = start + timedelta(days=1, minutes=1)
        for timestamp in (endpoint, next_open):
            day_index = min(index, count - 1)
            scale = 1 + 0.0001 * (timestamp.hour + timestamp.minute / 60)
            prices = {
                SYMBOLS[0]: sessions[SYMBOLS[0]][day_index].close * scale,
                SYMBOLS[1]: sessions[SYMBOLS[1]][day_index].close * scale,
            }
            vectors[timestamp] = JointVector(timestamp, timestamp, prices)
    aligned = {symbol: tuple(values) for symbol, values in sessions.items()}
    vector_times = tuple(sorted(vectors))
    return DevelopmentMarket(
        sessions=aligned,
        returns=paired_returns(aligned[SYMBOLS[0]], aligned[SYMBOLS[1]]),
        vectors=vectors,
        vector_times=vector_times,
        gap_detection_times=(),
        source_partition_count=36,
        holdout_values_read=False,
    )


def _contract() -> dict[str, object]:
    return json.loads((EXPERIMENT / "PREREGISTRATION_REVISED_DRAFT.json").read_text())


def test_target_builder_uses_frozen_sunday_recovery_and_lookback() -> None:
    market = _synthetic_market()
    targets = build_trial_targets(market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END)
    assert 40 <= len(targets) <= 53
    assert all(target.signal_session_end.weekday() == 0 for target in targets)
    assert all(target.expected_open.minute == 1 for target in targets)
    assert all(len(target.input_ids) == 122 for target in targets)


def test_delayed_targets_keep_hash_and_move_to_next_completed_session() -> None:
    market = _synthetic_market()
    base = build_trial_targets(market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END)
    delayed = build_trial_targets(market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END, delayed=True)
    assert len(base) == len(delayed)
    for ordinary, stressed in zip(base, delayed, strict=True):
        assert ordinary.canonical_hash == stressed.canonical_hash
        assert stressed.expected_open == ordinary.expected_open + timedelta(days=1)


def test_simulator_is_terminal_cash_and_self_financing() -> None:
    market = _synthetic_market()
    targets = build_trial_targets(market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END)
    result = simulate_path(
        market,
        PRIMARY.name,
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        _planned_trial_fills(targets),
        cost_rate=BASE_COST_RATE,
    )
    assert result.terminal_cash
    assert result.completed_rebalances == len(targets)
    assert len(result.daily_returns) == 364
    assert result.event_observations > len(result.daily_returns)
    assert sum(result.asset_contributions.values()) == pytest.approx(result.net_return)


def test_buy_and_hold_entry_only_does_not_weekly_rebalance() -> None:
    market = _synthetic_market()
    targets = build_trial_targets(market, PRIMARY, DEVELOPMENT_START, DEVELOPMENT_END)
    fills = tuple(
        PlannedFill(
            timestamp=target.expected_open,
            target_hash=target.canonical_hash,
            signal_session_end=target.signal_session_end,
            weights={SYMBOLS[0]: 0.5, SYMBOLS[1]: 0.5, CASH: 0.0},
            entry_only=True,
        )
        for target in targets
    )
    result = simulate_path(
        market,
        "equal_weight_buy_and_hold",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        fills,
        cost_rate=BASE_COST_RATE,
    )
    assert result.completed_rebalances == 1
    assert result.terminal_cash


def test_full_development_report_is_closed_holdout_and_complete() -> None:
    market = _synthetic_market()
    report = evaluate_development(market, _contract(), ROOT / "experiments")
    assert report["classification"] in {"DEVELOPMENT_GO", "HISTORICAL_NO_GO"}
    assert len(report["trials"]) == len(TRIALS)
    assert len(report["folds"]) == 4
    assert len(report["gate_checks"]) == 25
    assert report["common_panel_days"] == 365
    assert report["source_partition_count"] == 36
    assert report["holdout_opened"] is False
    assert report["holdout_values_read"] is False
    assert report["candidate_promoted"] is False
    assert report["capital_permitted"] == 0


def test_fold_target_prefix_isolation_ignores_later_sessions() -> None:
    market = _synthetic_market()
    fold_end = datetime(2025, 4, 1, tzinfo=DEVELOPMENT_START.tzinfo)
    before = build_trial_targets(market, PRIMARY, DEVELOPMENT_START, fold_end)
    changed_sessions = {symbol: list(values) for symbol, values in market.sessions.items()}
    future_index = next(
        index
        for index, session in enumerate(changed_sessions[SYMBOLS[0]])
        if session.start >= fold_end
    )
    for symbol in SYMBOLS:
        original = changed_sessions[symbol][future_index]
        changed_sessions[symbol][future_index] = replace(
            original,
            close=original.close * 100,
            input_hash="future-change",
        )
    aligned = {symbol: tuple(values) for symbol, values in changed_sessions.items()}
    mutated = replace(
        market,
        sessions=aligned,
        returns=paired_returns(aligned[SYMBOLS[0]], aligned[SYMBOLS[1]]),
    )
    after = build_trial_targets(mutated, PRIMARY, DEVELOPMENT_START, fold_end)
    assert tuple(target.canonical_hash for target in before) == tuple(
        target.canonical_hash for target in after
    )


def test_terminal_vector_is_exact_and_missing_terminal_fails() -> None:
    market = _synthetic_market()
    end = datetime(2025, 4, 1, tzinfo=DEVELOPMENT_START.tzinfo)
    target = build_trial_targets(market, PRIMARY, DEVELOPMENT_START, end)
    missing = dict(market.vectors)
    missing.pop(end - timedelta(minutes=1))
    broken = replace(market, vectors=missing)
    with pytest.raises(Exception, match="terminal"):
        simulate_path(
            broken,
            "terminal-proof",
            DEVELOPMENT_START,
            end,
            _planned_trial_fills(target),
            cost_rate=BASE_COST_RATE,
        )


def test_quarantine_cancels_older_pending_target_without_supersession() -> None:
    start = datetime(2025, 1, 1, tzinfo=DEVELOPMENT_START.tzinfo)
    end = start + timedelta(days=4)
    timestamps = [
        start + timedelta(hours=12),
        start.replace(hour=23, minute=59),
        start + timedelta(days=1, minutes=1),
        (start + timedelta(days=1)).replace(hour=23, minute=59),
        start + timedelta(days=2, minutes=1),
        (start + timedelta(days=2)).replace(hour=23, minute=59),
        (start + timedelta(days=3)).replace(hour=23, minute=59),
    ]
    vectors = {
        timestamp: JointVector(
            timestamp,
            timestamp,
            {
                SYMBOLS[0]: 100 + index * 3,
                SYMBOLS[1]: 50 + index,
            },
        )
        for index, timestamp in enumerate(timestamps)
    }
    market = DevelopmentMarket(
        sessions={symbol: () for symbol in SYMBOLS},
        returns=(),
        vectors=vectors,
        vector_times=tuple(sorted(vectors)),
        gap_detection_times=(start + timedelta(hours=12),),
        source_partition_count=36,
    )
    fills = (
        PlannedFill(
            start + timedelta(days=1, minutes=1),
            "old",
            start + timedelta(hours=1),
            {SYMBOLS[0]: 0.5, SYMBOLS[1]: 0.5, CASH: 0.0},
        ),
        PlannedFill(
            start + timedelta(days=2, minutes=1),
            "new-after-recovery-proof",
            start + timedelta(days=2),
            {SYMBOLS[0]: 0.5, SYMBOLS[1]: 0.5, CASH: 0.0},
        ),
    )
    result = simulate_path(market, "pending-proof", start, end, fills, cost_rate=BASE_COST_RATE)
    assert result.completed_rebalances == 1
    assert result.completed_fill_timestamps == (start + timedelta(days=2, minutes=1),)


def test_event_drawdown_retains_intraday_marks() -> None:
    start = datetime(2025, 1, 1, tzinfo=DEVELOPMENT_START.tzinfo)
    end = start + timedelta(days=4)
    price_points = (
        (start, 100.0),
        (start.replace(hour=23, minute=59), 102.0),
        (start + timedelta(days=1, hours=12), 50.0),
        ((start + timedelta(days=1)).replace(hour=23, minute=59), 108.0),
        ((start + timedelta(days=2)).replace(hour=23, minute=59), 103.0),
        ((start + timedelta(days=3)).replace(hour=23, minute=59), 115.0),
    )
    vectors = {
        timestamp: JointVector(
            timestamp,
            timestamp,
            {SYMBOLS[0]: price, SYMBOLS[1]: price},
        )
        for timestamp, price in price_points
    }
    market = DevelopmentMarket(
        sessions={symbol: () for symbol in SYMBOLS},
        returns=(),
        vectors=vectors,
        vector_times=tuple(sorted(vectors)),
        gap_detection_times=(),
        source_partition_count=36,
    )
    fill = PlannedFill(
        start,
        "entry",
        start,
        {SYMBOLS[0]: 0.5, SYMBOLS[1]: 0.5, CASH: 0.0},
    )
    result = simulate_path(market, "event-drawdown-proof", start, end, (fill,), cost_rate=0.0)
    assert result.maximum_drawdown > 0.5
