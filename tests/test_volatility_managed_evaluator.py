from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from strategy_control.volatility_managed import (
    BASE_COST,
    SYMBOLS,
    TRIALS,
    VolatilityManagedError,
    canonical_hash,
)
from strategy_control.volatility_managed_evaluator import (
    ALLOWLIST_SHA256,
    DEVELOPMENT_END,
    DEVELOPMENT_START,
    OBSERVATION_START,
    SOURCE_COMMIT,
    AssetSession,
    DevelopmentMarket,
    JointSession,
    JointVector,
    OpenedPartition,
    _ordinary_fills,
    build_trial_targets,
    delayed_fills,
    evaluate_development,
    load_development_market,
    simulate_path,
    verify_frozen_contract,
)

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT = ROOT / "experiments" / "btc-eth-volatility-managed-equal-weight-v1"
DATA_CONTRACT = ROOT / "experiments" / "btc-eth-vol-targeted-trend-v1" / "DATA_CONTRACT.json"


def _effective() -> dict[str, object]:
    return cast(
        dict[str, object],
        json.loads((EXPERIMENT / "PREREGISTRATION_REVISED_DRAFT.json").read_text()),
    )


def _data_contract() -> dict[str, object]:
    return cast(dict[str, object], json.loads(DATA_CONTRACT.read_text()))


def _vector(timestamp: datetime, btc: float, eth: float) -> JointVector:
    btc_hash = canonical_hash({"BTC": timestamp.isoformat(), "price": btc})
    eth_hash = canonical_hash({"ETH": timestamp.isoformat(), "price": eth})
    payload = {
        "timestamp": timestamp.isoformat(),
        "event_timestamp": (timestamp + timedelta(minutes=1)).isoformat(),
        "available_timestamp": (timestamp + timedelta(minutes=1)).isoformat(),
        "BTC_row_sha256": btc_hash,
        "ETH_row_sha256": eth_hash,
        "prices": [btc, eth],
    }
    return JointVector(
        timestamp,
        timestamp + timedelta(minutes=1),
        timestamp + timedelta(minutes=1),
        {SYMBOLS[0]: btc, SYMBOLS[1]: eth},
        {SYMBOLS[0]: btc_hash, SYMBOLS[1]: eth_hash},
        canonical_hash(payload),
    )


def _synthetic_market() -> DevelopmentMarket:
    sessions: list[JointSession] = []
    paired: list[tuple[float, float] | None] = []
    vectors: dict[datetime, JointVector] = {}
    prior: tuple[float, float] | None = None
    day = OBSERVATION_START
    btc = 100.0
    eth = 50.0
    index = 0
    while day < DEVELOPMENT_END:
        btc *= math.exp(0.0007 + 0.006 * math.sin(index / 7) + 0.002 * math.cos(index / 19))
        eth *= math.exp(0.0005 + 0.007 * math.sin(index / 9) - 0.001 * math.cos(index / 17))
        prices = (btc, eth)
        assets = {}
        for symbol, price in zip(SYMBOLS, prices, strict=True):
            digest = canonical_hash({"symbol": symbol, "session": day.isoformat()})
            assets[symbol] = AssetSession(
                symbol,
                day,
                True,
                day + timedelta(days=1),
                price,
                (f"{symbol}|{day.isoformat()}",),
                (digest,),
                digest,
            )
        sessions.append(JointSession(day, True, assets, ()))
        paired.append(
            None
            if prior is None
            else (prices[0] / prior[0] - 1, prices[1] / prior[1] - 1)
        )
        endpoint = day + timedelta(hours=23, minutes=59)
        vectors[endpoint] = _vector(endpoint, btc, eth)
        next_open = day + timedelta(days=1, minutes=1)
        if next_open < DEVELOPMENT_END:
            vectors[next_open] = _vector(next_open, btc * 1.0001, eth * 1.0001)
        prior = prices
        day += timedelta(days=1)
        index += 1
    opened = tuple(
        OpenedPartition(f"canonical/{index}", 1, f"{index:064x}", SYMBOLS[index // 18], "x")
        for index in range(36)
    )
    return DevelopmentMarket(
        tuple(sessions),
        tuple(paired),
        vectors,
        tuple(sorted(vectors)),
        (),
        opened,
        ALLOWLIST_SHA256,
        SOURCE_COMMIT,
        False,
    )


def test_production_contract_hashes_recompute_before_data_access() -> None:
    wrapper = json.loads((EXPERIMENT / "PREREGISTRATION.json").read_text())
    effective_path = EXPERIMENT / "PREREGISTRATION_REVISED_DRAFT.json"
    effective = json.loads(effective_path.read_text())
    verify_frozen_contract(wrapper, effective, effective_path.read_bytes())


def test_production_loader_rejects_identity_before_opener_and_rehashes_opened_bytes() -> None:
    calls: list[str] = []

    def opener(relative: str) -> bytes:
        calls.append(relative)
        return b"wrong"

    with pytest.raises(VolatilityManagedError):
        load_development_market(
            Path("/unused"),
            _effective(),
            _data_contract(),
            byte_opener=opener,
            source_manifest_bytes=b"wrong",
        )
    assert calls == []
    manifest = (
        ROOT.parent
        / "crypto-direction-lab"
        / "data/frozen/historical-v2-pathc-20260723T175155Z"
        / "DATASET_FREEZE_MANIFEST_historical-v2-pathc-20260723T175155Z.json"
    ).read_bytes()
    with pytest.raises(VolatilityManagedError, match="opened byte"):
        load_development_market(
            Path("/unused"),
            _effective(),
            _data_contract(),
            byte_opener=opener,
            source_manifest_bytes=manifest,
        )
    assert len(calls) == 1 and "2026" not in calls[0]


def test_production_targets_bind_equal_sleeve_trace_and_distinct_context() -> None:
    market = _synthetic_market()
    primary = build_trial_targets(market, TRIALS[0], DEVELOPMENT_START, DEVELOPMENT_END)
    neighbor = build_trial_targets(market, TRIALS[1], DEVELOPMENT_START, DEVELOPMENT_END)
    assert len(primary) >= 40 and len(neighbor) >= 40
    assert len({target.target.sha256 for target in primary}) == len(primary)
    assert primary[0].target.sha256 != neighbor[0].target.sha256
    assert all(target.weights[0] == target.weights[1] for target in primary)
    assert all(sum(target.weights) == pytest.approx(1.0) for target in primary)
    assert all(len(target.target.record["ordered_source_record_ids"]) == 122 for target in primary)


def test_production_delay_preserves_target_hash_and_moves_one_complete_session() -> None:
    market = _synthetic_market()
    primary = build_trial_targets(market, TRIALS[0], DEVELOPMENT_START, DEVELOPMENT_END)
    delayed = delayed_fills(market, primary)
    assert len(delayed) == len(primary)
    for target, fill in zip(primary, delayed, strict=True):
        assert fill.target.target.sha256 == target.target.sha256
        assert fill.timestamp == target.expected_open + timedelta(days=1)


def test_production_path_persists_parent_bound_fill_hashes_and_terminal_cash() -> None:
    market = _synthetic_market()
    targets = build_trial_targets(market, TRIALS[0], DEVELOPMENT_START, DEVELOPMENT_END)
    result = simulate_path(
        market,
        "primary",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        _ordinary_fills(targets),
        cost_rate=BASE_COST,
    )
    assert result.terminal_cash and result.trace_reconciled
    assert result.completed_rebalances == len(result.fill_hashes) == len(targets)
    assert len(set(result.target_hashes)) == len(targets)
    assert sum(result.asset_contributions) == pytest.approx(result.net_return)


def test_production_fold_prefix_ignores_future_session_mutation() -> None:
    market = _synthetic_market()
    fold_end = datetime(2025, 4, 1, tzinfo=UTC)
    before = build_trial_targets(market, TRIALS[0], DEVELOPMENT_START, fold_end)
    sessions = list(market.sessions)
    future = next(index for index, session in enumerate(sessions) if session.start >= fold_end)
    changed_assets = dict(sessions[future].assets)
    btc = changed_assets[SYMBOLS[0]]
    assert btc.close is not None
    changed_assets[SYMBOLS[0]] = replace(btc, close=btc.close * 100)
    sessions[future] = replace(sessions[future], assets=changed_assets)
    mutated = replace(market, sessions=tuple(sessions))
    after = build_trial_targets(mutated, TRIALS[0], DEVELOPMENT_START, fold_end)
    assert [target.target.sha256 for target in before] == [
        target.target.sha256 for target in after
    ]


def test_production_terminal_vector_has_no_fallback() -> None:
    market = _synthetic_market()
    targets = build_trial_targets(market, TRIALS[0], DEVELOPMENT_START, DEVELOPMENT_END)
    vectors = dict(market.vectors)
    vectors.pop(DEVELOPMENT_END - timedelta(minutes=1))
    broken = replace(market, vectors=vectors)
    with pytest.raises(VolatilityManagedError, match="terminal"):
        simulate_path(
            broken,
            "terminal-proof",
            DEVELOPMENT_START,
            DEVELOPMENT_END,
            _ordinary_fills(targets),
            cost_rate=BASE_COST,
        )


def test_production_synthetic_report_has_exact_closed_gate_map() -> None:
    report = evaluate_development(_synthetic_market(), _effective(), ROOT / "experiments")
    assert report["classification"] in {"DEVELOPMENT_GO", "HISTORICAL_NO_GO"}
    assert len(report["trials"]) == 7 and len(report["folds"]) == 4
    assert len(report["gate_checks"]) == 24
    assert report["source_partition_count"] == 36
    assert report["input_identity_sha256"] == ALLOWLIST_SHA256
    assert report["holdout_opened"] is False and report["holdout_values_read"] is False
    assert report["candidate_promoted"] is False and report["capital_permitted"] == 0
