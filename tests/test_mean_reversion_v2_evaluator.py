"""Focused synthetic checks for the formal v2 evaluator boundary."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.mean_reversion_v2 import ASSETS
from strategy_control.mean_reversion_v2_evaluator import (
    FormalEvaluationError,
    PreResultBindings,
    _path,
)
from strategy_control.mean_reversion_v2_pipeline import FillIdentity, JointSession


def bindings() -> PreResultBindings:
    names = (
        "frozen_preregistration_sha256",
        "implementation_commit",
        "implementation_hashes",
        "source_commit",
        "allowlist_sha256",
        "session_input_manifest_sha256",
        "target_trace_schema_sha256",
        "fill_trace_schema_sha256",
        "environment_sha256",
        "formal_invocation_id",
    )
    return PreResultBindings({name: name for name in names}, 36)


def synthetic_path() -> tuple[list[JointSession], list[FillIdentity], datetime, datetime]:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    sessions: list[JointSession] = []
    fills: list[FillIdentity] = []
    for index in range(180):
        day = start + timedelta(days=index)
        base = day + timedelta(days=1, minutes=1)
        phase = index % 25
        price = 100.0 + (phase % 2) * 0.1 if phase < 21 else (95.0, 90.0, 85.0, 86.0)[phase - 21]
        sessions.append(
            JointSession(day, True, day + timedelta(days=1), {asset: price for asset in ASSETS}, 0)
        )
        fills.append(
            FillIdentity(
                day,
                index,
                base,
                base + timedelta(days=1),
                {asset: price for asset in ASSETS},
                {asset: f"b{index}" for asset in ASSETS},
                {asset: price for asset in ASSETS},
                {asset: f"d{index}" for asset in ASSETS},
            )
        )
    return sessions, fills, start, start + timedelta(days=181)


def test_pre_result_bindings_fail_closed_and_are_json_safe() -> None:
    valid = bindings()
    valid.validate()
    assert json.loads(json.dumps(dict(valid.values))) == dict(valid.values)
    with pytest.raises(FormalEvaluationError, match="bindings"):
        PreResultBindings({"wrong": "x"}, 36).validate()
    with pytest.raises(FormalEvaluationError, match="holdout"):
        PreResultBindings(valid.values, 36, True).validate()


def test_synthetic_path_reconciles_terminal_cash_and_trace_families() -> None:
    sessions, fills, start, end = synthetic_path()
    result = _path(
        name="synthetic",
        sessions=sessions,
        identities=fills,
        start=start,
        end=end,
        trial=__import__("strategy_control.mean_reversion_v2", fromlist=["TRIALS"]).TRIALS[0],
    )
    assert result.terminal_cash
    assert set(result.trace_hashes) == {
        "input",
        "decision",
        "target",
        "fill",
        "disposition",
        "cost",
        "return",
    }
    assert len(result.intervals) == len(result.returns)


def test_missing_or_degenerate_evidence_fails_before_any_result() -> None:
    sessions, fills, start, end = synthetic_path()
    bad = [*fills[:-1], replace(fills[-1], base_timestamp=end)]
    with pytest.raises(FormalEvaluationError, match="terminal fill"):
        _path(
            name="bad",
            sessions=sessions,
            identities=bad,
            start=start,
            end=end,
            trial=__import__("strategy_control.mean_reversion_v2", fromlist=["TRIALS"]).TRIALS[0],
        )
