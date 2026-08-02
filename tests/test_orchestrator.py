"""Safety properties for the persistent research controller."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from strategy_control import orchestrator


def test_atomic_json_replaces_complete_document(tmp_path: Path) -> None:
    target = tmp_path / "state.json"
    orchestrator.atomic_json(target, {"complete": True})
    assert json.loads(target.read_text()) == {"complete": True}
    assert not target.with_suffix(".json.tmp").exists()


def test_corrupt_state_fails_closed(tmp_path: Path) -> None:
    target = tmp_path / "state.json"
    target.write_text("{not-json")
    with pytest.raises(orchestrator.StateError, match="corruption"):
        orchestrator.load_json(target)


def test_live_lock_refuses_concurrent_mutation(tmp_path: Path) -> None:
    lock = tmp_path / "cycle.lock"
    with (
        orchestrator.exclusive_lock(lock),
        pytest.raises(orchestrator.StateError, match="concurrent"),
        orchestrator.exclusive_lock(lock),
    ):
        pass


def test_stale_lock_is_quarantined_then_recovered(tmp_path: Path) -> None:
    lock = tmp_path / "cycle.lock"
    lock.write_text("stale")
    old = time.time() - 1000
    os.utime(lock, (old, old))
    with orchestrator.exclusive_lock(lock, stale_seconds=1):
        assert lock.exists()
    assert not lock.exists()
    assert list(tmp_path.glob("cycle.lock.stale-*"))


@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        ("success", "USED"),
        ("unavailable", "FALLBACK"),
        ("terra_unavailable", "FALLBACK"),
        ("quota", "PAUSED_FOR_USAGE"),
        ("substantive_failure", "FAILED"),
        ("interface_missing", "INTERFACE_UNAVAILABLE"),
    ],
)
def test_model_routing_preserves_quality_or_stops(outcome: str, status: str) -> None:
    route = orchestrator.model_route(outcome)
    assert route["status"] == status
    if outcome in {"unavailable", "terra_unavailable"}:
        assert route["reasoning"] == "high"
    if outcome == "substantive_failure":
        assert route["model"] is None


def test_preregistration_hash_detects_mutation() -> None:
    prereg = orchestrator.preregistration({"family": "x", "hypothesis": "y"}, "abc")
    prereg["preregistration_sha256"] = orchestrator._sha(prereg)
    changed = dict(prereg)
    changed["target"] = "post-hoc target"
    digest = changed.pop("preregistration_sha256")
    assert digest != orchestrator._sha(changed)
