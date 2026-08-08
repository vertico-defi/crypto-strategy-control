import json
import os
import subprocess
import sys
import time
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

PROGRAM = Path(__file__).resolve().parents[1]
ROOT = PROGRAM.parent
ENTRY = ROOT / "orchestrate_regimemoe.py"


def invoke(*args: str) -> dict[str, object]:
    process = subprocess.run(
        [sys.executable, str(ENTRY), *args], capture_output=True, text=True, check=True
    )
    return json.loads(process.stdout)


def test_status_validates_queue() -> None:
    assert "task_counts" in invoke("status")


def test_dry_run_selects_ready_while_waiting_external_exists() -> None:
    result = invoke("dry-run")
    assert result["checkpoint"]["task_id"] != "funding-finalization"  # type: ignore[index]


def test_dry_run_does_not_mutate_queue() -> None:
    queue = PROGRAM / "REGIMEMOE_WORK_QUEUE.json"
    before = queue.read_bytes()
    invoke("dry-run")
    assert queue.read_bytes() == before


def test_website_lock_is_explicit() -> None:
    state = json.loads((PROGRAM / "REGIMEMOE_STATE.json").read_text())
    assert state["website_external_lock"]["status"].startswith("EXTERNALLY_LOCKED")


def test_auditor_requires_promotion_eligibility() -> None:
    sys.path.insert(0, str(ROOT))
    import orchestrate_regimemoe as controller

    with pytest.raises(ValueError):
        controller.route_for({"role": "independent_auditor"})


def test_stale_lock_is_recovered(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sys.path.insert(0, str(ROOT))
    import orchestrate_regimemoe as controller

    lock = tmp_path / "lock"
    lock.write_text("stale")
    old = time.time() - 3601
    os.utime(lock, (old, old))
    monkeypatch.setattr(controller, "LOCK", lock)
    controller.acquire_lock()
    assert lock.exists()
    controller.release_lock()


def test_interrupted_journal_replays(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sys.path.insert(0, str(ROOT))
    import orchestrate_regimemoe as controller

    queue, state, scorecard = (
        tmp_path / name for name in ("queue.json", "state.json", "score.json")
    )
    journal = tmp_path / "journal.json"
    monkeypatch.setattr(controller, "QUEUE", queue)
    monkeypatch.setattr(controller, "STATE", state)
    monkeypatch.setattr(controller, "SCORECARD", scorecard)
    monkeypatch.setattr(controller, "JOURNAL", journal)
    controller.atomic_write(
        journal, {"status": "PREPARED", "queue": {"x": 1}, "state": {"y": 2}, "scorecard": {"z": 3}}
    )
    controller.recover()
    assert controller.read(queue) == {"x": 1}
    assert not journal.exists()


def controller_module() -> object:
    sys.path.insert(0, str(ROOT))
    import orchestrate_regimemoe as controller

    return controller


def queue_fixture() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "workstreams": ["DATA_AND_EVALUATION", "AI_ROUTER"],
        "tasks": [
            {
                "id": "data",
                "workstream": "DATA_AND_EVALUATION",
                "role": "deterministic_evidence",
                "state": "TERMINAL",
                "priority": 1,
                "commands": ["fixture"],
                "expected_artifact": "data.json",
                "validation": ["fixture validation"],
                "last_checkpoint": {
                    "status": "TERMINAL",
                    "expected_artifact": "data.json",
                    "deterministic_validation": ["fixture validation"],
                },
            },
            {
                "id": "router",
                "workstream": "AI_ROUTER",
                "role": "implementation_engineer",
                "state": "READY",
                "priority": 2,
                "commands": ["fixture"],
                "expected_artifact": "router.json",
                "validation": ["fixture validation"],
                "depends_on": ["data"],
                "dependency_outcomes": {"data": "TERMINAL"},
            },
        ],
        "weekly_completed_by_iso_week": {"2026-W32": {"DATA_AND_EVALUATION": 1}},
    }


def test_dependency_requires_explicit_success_outcome() -> None:
    controller = controller_module()
    queue = queue_fixture()
    task = queue["tasks"][1]  # type: ignore[index]
    task.pop("dependency_outcomes")
    with pytest.raises(ValueError, match="explicit accepted outcome"):
        controller.validate(queue)  # type: ignore[attr-defined]


def test_failed_dependency_never_releases_ready_task() -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["tasks"][0]["state"] = "FAILED"  # type: ignore[index]
    controller.validate(queue)  # type: ignore[attr-defined]
    assert controller.select(queue) is None  # type: ignore[attr-defined]


def test_terminal_requires_matching_artifact_evidence() -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["tasks"][0]["last_checkpoint"]["expected_artifact"] = "wrong.json"  # type: ignore[index]
    with pytest.raises(ValueError, match="deterministic artifact evidence"):
        controller.validate(queue)  # type: ignore[attr-defined]


def test_ready_content_requires_verified_provenance() -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["workstreams"].append("AUDIENCE_AND_MONETIZATION")  # type: ignore[index]
    queue["tasks"].append(  # type: ignore[index]
        {
            "id": "content",
            "workstream": "AUDIENCE_AND_MONETIZATION",
            "role": "content_product",
            "state": "READY",
            "priority": 3,
            "commands": ["fixture"],
            "expected_artifact": "content.md",
            "validation": ["fixture validation"],
        }
    )
    with pytest.raises(ValueError, match="verified provenance"):
        controller.validate(queue)  # type: ignore[attr-defined]


def test_typed_availability_fallback_only_allows_temporary_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = controller_module()
    monkeypatch.setattr(controller, "available_models", lambda: {"fallback"})
    task = {"role": "implementation_engineer", "temporary_availability_fallback": "fallback"}
    assert controller.route_for(task, "rate_limit")["model"] == "fallback"  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="substantive failure"):
        controller.route_for(task)  # type: ignore[attr-defined]


def test_iso_week_fairness_ignores_historical_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["tasks"].append(  # type: ignore[index]
        {
            "id": "independent-data",
            "workstream": "DATA_AND_EVALUATION",
            "role": "deterministic_evidence",
            "state": "READY",
            "priority": 1,
            "commands": ["fixture"],
            "expected_artifact": "independent.json",
            "validation": ["fixture validation"],
        }
    )
    monkeypatch.setattr(controller, "iso_week", lambda: "2026-W33")
    assert controller.select(queue)["id"] == "independent-data"  # type: ignore[attr-defined,index]


def test_usage_pause_requires_retry_and_prevents_selection() -> None:
    controller = controller_module()
    state = json.loads((PROGRAM / "REGIMEMOE_STATE.json").read_text())
    paused = deepcopy(state)
    paused["usage_paused"] = True
    paused["retry_after_utc"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    controller.validate_state(paused)  # type: ignore[attr-defined]
    assert not controller.retry_after_reached(paused)  # type: ignore[attr-defined]
    paused.pop("retry_after_utc")
    with pytest.raises(ValueError, match="retry_after_utc"):
        controller.validate_state(paused)  # type: ignore[attr-defined]


def test_schema_documents_cover_current_state_and_queue() -> None:
    schemas = PROGRAM / "schemas"
    queue_schema = json.loads((schemas / "queue.schema.json").read_text())
    state_schema = json.loads((schemas / "state.schema.json").read_text())
    assert {"schema_version", "workstreams", "tasks"} <= set(queue_schema["required"])
    assert {"capital_permitted", "holdout_access", "website_external_lock"} <= set(
        state_schema["required"]
    )


def test_strict_schema_rejects_missing_evidence_contract() -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["tasks"][1].pop("validation")  # type: ignore[index]
    with pytest.raises(ValueError, match="schema validation failed"):
        controller.validate(queue)  # type: ignore[attr-defined]
