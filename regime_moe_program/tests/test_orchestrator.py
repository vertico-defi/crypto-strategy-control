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
    queued = json.loads((PROGRAM / "REGIMEMOE_WORK_QUEUE.json").read_text())
    stated = json.loads((PROGRAM / "REGIMEMOE_STATE.json").read_text())
    scored = json.loads((PROGRAM / "REGIMEMOE_SCORECARD.json").read_text())
    controller.atomic_write(
        journal, {"status": "PREPARED", "queue": queued, "state": stated, "scorecard": scored}
    )
    controller.recover()
    assert controller.read(queue) == queued
    assert not journal.exists()


def controller_module() -> object:
    sys.path.insert(0, str(ROOT))
    import orchestrate_regimemoe as controller

    return controller


def checkpoint_fixture(expected_artifact: str = "REGIMEMOE_WORK_QUEUE.json") -> dict[str, object]:
    return {
        "at_utc": "2026-08-09T00:00:00Z",
        "task_id": "data",
        "workstream": "DATA_AND_EVALUATION",
        "expected_artifact": expected_artifact,
        "commands": ["fixture"],
        "model_role": "deterministic_evidence",
        "model_route": {"model": "gpt-5.6-luna", "reasoning": "medium"},
        "deterministic_validation": ["fixture validation"],
        "status": "TERMINAL",
        "source_commit_or_blocker": "fixture",
    }


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
                "expected_artifact": "REGIMEMOE_WORK_QUEUE.json",
                "validation": ["fixture validation"],
                "last_checkpoint": checkpoint_fixture(),
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


def test_terminal_requires_existing_artifact() -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["tasks"][0]["expected_artifact"] = "absent-artifact.json"  # type: ignore[index]
    queue["tasks"][0]["last_checkpoint"]["expected_artifact"] = "absent-artifact.json"  # type: ignore[index]
    with pytest.raises(ValueError, match="required artifact is absent"):
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


def test_missing_runtime_model_discovery_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    controller = controller_module()
    monkeypatch.delenv("REGIMEMOE_AVAILABLE_MODELS", raising=False)
    with pytest.raises(RuntimeError, match="substantive failure"):
        controller.route_for({"role": "implementation_engineer"})  # type: ignore[attr-defined]


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


def test_stale_lock_with_live_owner_is_not_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = controller_module()
    lock = tmp_path / "lock"
    lock.write_text(json.dumps({"pid": 12345}))
    old = time.time() - 3601
    os.utime(lock, (old, old))
    monkeypatch.setattr(controller, "LOCK", lock)
    monkeypatch.setattr(controller.os, "kill", lambda pid, signal: None)
    with pytest.raises(RuntimeError, match="still active"):
        controller.acquire_lock()  # type: ignore[attr-defined]
    assert lock.exists()


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


def test_strict_schema_rejects_undeclared_property() -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["unexpected"] = True
    with pytest.raises(ValueError, match="schema validation failed"):
        controller.validate(queue)  # type: ignore[attr-defined]


def test_strict_schema_rejects_undeclared_nested_model_route_field() -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["tasks"][0]["last_checkpoint"]["model_route"]["injected"] = True  # type: ignore[index]
    with pytest.raises(ValueError, match="schema validation failed"):
        controller.validate(queue)  # type: ignore[attr-defined]


def test_strict_schema_rejects_malformed_nested_model_route_field() -> None:
    controller = controller_module()
    queue = queue_fixture()
    queue["tasks"][0]["last_checkpoint"]["model_route"]["model"] = 7  # type: ignore[index]
    with pytest.raises(ValueError, match="schema validation failed"):
        controller.validate(queue)  # type: ignore[attr-defined]


def test_scorecard_rejects_undeclared_nested_checkpoint_field() -> None:
    controller = controller_module()
    scorecard = json.loads((PROGRAM / "REGIMEMOE_SCORECARD.json").read_text())
    scorecard["completed_checkpoints"][0]["model_route"]["injected"] = True
    with pytest.raises(ValueError, match="schema validation failed"):
        controller.validate_scorecard(scorecard)  # type: ignore[attr-defined]


def test_scorecard_rejects_malformed_nested_checkpoint_field() -> None:
    controller = controller_module()
    scorecard = json.loads((PROGRAM / "REGIMEMOE_SCORECARD.json").read_text())
    scorecard["completed_checkpoints"][0]["model_route"]["reasoning"] = False
    with pytest.raises(ValueError, match="schema validation failed"):
        controller.validate_scorecard(scorecard)  # type: ignore[attr-defined]


def test_replay_rejects_invalid_scorecard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    controller = controller_module()
    queue, state, scorecard = (
        tmp_path / name for name in ("queue.json", "state.json", "score.json")
    )
    journal = tmp_path / "journal.json"
    monkeypatch.setattr(controller, "QUEUE", queue)
    monkeypatch.setattr(controller, "STATE", state)
    monkeypatch.setattr(controller, "SCORECARD", scorecard)
    monkeypatch.setattr(controller, "JOURNAL", journal)
    controller.atomic_write(
        journal,
        {
            "status": "PREPARED",
            "queue": json.loads((PROGRAM / "REGIMEMOE_WORK_QUEUE.json").read_text()),
            "state": json.loads((PROGRAM / "REGIMEMOE_STATE.json").read_text()),
            "scorecard": {"schema_version": "1.0"},
        },
    )
    with pytest.raises(ValueError, match="schema validation failed"):
        controller.recover()  # type: ignore[attr-defined]
    assert journal.exists()


def test_mock_multi_workstream_cycles_use_isolated_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = controller_module()
    queue_path, state_path, scorecard_path = (
        tmp_path / name for name in ("queue.json", "state.json", "scorecard.json")
    )
    journal = tmp_path / "journal.json"
    queue = {
        "schema_version": "1.0",
        "workstreams": ["DATA_AND_EVALUATION", "AI_ROUTER"],
        "tasks": [
            {
                "id": "data-review",
                "workstream": "DATA_AND_EVALUATION",
                "role": "deterministic_evidence",
                "state": "READY",
                "priority": 1,
                "commands": ["fixture"],
                "expected_artifact": "data-review.json",
                "validation": ["fixture validation"],
            },
            {
                "id": "router-review",
                "workstream": "AI_ROUTER",
                "role": "implementation_engineer",
                "state": "READY",
                "priority": 2,
                "commands": ["fixture"],
                "expected_artifact": "router-review.json",
                "validation": ["fixture validation"],
            },
        ],
    }
    state = json.loads((PROGRAM / "REGIMEMOE_STATE.json").read_text())
    scorecard = json.loads((PROGRAM / "REGIMEMOE_SCORECARD.json").read_text())
    controller.atomic_write(queue_path, queue)
    controller.atomic_write(state_path, state)
    controller.atomic_write(scorecard_path, scorecard)
    monkeypatch.setattr(controller, "QUEUE", queue_path)
    monkeypatch.setattr(controller, "STATE", state_path)
    monkeypatch.setattr(controller, "SCORECARD", scorecard_path)
    monkeypatch.setattr(controller, "JOURNAL", journal)

    first, second = controller.cycle(True), controller.cycle(True)  # type: ignore[attr-defined]

    assert first["checkpoint"]["task_id"] == "data-review"
    assert second["checkpoint"]["task_id"] == "router-review"
    assert [task["state"] for task in controller.read(queue_path)["tasks"]] == [  # type: ignore[attr-defined]
        "HUMAN_APPROVAL",
        "HUMAN_APPROVAL",
    ]
    assert not journal.exists()


def test_g0_transition_is_atomic_and_activates_only_contract_adapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = controller_module()
    queue_path, state_path, scorecard_path = (
        tmp_path / name for name in ("queue.json", "state.json", "scorecard.json")
    )
    journal = tmp_path / "journal.json"
    controller.atomic_write(
        queue_path, json.loads((PROGRAM / "REGIMEMOE_WORK_QUEUE.json").read_text())
    )
    controller.atomic_write(
        state_path, json.loads((PROGRAM / "REGIMEMOE_STATE.json").read_text())
    )
    controller.atomic_write(
        scorecard_path, json.loads((PROGRAM / "REGIMEMOE_SCORECARD.json").read_text())
    )
    monkeypatch.setattr(controller, "QUEUE", queue_path)
    monkeypatch.setattr(controller, "STATE", state_path)
    monkeypatch.setattr(controller, "SCORECARD", scorecard_path)
    monkeypatch.setattr(controller, "JOURNAL", journal)

    controller.record_g0_foundation_pass()  # type: ignore[attr-defined]

    transitioned = controller.read(state_path)  # type: ignore[attr-defined]
    transitioned_queue = controller.read(queue_path)  # type: ignore[attr-defined]
    assert transitioned["phase"] == 1
    assert transitioned["g0_foundation_pass"]["verdict"] == "PASS"
    assert transitioned["production_adapters_active"] == ["data_contract_validation"]
    g1_task = next(
        task for task in transitioned_queue["tasks"] if task["id"] == "g1-data-contract-validation"
    )
    assert g1_task["state"] == "READY"
    assert not journal.exists()


def test_contract_adapter_writes_only_deterministic_contract_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller = controller_module()
    lab = tmp_path / "regime-moe-lab"
    contracts = lab / "contracts"
    contracts.mkdir(parents=True)
    (contracts / "DATA_CONTRACT.md").write_text(
        "hash-verified BTC/ETH\nevent and availability timestamps\n"
        "Phase 0 reads no market values\nresolves no holdout path\n"
    )
    (contracts / "FROZEN_DEVELOPMENT_PROTOCOL.yaml").write_text(
        "scope: development_only\nholdout_access: FORBIDDEN\nsame_bar_execution: FORBIDDEN\n"
    )
    monkeypatch.setattr(controller, "LAB_ROOT", lab)
    task = {
        "id": "g1-data-contract-validation",
        "workstream": "DATA_AND_EVALUATION",
        "role": "deterministic_evidence",
        "commands": ["fixture"],
        "expected_artifact": "regime-moe-lab/artifacts/g1-data-contract-validation.json",
        "validation": ["fixture validation"],
    }

    outcome, report = controller.execute_data_contract_validation(task)  # type: ignore[attr-defined]

    artifact = lab / "artifacts" / "g1-data-contract-validation.json"
    assert outcome == "TERMINAL"
    assert report["validation_result"] == "G1_DATA_CONTRACT_VALIDATION_PASS"
    assert json.loads(artifact.read_text())["holdout_access"] == "FORBIDDEN"
