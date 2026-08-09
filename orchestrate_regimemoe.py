#!/usr/bin/env python3
"""Lean, persistent Phase-0 controller for the RegimeMoE program.

It plans bounded work; it never acquires data, trains models, evaluates a strategy,
opens a holdout, routes orders, or starts a timer.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parent / "regime_moe_program"
LAB_ROOT = ROOT.parent.parent / "regime-moe-lab"
QUEUE = ROOT / "REGIMEMOE_WORK_QUEUE.json"
STATE = ROOT / "REGIMEMOE_STATE.json"
SCORECARD = ROOT / "REGIMEMOE_SCORECARD.json"
LOCK = ROOT / ".orchestrator.lock"
JOURNAL = ROOT / "REGIMEMOE_TRANSACTION.json"
SCHEMAS = ROOT / "schemas"
STATES = {
    "READY",
    "RUNNING",
    "WAITING_EXTERNAL",
    "BLOCKED_DEPENDENCY",
    "HUMAN_APPROVAL",
    "DEFERRED",
    "TERMINAL",
    "FAILED",
    "PAUSED_FOR_USAGE",
}
TERMINAL_DEPENDENCY_OUTCOMES = {"TERMINAL"}
ROLES = {
    "research_director": {"model": "gpt-5.6-sol", "reasoning": "high"},
    "independent_auditor": {
        "model": "gpt-5.6-sol",
        "reasoning": "high_or_xhigh_when_promotion_possible",
    },
    "implementation_engineer": {"model": "gpt-5.6-terra", "reasoning": "medium"},
    "deterministic_evidence": {"model": "gpt-5.6-luna", "reasoning": "medium"},
    "content_product": {"model": "gpt-5.6-terra", "reasoning": "medium"},
}
AvailabilityError = Literal["quota", "rate_limit", "temporary_unavailable"]
ALLOWED_AVAILABILITY_ERRORS: set[str] = {"quota", "rate_limit", "temporary_unavailable"}


def available_models() -> set[str]:
    """Read the runtime availability record; absence fails closed for model work."""
    configured = os.environ.get("REGIMEMOE_AVAILABLE_MODELS")
    if not configured:
        return set()
    return {model.strip() for model in configured.split(",") if model.strip()}


def route_for(
    task: dict[str, Any], availability_error: AvailabilityError | None = None
) -> dict[str, str]:
    route = ROLES[task["role"]]
    if task["role"] == "independent_auditor" and not task.get("promotion_possible", False):
        raise ValueError("independent audit is forbidden before promotion eligibility")
    if route["model"] in available_models():
        return route
    if availability_error not in ALLOWED_AVAILABILITY_ERRORS:
        raise RuntimeError(
            "preferred model unavailable; substantive failure never triggers fallback"
        )
    fallback = task.get("temporary_availability_fallback")
    if not isinstance(fallback, str) or fallback not in available_models():
        raise RuntimeError("no permitted temporary-availability fallback")
    return {
        "model": fallback,
        "reasoning": route["reasoning"],
        "fallback_reason": availability_error,
    }


def now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("controller JSON root must be an object")
    return cast(dict[str, Any], value)


def validate_state(state: dict[str, Any]) -> None:
    validate_json_schema(state, SCHEMAS / "state.schema.json")
    required = {"program", "phase", "capital_permitted", "holdout_access", "usage_paused"}
    if not required <= state.keys() or state["program"] != "RegimeMoE":
        raise ValueError("invalid program state")
    if state["capital_permitted"] != 0 or state["holdout_access"] != "FORBIDDEN":
        raise ValueError("capital or holdout safety boundary violated")
    lock = state.get("website_external_lock")
    if not isinstance(lock, dict) or not str(
        lock.get("status", "")
    ).startswith("EXTERNALLY_LOCKED"):
        raise ValueError("website external lock must remain enforced")
    if state["usage_paused"] and not isinstance(state.get("retry_after_utc"), str):
        raise ValueError("usage pause requires retry_after_utc")
    if state["phase"] > 0:
        gate = state.get("g0_foundation_pass")
        if not isinstance(gate, dict) or gate.get("verdict") != "PASS":
            raise ValueError("Phase 1 requires a recorded G0 foundation pass")


def validate_scorecard(scorecard: dict[str, Any]) -> None:
    validate_json_schema(scorecard, SCHEMAS / "scorecard.schema.json")


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as file:
        json.dump(value, file, indent=2, sort_keys=True)
        file.write("\n")
        temporary = Path(file.name)
    os.replace(temporary, path)


def validate(queue: dict[str, Any]) -> None:
    validate_json_schema(queue, SCHEMAS / "queue.schema.json")
    if not isinstance(queue.get("workstreams"), list) or not isinstance(queue.get("tasks"), list):
        raise ValueError("queue workstreams and tasks must be arrays")
    seen: set[str] = set()
    tasks_by_id: dict[str, dict[str, Any]] = {}
    for task in queue["tasks"]:
        if not isinstance(task, dict) or task.get("id") in seen or task.get("state") not in STATES:
            raise ValueError("invalid queue task")
        seen.add(task["id"])
        if task["workstream"] not in queue["workstreams"] or task["role"] not in ROLES:
            raise ValueError("invalid workstream or role")
        if (
            not isinstance(task.get("expected_artifact"), str)
            or not task["expected_artifact"]
            or not isinstance(task.get("validation"), list)
            or not task["validation"]
        ):
            raise ValueError("task lacks mandatory evidence contract")
        tasks_by_id[task["id"]] = task
    for task in queue["tasks"]:
        dependencies = task.get("depends_on", [])
        outcomes = task.get("dependency_outcomes", {})
        if not isinstance(dependencies, list) or not isinstance(outcomes, dict):
            raise ValueError("dependencies and dependency outcomes must be structured")
        if set(outcomes) != set(dependencies):
            raise ValueError("each dependency requires an explicit accepted outcome")
        for dependency in dependencies:
            if dependency not in tasks_by_id:
                raise ValueError("task depends on an unknown task")
            if outcomes[dependency] not in TERMINAL_DEPENDENCY_OUTCOMES:
                raise ValueError("only validated terminal dependency outcomes may release work")
        if (
            task.get("workstream") == "AUDIENCE_AND_MONETIZATION"
            and task.get("state") == "READY"
            and not task.get("verified_provenance")
        ):
            raise ValueError("ready content task lacks verified provenance")
        if task.get("state") == "TERMINAL":
            checkpoint_data = task.get("last_checkpoint")
            if (
                not isinstance(checkpoint_data, dict)
                or checkpoint_data.get("status") != "TERMINAL"
                or checkpoint_data.get("expected_artifact") != task["expected_artifact"]
                or not checkpoint_data.get("deterministic_validation")
            ):
                raise ValueError("terminal task lacks deterministic artifact evidence")
            if not artifact_path(task["expected_artifact"]).is_file():
                raise ValueError("terminal task required artifact is absent")


def validate_json_schema(value: dict[str, Any], schema_path: Path) -> None:
    schema = read(schema_path)
    errors = sorted(Draft202012Validator(schema).iter_errors(value), key=lambda error: error.path)
    if errors:
        location = ".".join(str(part) for part in errors[0].path) or "root"
        raise ValueError(f"schema validation failed at {location}: {errors[0].message}")


def artifact_path(expected_artifact: str) -> Path:
    root_relative = Path(expected_artifact)
    if root_relative.is_absolute() or ".." in root_relative.parts:
        raise ValueError("artifact path is outside the permitted Phase 0 roots")
    if root_relative.parts and root_relative.parts[0] == "regime-moe-lab":
        candidate = LAB_ROOT.joinpath(*root_relative.parts[1:])
    else:
        candidate = ROOT / root_relative
    if not candidate.is_relative_to(ROOT) and not candidate.is_relative_to(LAB_ROOT):
        raise ValueError("artifact path is outside the permitted Phase 0 roots")
    return candidate


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def acquire_lock() -> None:
    try:
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        age = datetime.now().timestamp() - LOCK.stat().st_mtime
        if age < 3600:
            raise RuntimeError("exclusive lock held") from error
        try:
            existing = read(LOCK)
            existing_pid = existing.get("pid")
            if isinstance(existing_pid, int):
                os.kill(existing_pid, 0)
                raise RuntimeError("stale-looking lock owner is still active") from error
        except ProcessLookupError:
            pass
        except PermissionError as pid_error:
            raise RuntimeError("cannot establish stale lock owner is inactive") from pid_error
        except (ValueError, OSError):
            pass
        LOCK.unlink()
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        json.dump({"pid": os.getpid(), "created_at": now()}, file)


def release_lock() -> None:
    LOCK.unlink(missing_ok=True)


def commit_snapshot(
    queue: dict[str, Any], state: dict[str, Any], scorecard: dict[str, Any]
) -> None:
    atomic_write(
        JOURNAL, {"status": "PREPARED", "queue": queue, "state": state, "scorecard": scorecard}
    )
    atomic_write(QUEUE, queue)
    atomic_write(STATE, state)
    atomic_write(SCORECARD, scorecard)
    JOURNAL.unlink(missing_ok=True)


def recover() -> None:
    if JOURNAL.exists():
        pending = read(JOURNAL)
        if pending.get("status") != "PREPARED":
            raise RuntimeError("invalid interrupted transaction journal")
        if not all(isinstance(pending.get(key), dict) for key in ("queue", "state", "scorecard")):
            raise RuntimeError("interrupted transaction journal is incomplete")
        validate(cast(dict[str, Any], pending["queue"]))
        validate_state(cast(dict[str, Any], pending["state"]))
        validate_scorecard(cast(dict[str, Any], pending["scorecard"]))
        commit_snapshot(pending["queue"], pending["state"], pending["scorecard"])


def iso_week() -> str:
    current = datetime.now(UTC).isocalendar()
    return f"{current.year}-W{current.week:02d}"


def dependency_outcomes_satisfied(
    task: dict[str, Any], tasks_by_id: dict[str, dict[str, Any]]
) -> bool:
    return all(
        tasks_by_id[dependency]["state"] == outcome
        for dependency, outcome in task.get("dependency_outcomes", {}).items()
    )


def select(queue: dict[str, Any]) -> dict[str, Any] | None:
    """Choose one independent READY task; waiting work is intentionally ignored."""
    tasks_by_id = {task["id"]: task for task in queue["tasks"]}
    candidates = [
        task
        for task in queue["tasks"]
        if task["state"] == "READY" and dependency_outcomes_satisfied(task, tasks_by_id)
    ]
    if not candidates:
        return None
    weekly = queue.get("weekly_completed_by_iso_week", {})
    recent = weekly.get(iso_week(), queue.get("weekly_completed_by_workstream", {}))
    return cast(
        dict[str, Any],
        min(
            candidates,
            key=lambda task: (recent.get(task["workstream"], 0), task["priority"], task["id"]),
        ),
    )


def checkpoint(task: dict[str, Any], status: str, blocker: str | None = None) -> dict[str, Any]:
    route = dict(ROLES[task["role"]])
    route["availability"] = "NOT_INVOKED"
    return {
        "at_utc": now(),
        "task_id": task["id"],
        "workstream": task["workstream"],
        "expected_artifact": task["expected_artifact"],
        "commands": task["commands"],
        "model_role": task["role"],
        "model_route": route,
        "deterministic_validation": task["validation"],
        "status": status,
        "source_commit_or_blocker": blocker or "NO_MUTATION_DRY_OR_PHASE_0_CHECKPOINT",
    }


def execute_phase_zero(task: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if task.get("adapter") != "repository_state_smoke":
        return "HUMAN_APPROVAL", checkpoint(
            task, "HUMAN_APPROVAL", "no execution adapter authorized"
        )
    result = subprocess.run(
        ["git", "-C", str(ROOT.parent), "status", "--short", "--branch"],
        check=True,
        capture_output=True,
        text=True,
    )
    report = checkpoint(task, "TERMINAL", result.stdout.strip() or "CLEAN_REPOSITORY_STATE")
    artifact = artifact_path(task["expected_artifact"])
    if not artifact.is_file():
        raise RuntimeError("Phase 0 adapter cannot terminalize without its required artifact")
    report["artifact_exists"] = True
    report["validation_result"] = "READ_ONLY_REPOSITORY_STATE_SMOKE"
    return "TERMINAL", report


def execute_data_contract_validation(task: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Produce a deterministic contract-only G1 artifact without resolving data locations."""
    source = LAB_ROOT / "contracts" / "DATA_CONTRACT.md"
    frozen = LAB_ROOT / "contracts" / "FROZEN_DEVELOPMENT_PROTOCOL.yaml"
    required_data_clauses = (
        "hash-verified BTC/ETH",
        "event and availability timestamps",
        "Phase 0 reads no market values",
        "resolves no holdout path",
    )
    required_protocol_clauses = (
        "scope: development_only",
        "holdout_access: FORBIDDEN",
        "same_bar_execution: FORBIDDEN",
    )
    if not source.is_file() or not frozen.is_file():
        raise RuntimeError("G1 contract sources are absent")
    if any(clause not in source.read_text(encoding="utf-8") for clause in required_data_clauses):
        raise RuntimeError("G1 data contract is incomplete")
    if any(
        clause not in frozen.read_text(encoding="utf-8") for clause in required_protocol_clauses
    ):
        raise RuntimeError("G1 frozen protocol is incomplete")
    artifact = artifact_path(task["expected_artifact"])
    artifact.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "artifact_type": "G1_DATA_CONTRACT_VALIDATION",
        "capital_permitted": 0,
        "holdout_access": "FORBIDDEN",
        "inputs": {
            "data_contract_sha256": sha256(source),
            "frozen_protocol_sha256": sha256(frozen),
        },
        "result": "PASS",
        "scope": "contract_only_no_market_data_or_holdout_resolution",
        "schema_version": "1.0",
    }
    atomic_write(artifact, payload)
    report = checkpoint(task, "TERMINAL", f"artifact_sha256={sha256(artifact)}")
    report["artifact_exists"] = True
    report["validation_result"] = "G1_DATA_CONTRACT_VALIDATION_PASS"
    return "TERMINAL", report


def execute_task(task: dict[str, Any], state: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if state["phase"] == 0:
        return execute_phase_zero(task)
    if task.get("adapter") == "data_contract_validation":
        return execute_data_contract_validation(task)
    return "HUMAN_APPROVAL", checkpoint(task, "HUMAN_APPROVAL", "no production adapter authorized")


def production_queue(queue: dict[str, Any]) -> dict[str, Any]:
    promoted = cast(dict[str, Any], json.loads(json.dumps(queue)))
    for task in promoted["tasks"]:
        if task["id"] == "phase0-data-contract-review":
            task["state"] = "HUMAN_APPROVAL"
            task["blocker"] = "superseded by recorded G0 foundation pass"
    promoted["tasks"].append(
        {
            "adapter": "data_contract_validation",
            "commands": ["validate published development-only contracts"],
            "expected_artifact": "regime-moe-lab/artifacts/g1-data-contract-validation.json",
            "id": "g1-data-contract-validation",
            "priority": 10,
            "role": "deterministic_evidence",
            "state": "READY",
            "validation": [
                "development-only scope",
                "holdout remains unresolved",
                "no market-data path resolution",
                "deterministic artifact",
            ],
            "workstream": "DATA_AND_EVALUATION",
        }
    )
    return promoted


def record_g0_foundation_pass() -> None:
    queue, state, scorecard = read(QUEUE), read(STATE), read(SCORECARD)
    validate(queue)
    validate_state(state)
    validate_scorecard(scorecard)
    if state["phase"] != 0:
        raise RuntimeError("G0 foundation pass is already recorded")
    state["phase"] = 1
    state["g0_foundation_pass"] = {
        "control_commit": "f6128c9",
        "lab_commit": "3b7443c",
        "model": "gpt-5.6-sol",
        "reasoning": "high",
        "verdict": "PASS",
    }
    state["production_adapters_active"] = ["data_contract_validation"]
    commit_snapshot(production_queue(queue), state, scorecard)


def cycle(mutate: bool) -> dict[str, Any]:
    recover()
    queue, state, scorecard = read(QUEUE), read(STATE), read(SCORECARD)
    validate(queue)
    validate_state(state)
    validate_scorecard(scorecard)
    if state["usage_paused"]:
        return {"status": "USAGE_PAUSED", "checkpoint": None}
    task = select(queue)
    if task is None:
        return {"status": "NO_OP", "checkpoint": None}
    report = checkpoint(task, "PLANNED")
    if mutate:
        task["state"] = "RUNNING"
        commit_snapshot(queue, state, scorecard)
        outcome, report = execute_task(task, state)
        if outcome == "TERMINAL" and not report.get("artifact_exists"):
            raise RuntimeError("terminal task requires deterministic artifact evidence")
        task["state"] = outcome
        task["last_checkpoint"] = report
        if outcome == "TERMINAL":
            weekly = queue.setdefault("weekly_completed_by_iso_week", {})
            counts = weekly.setdefault(iso_week(), {})
            counts[task["workstream"]] = counts.get(task["workstream"], 0) + 1
            scorecard["completed_checkpoints"].append(report)
        state["last_cycle_at_utc"] = now()
        commit_snapshot(queue, state, scorecard)
    return {"status": report["status"], "checkpoint": report}


def retry_after_reached(state: dict[str, Any]) -> bool:
    retry_after = state.get("retry_after_utc")
    if not isinstance(retry_after, str):
        return False
    return datetime.fromisoformat(retry_after.replace("Z", "+00:00")) <= datetime.now(UTC)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=(
            "status",
            "dry-run",
            "one-cycle",
            "multi-cycle",
            "resume",
            "weekly-report",
            "record-g0-pass",
        ),
    )
    parser.add_argument("--max-cycles", type=int, default=1)
    args = parser.parse_args()
    if args.command == "status":
        queue, state = read(QUEUE), read(STATE)
        validate(queue)
        validate_state(state)
        validate_scorecard(read(SCORECARD))
        print(
            json.dumps(
                {
                    "state": state,
                    "task_counts": {
                        s: sum(t["state"] == s for t in queue["tasks"]) for s in STATES
                    },
                },
                indent=2,
            )
        )
        return
    if args.command == "weekly-report":
        scorecard = read(SCORECARD)
        validate_scorecard(scorecard)
        print(json.dumps(scorecard, indent=2))
        return
    if args.command == "resume":
        state = read(STATE)
        validate_state(state)
        if state["usage_paused"] and not retry_after_reached(state):
            raise RuntimeError("usage pause remains in effect until retry_after_utc")
        state["usage_paused"] = False
        state.pop("retry_after_utc", None)
        atomic_write(STATE, state)
        print(json.dumps({"status": "RESUMED"}))
        return
    if args.command == "record-g0-pass":
        acquire_lock()
        try:
            recover()
            record_g0_foundation_pass()
            print(json.dumps({"status": "G0_FOUNDATION_PASS"}))
        finally:
            release_lock()
        return
    acquire_lock()
    try:
        if args.command == "dry-run":
            result = cycle(False)
        elif args.command == "one-cycle":
            result = cycle(True)
        else:
            reports = []
            for _ in range(max(0, args.max_cycles)):
                report = cycle(True)
                reports.append(report)
                if report["status"] in {"NO_OP", "USAGE_PAUSED"}:
                    break
            result = {"status": "MULTI_CYCLE", "reports": reports}
        print(json.dumps(result, indent=2))
    finally:
        release_lock()


if __name__ == "__main__":
    main()
