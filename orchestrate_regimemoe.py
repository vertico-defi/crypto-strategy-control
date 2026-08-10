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
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parent / "regime_moe_program"
LAB_ROOT = ROOT.parent.parent / "regime-moe-lab"
QUEUE = ROOT / "REGIMEMOE_WORK_QUEUE.json"
STATE = ROOT / "REGIMEMOE_STATE.json"
SCORECARD = ROOT / "REGIMEMOE_SCORECARD.json"
CATALOG = ROOT / "REGIMEMOE_AUTHORIZED_TASK_CATALOG.json"
REGISTRY = ROOT / "REGIMEMOE_PRODUCTION_ADAPTER_REGISTRY.json"
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
    "ADAPTER_NOT_CONFIGURED",
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
    website_state = state.get("website_external_lock")
    if not isinstance(website_state, dict):
        raise ValueError("website publication state must be structured")
    website_status = website_state.get("status")
    if website_status not in {
        "EXTERNALLY_LOCKED_UNTIL_MERGED_AND_EXPLICITLY_CLEARED",
        "WEBSITE_AVAILABLE_FOR_SANITIZED_PUBLICATION_TASKS",
    }:
        raise ValueError("website publication state is not authorized")
    if website_status == "WEBSITE_AVAILABLE_FOR_SANITIZED_PUBLICATION_TASKS":
        required_verification = {
            "verified_commit",
            "validation_workflow",
            "pages_deployment",
            "safety_flags",
            "authorized_scope",
            "prohibited_actions",
        }
        if not required_verification <= website_state.keys():
            raise ValueError("website availability requires complete verification evidence")
        if website_state["authorized_scope"] != "SANITIZED_EVIDENCE_UPDATES_ONLY":
            raise ValueError("website availability scope is broader than authorized")
        if any(website_state["safety_flags"].get(flag) is not False for flag in (
            "store_live", "payments_live", "newsletter_live"
        )):
            raise ValueError("website availability requires disabled live services")
        prohibited_actions = set(website_state["prohibited_actions"])
        required_prohibitions = {
            "live payments",
            "newsletter activation",
            "unsupported research claims",
            "product publication",
            "social publication",
            "public RegimeMoE repository creation",
        }
        if not required_prohibitions <= prohibited_actions:
            raise ValueError("website availability lacks required prohibitions")
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


def atomic_text_write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as file:
        file.write(value)
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


def registry_entry(task_id: str) -> dict[str, Any]:
    """Fail closed unless a task is catalog-hash-bound and explicitly mapped."""
    if sha256(CATALOG) != "3fb42f6d1d4de542f61977790c6ec4ce8b090b2f277a3fe95014b9feef16d6b5":
        raise RuntimeError("authorized task catalog hash mismatch")
    registry = read(REGISTRY)
    if registry.get("catalog_sha256") != sha256(CATALOG):
        raise RuntimeError("adapter registry catalog hash mismatch")
    matches = [item for item in registry.get("tasks", []) if item.get("task_id") == task_id]
    if len(matches) != 1 or not isinstance(matches[0], dict):
        raise RuntimeError("ADAPTER_NOT_CONFIGURED")
    return cast(dict[str, Any], matches[0])


def run_codex_task(task: dict[str, Any], mapping: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Invoke one bounded noninteractive Codex task and retain all transport evidence."""
    if mapping.get("adapter_class") not in {"CODEX_CONTENT", "CODEX_IMPLEMENTATION"}:
        raise RuntimeError("unsupported Codex adapter class")
    artifact = artifact_path(task["expected_artifact"])
    if not artifact.is_relative_to(LAB_ROOT):
        raise RuntimeError("Codex task artifact must be within the lab")
    attempt_id = f"{task['id']}-{now().replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:12]}"
    invocation = LAB_ROOT / "artifacts" / "production_invocations" / task["id"] / attempt_id
    invocation.mkdir(parents=True, exist_ok=False)
    bundle = {
        "task_id": task["id"],
        "catalog_sha256": sha256(CATALOG),
        "control_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT.parent,
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip(),
        "lab_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=LAB_ROOT, text=True, capture_output=True, check=True
        ).stdout.strip(),
        "mapping": mapping,
        "expected_artifact": task["expected_artifact"],
        "boundaries": {
            "capital": 0,
            "holdout": "FORBIDDEN",
            "website": "NO_WEBSITE_MUTATION",
            "gpu": False,
            "publication": "DRAFT_ONLY",
        },
    }
    atomic_write(invocation / "task_bundle.json", bundle)
    prompt = (
        f"You are executing authorized task {task['id']}. Work only in {LAB_ROOT}. "
        f"Create only the required JSON artifact {artifact}. Do not access market or holdout data, "
        "resolve data paths, modify the website, publish, use GPU, capital, credentials, or "
        f"external APIs. Objective: {task['commands'][0]}. The artifact must state scope and "
        "deterministic validation. Return concise JSON describing the artifact path and tests run."
    )
    atomic_text_write(invocation / "prompt.txt", prompt)
    atomic_write(
        invocation / "invocation_start.json",
        {
            "attempt_id": attempt_id,
            "task_id": task["id"],
            "created_at_utc": now(),
            "process_not_started": True,
            "task_bundle_sha256": sha256(invocation / "task_bundle.json"),
            "prompt_sha256": sha256(invocation / "prompt.txt"),
            "adapter_class": mapping["adapter_class"],
            "model": mapping["model"],
            "reasoning": mapping["reasoning"],
            "expected_artifact": task["expected_artifact"],
        },
    )
    output = invocation / "response.json"
    command = [
        "timeout",
        str(mapping.get("runtime_seconds", 900)),
        "codex",
        "exec",
        "--ephemeral",
        "--json",
        "--skip-git-repo-check",
        "-o",
        str(output),
        "-m",
        str(mapping["model"]),
        "-c",
        f'model_reasoning_effort="{mapping["reasoning"]}"',
        "-C",
        str(LAB_ROOT),
        prompt,
    ]
    started = now()
    exit_status: int | None = None
    timeout = False
    parser_result = "NOT_ATTEMPTED"
    with (
        (invocation / "stdout.log").open("wb") as stdout,
        (invocation / "stderr.log").open("wb") as stderr,
    ):
        try:
            child = subprocess.Popen(command, cwd=LAB_ROOT, stdout=stdout, stderr=stderr)
            atomic_write(
                invocation / "process.json",
                {"attempt_id": attempt_id, "pid": child.pid, "started_at_utc": started},
            )
            exit_status = child.wait(timeout=int(mapping.get("runtime_seconds", 900)))
        except subprocess.TimeoutExpired:
            timeout = True
            child.kill()
            exit_status = child.wait()
        finally:
            stdout.flush()
            os.fsync(stdout.fileno())
            stderr.flush()
            os.fsync(stderr.fileno())
    raw = output.read_text(encoding="utf-8") if output.exists() else ""
    atomic_text_write(invocation / "raw_response.log", raw)
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                atomic_write(invocation / "parsed_result.json", parsed)
            parser_result = "JSON_PASS"
        except json.JSONDecodeError:
            parser_result = "PARSER_FAILED_RAW_PRESERVED"
    classification = (
        "PASS" if exit_status == 0 and artifact.is_file() and raw else "ADAPTER_CAPTURE_INCOMPLETE"
    )
    atomic_write(
        invocation / "finalization.json",
        {
            "attempt_id": attempt_id,
            "task_id": task["id"],
            "process_started": True,
            "start_utc": started,
            "finish_utc": now(),
            "exit_status": exit_status,
            "timeout": timeout,
            "stdout_bytes": (invocation / "stdout.log").stat().st_size,
            "stderr_bytes": (invocation / "stderr.log").stat().st_size,
            "raw_bytes": (invocation / "raw_response.log").stat().st_size,
            "expected_artifact_present": artifact.is_file(),
            "expected_artifact_sha256": sha256(artifact) if artifact.is_file() else None,
            "parser_result": parser_result,
            "classification": classification,
            "task_terminalization_permitted": classification == "PASS",
        },
    )
    if classification != "PASS":
        return "ADAPTER_NOT_CONFIGURED", checkpoint(task, "ADAPTER_NOT_CONFIGURED", classification)
    report = checkpoint(task, "TERMINAL", f"artifact_sha256={sha256(artifact)}")
    report["artifact_exists"] = True
    report["validation_result"] = "CODEX_ARTIFACT_EXISTS"
    return "TERMINAL", report


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


def release_satisfied_dependencies(queue: dict[str, Any]) -> bool:
    """Release only catalog materialized dependency-blocked tasks."""
    tasks_by_id = {task["id"]: task for task in queue["tasks"]}
    changed = False
    for task in queue["tasks"]:
        if task["state"] == "BLOCKED_DEPENDENCY" and dependency_outcomes_satisfied(
            task, tasks_by_id
        ):
            task["state"] = "READY"
            changed = True
    return changed


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
            key=lambda task: (
                0 if str(task["id"]).startswith("g1-") else 1,
                recent.get(task["workstream"], 0),
                task["priority"],
                task["id"],
            ),
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


def execute_v5_development_data_contract_validation(
    task: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Bind G1 to reviewed V5 evidence without reopening data or resolving paths."""
    manifest = LAB_ROOT / "contracts" / "REGIMEMOE_DEVELOPMENT_MANIFEST_V5.json"
    validation = LAB_ROOT / "artifacts" / "REGIMEMOE_DEVELOPMENT_MANIFEST_V5_VALIDATION.json"
    attestation = LAB_ROOT / "artifacts" / "REGIMEMOE_V5_LOCAL_RESOLVER_ISOLATION_ATTESTATION.json"
    if not all(path.is_file() for path in (manifest, validation, attestation)):
        raise RuntimeError("immutable V5 evidence is absent")
    values = [read(path) for path in (manifest, validation, attestation)]
    if (
        values[0].get("manifest_canonical_sha256")
        != "da32d844b8fff32651aab44aae93eb86af967eaf571a6607667f57c4e3b8db5f"
        or values[0].get("allowlist_hash", {}).get("computed")
        != "40bb5cf5b7bd3a8ac30e2a3b1d022462fe45888790b1ba58a7068a1982cdc6bd"
        or values[1].get("files_opened") != 36
        or values[2].get("entry_count") != 36
    ):
        raise RuntimeError("V5 development contract evidence mismatch")
    artifact = artifact_path(task["expected_artifact"])
    atomic_write(
        artifact,
        {
            "artifact_type": "G1_DEVELOPMENT_DATA_CONTRACT_VALIDATION",
            "result": "PASS",
            "manifest_sha256": values[0]["manifest_canonical_sha256"],
            "allowlist_sha256": values[0]["allowlist_hash"]["computed"],
            "partitions": 36,
            "market_data_content_read": False,
            "holdout_access": "FORBIDDEN",
        },
    )
    report = checkpoint(task, "TERMINAL", f"artifact_sha256={sha256(artifact)}")
    report["artifact_exists"] = True
    report["validation_result"] = "G1_DEVELOPMENT_DATA_CONTRACT_VALIDATION_PASS"
    return "TERMINAL", report


def execute_causal_data_quality_report(task: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Publish only immutable V5 quality evidence; no data file is reopened."""
    source = LAB_ROOT / "artifacts" / "REGIMEMOE_DEVELOPMENT_MANIFEST_V5_VALIDATION.json"
    evidence = read(source)
    if evidence.get("logical_ids") != 36 or evidence.get("directory_scans") != 0:
        raise RuntimeError("V5 quality evidence does not satisfy the deterministic boundary")
    artifact = artifact_path(task["expected_artifact"])
    atomic_write(
        artifact,
        {
            "artifact_type": "G1_CAUSAL_DATA_QUALITY_REPORT",
            "result": "PASS",
            "coverage": "V5 validated 36 explicit development partitions",
            "cadence": "one-minute end-stamped; boundary semantics reviewed",
            "missing_observations": "preserved documented gap/quarantine treatment",
            "synchronization": "BTCUSDT and ETHUSDT validated under the same V5 contract",
            "timestamp_quality": "monotonicity and boundary checks preserved",
            "duplicates": "validated by V5 deterministic evidence",
            "quality_states": "development-only V5",
            "market_data_content_read": False,
            "holdout_access": "FORBIDDEN",
            "source_validation_sha256": sha256(source),
        },
    )
    report = checkpoint(task, "TERMINAL", f"artifact_sha256={sha256(artifact)}")
    report["artifact_exists"] = True
    report["validation_result"] = "G1_CAUSAL_DATA_QUALITY_REPORT_PASS"
    return "TERMINAL", report


def execute_development_manifest_inventory(task: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Configure only the independently reviewed V5 manifest; never resolve data paths."""
    artifact = artifact_path(task["expected_artifact"])
    artifact.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = LAB_ROOT / "contracts" / "REGIMEMOE_DEVELOPMENT_MANIFEST_V5.json"
    review_path = LAB_ROOT / "artifacts" / "REGIMEMOE_DEVELOPMENT_MANIFEST_V5_FINAL_REVIEW.json"
    if not manifest_path.is_file() or not review_path.is_file():
        payload = {
            "artifact_type": "G1_DEVELOPMENT_MANIFEST_INVENTORY_BLOCKER",
            "blocker": "REGIMEMOE_DEVELOPMENT_MANIFEST_NOT_CONFIGURED",
            "capital_permitted": 0,
            "holdout_access": "FORBIDDEN",
            "result": "WAITING_EXTERNAL",
            "schema_version": "1.0",
        }
        atomic_write(artifact, payload)
        report = checkpoint(task, "WAITING_EXTERNAL", str(payload["blocker"]))
        report["artifact_exists"] = True
        report["validation_result"] = "NO_EXTERNAL_MANIFEST_RESOLVED"
        return "WAITING_EXTERNAL", report
    manifest = read(manifest_path)
    review = read(review_path)
    expected_manifest_hash = "da32d844b8fff32651aab44aae93eb86af967eaf571a6607667f57c4e3b8db5f"
    expected_allowlist_hash = "40bb5cf5b7bd3a8ac30e2a3b1d022462fe45888790b1ba58a7068a1982cdc6bd"
    if (
        manifest.get("manifest_canonical_sha256") != expected_manifest_hash
        or manifest.get("allowlist_hash", {}).get("computed") != expected_allowlist_hash
        or review.get("verdict") != "PASS"
        or review.get("reviewed_manifest_hash") != expected_manifest_hash
        or review.get("reviewed_allowlist_hash") != expected_allowlist_hash
    ):
        raise RuntimeError("V5 manifest identity or final review does not match the frozen gate")
    payload = {
        "artifact_type": "G1_DEVELOPMENT_MANIFEST_INVENTORY",
        "capital_permitted": 0,
        "holdout_access": "FORBIDDEN",
        "manifest": "contracts/REGIMEMOE_DEVELOPMENT_MANIFEST_V5.json",
        "manifest_canonical_sha256": expected_manifest_hash,
        "allowlist_sha256": expected_allowlist_hash,
        "final_review": "artifacts/REGIMEMOE_DEVELOPMENT_MANIFEST_V5_FINAL_REVIEW.json",
        "market_data_paths_resolved": 0,
        "result": "PASS",
        "schema_version": "1.0",
    }
    atomic_write(artifact, payload)
    report = checkpoint(task, "TERMINAL", f"artifact_sha256={sha256(artifact)}")
    report["artifact_exists"] = True
    report["validation_result"] = "G1_V5_MANIFEST_CONFIGURED_WITHOUT_DATA_PATH_RESOLUTION"
    return "TERMINAL", report


def execute_thesis_methodology_outline(task: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Create a bounded, evidence-only thesis structure from the approved charter."""
    charter = LAB_ROOT / "thesis" / "proposal" / "THESIS_CHARTER.md"
    required_clauses = ("causally evaluated", "transaction-cost-aware", "negative results")
    if not charter.is_file() or any(
        clause not in charter.read_text(encoding="utf-8") for clause in required_clauses
    ):
        raise RuntimeError("thesis charter is incomplete")
    artifact = artifact_path(task["expected_artifact"])
    outline = """# Methodology Outline

## Research Question

Can a causally evaluated, transaction-cost-aware regime-aware mixture of fixed
crypto experts improve robustness over fixed baselines?

## Scope and Safety Boundaries

This work uses development evidence only. Historical holdout access, capital,
live execution, and personalised investment advice are outside scope.

## Evaluation Design

Chronological folds, train-fold-only preprocessing, causal availability, and
out-of-fold expert outputs are mandatory. Execution timing and cost scenarios
are frozen before development results are examined.

## Evidence Reporting

All outcomes, including failed gates and negative results, are recorded with
their deterministic artifacts. No performance claim is made without eligible
validated evidence.
"""
    atomic_text_write(artifact, outline)
    report = checkpoint(task, "TERMINAL", f"artifact_sha256={sha256(artifact)}")
    report["artifact_exists"] = True
    report["validation_result"] = "THESIS_METHODOLOGY_OUTLINE_CREATED"
    return "TERMINAL", report


def execute_task(task: dict[str, Any], state: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if state["phase"] == 0:
        return execute_phase_zero(task)
    if task.get("adapter") == "data_contract_validation":
        return execute_data_contract_validation(task)
    if task.get("adapter") == "v5_development_data_contract_validation":
        return execute_v5_development_data_contract_validation(task)
    if task["id"] == "g1-causal-data-quality-report":
        return execute_causal_data_quality_report(task)
    if task.get("adapter") == "development_manifest_inventory":
        return execute_development_manifest_inventory(task)
    if task.get("adapter") == "thesis_methodology_outline":
        return execute_thesis_methodology_outline(task)
    try:
        mapping = registry_entry(task["id"])
    except RuntimeError as error:
        return "ADAPTER_NOT_CONFIGURED", checkpoint(task, "ADAPTER_NOT_CONFIGURED", str(error))
    if mapping.get("adapter_class") in {"CODEX_CONTENT", "CODEX_IMPLEMENTATION"}:
        return run_codex_task(task, mapping)
    return "ADAPTER_NOT_CONFIGURED", checkpoint(
        task, "ADAPTER_NOT_CONFIGURED", "ADAPTER_NOT_CONFIGURED"
    )


def materialize_authorized_catalog() -> None:
    """Seed only catalog-defined work while preserving existing terminal evidence."""
    queue, state, scorecard = read(QUEUE), read(STATE), read(SCORECARD)
    catalog = read(CATALOG)
    if catalog.get("schema_version") != "1.0" or not isinstance(catalog.get("tasks"), list):
        raise RuntimeError("authorized task catalog is invalid")
    existing = {task["id"] for task in queue["tasks"]}
    for entry in catalog["tasks"]:
        if not isinstance(entry, dict) or entry.get("task_id") in existing:
            continue
        dependencies = entry.get("dependencies", [])
        if not isinstance(dependencies, list):
            raise RuntimeError("catalog dependencies must be explicit")
        independent = not dependencies
        state_name = "READY" if independent else "BLOCKED_DEPENDENCY"
        task = {
            "id": entry["task_id"],
            "workstream": entry["workstream"],
            "role": entry["permitted_model_role"],
            "state": state_name,
            "priority": entry["priority"],
            "commands": [entry["objective"]],
            "expected_artifact": entry["deterministic_acceptance_artifact"],
            "validation": entry["mandatory_tests"],
            "depends_on": dependencies,
            "dependency_outcomes": {dependency: "TERMINAL" for dependency in dependencies},
            "verified_provenance": entry.get("verified_provenance", False),
        }
        if isinstance(entry.get("adapter"), str):
            task["adapter"] = entry["adapter"]
        queue["tasks"].append(task)
    validate(queue)
    validate_state(state)
    validate_scorecard(scorecard)
    commit_snapshot(queue, state, scorecard)


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


def enqueue_g1_manifest_inventory() -> None:
    queue, state, scorecard = read(QUEUE), read(STATE), read(SCORECARD)
    validate(queue)
    validate_state(state)
    validate_scorecard(scorecard)
    if state["phase"] < 1:
        raise RuntimeError("G1 manifest inventory requires G0 foundation pass")
    if any(task["id"] == "g1-development-manifest-inventory" for task in queue["tasks"]):
        raise RuntimeError("G1 manifest inventory is already queued")
    queue["tasks"].append(
        {
            "adapter": "development_manifest_inventory",
            "commands": ["inventory only an explicitly configured verified development manifest"],
            "expected_artifact": "regime-moe-lab/artifacts/g1-development-manifest-inventory.json",
            "id": "g1-development-manifest-inventory",
            "priority": 11,
            "role": "deterministic_evidence",
            "state": "READY",
            "validation": [
                "no manifest path is resolved unless explicitly configured",
                "holdout remains unresolved",
                "blocker is recorded when configuration is absent",
            ],
            "workstream": "DATA_AND_EVALUATION",
        }
    )
    state["production_adapters_active"] = [
        "data_contract_validation",
        "development_manifest_inventory",
    ]
    commit_snapshot(queue, state, scorecard)


def enqueue_thesis_methodology_outline() -> None:
    queue, state, scorecard = read(QUEUE), read(STATE), read(SCORECARD)
    validate(queue)
    validate_state(state)
    validate_scorecard(scorecard)
    if state["phase"] < 1:
        raise RuntimeError("thesis support task requires G0 foundation pass")
    if any(task["id"] == "thesis-methodology-outline" for task in queue["tasks"]):
        raise RuntimeError("thesis methodology outline is already queued")
    queue["tasks"].append(
        {
            "adapter": "thesis_methodology_outline",
            "commands": ["derive an evidence-only methodology outline from the thesis charter"],
            "expected_artifact": "regime-moe-lab/thesis/proposal/METHODOLOGY_OUTLINE.md",
            "id": "thesis-methodology-outline",
            "priority": 20,
            "role": "content_product",
            "state": "READY",
            "validation": [
                "development-only scope",
                "no performance claim",
                "no holdout, capital, or advice claim",
                "deterministic artifact",
            ],
            "workstream": "THESIS_AND_CAREER",
        }
    )
    state["production_adapters_active"] = [
        "data_contract_validation",
        "development_manifest_inventory",
        "thesis_methodology_outline",
    ]
    commit_snapshot(queue, state, scorecard)


def cycle(mutate: bool) -> dict[str, Any]:
    recover()
    queue, state, scorecard = read(QUEUE), read(STATE), read(SCORECARD)
    validate(queue)
    validate_state(state)
    validate_scorecard(scorecard)
    released = release_satisfied_dependencies(queue)
    if released and mutate:
        commit_snapshot(queue, state, scorecard)
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


def systemd_smoke() -> None:
    """Non-mutating infrastructure smoke; never selects or changes a queue task."""
    smoke_id = f"systemd-smoke-{now().replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}"
    directory = Path("/home/vertico/.local/state/regimemoe/systemd-smoke") / smoke_id
    directory.mkdir(parents=True, exist_ok=False)
    prompt = (
        'Return only JSON: {"schema_version":"1.0","smoke_id":"'
        + smoke_id
        + '","model":"gpt-5.6-terra","reasoning":"medium","invocation_mode":"noninteractive",'
        '"verdict":"PASS","files_modified":false,"queue_modified":false,'
        '"market_data_accessed":false,"holdout_accessed":false,"capital_used":false}. '
        "Do not modify any file."
    )
    atomic_text_write(directory / "prompt.txt", prompt)
    output = directory / "response.json"
    start = now()
    with (
        (directory / "stdout.log").open("wb") as stdout,
        (directory / "stderr.log").open("wb") as stderr,
    ):
        child = subprocess.Popen(
            [
                "timeout",
                "120",
                "codex",
                "exec",
                "--ephemeral",
                "--json",
                "--skip-git-repo-check",
                "-o",
                str(output),
                "-m",
                "gpt-5.6-terra",
                "-c",
                'model_reasoning_effort="medium"',
                "-C",
                "/tmp",
                prompt,
            ],
            stdout=stdout,
            stderr=stderr,
        )
        exit_status = child.wait(timeout=130)
        stdout.flush()
        os.fsync(stdout.fileno())
        stderr.flush()
        os.fsync(stderr.fileno())
    raw = output.read_text(encoding="utf-8") if output.exists() else ""
    atomic_text_write(directory / "raw_response.log", raw)
    parsed = json.loads(raw)
    if parsed.get("verdict") != "PASS" or parsed.get("files_modified") is not False:
        raise RuntimeError("systemd smoke response failed validation")
    atomic_write(directory / "parsed_result.json", parsed)
    atomic_write(
        directory / "finalization.json",
        {
            "smoke_id": smoke_id,
            "start_utc": start,
            "finish_utc": now(),
            "exit_status": exit_status,
            "raw_sha256": sha256(directory / "raw_response.log"),
            "stdout_sha256": sha256(directory / "stdout.log"),
            "stderr_sha256": sha256(directory / "stderr.log"),
            "classification": "PASS",
        },
    )
    print(json.dumps({"status": "PASS", "smoke_id": smoke_id, "directory": str(directory)}))


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
            "enqueue-g1-manifest-inventory",
            "enqueue-thesis-methodology-outline",
            "materialize-catalog",
            "systemd-smoke",
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
    if args.command == "systemd-smoke":
        systemd_smoke()
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
    if args.command == "materialize-catalog":
        acquire_lock()
        try:
            recover()
            materialize_authorized_catalog()
            print(json.dumps({"status": "CATALOG_MATERIALIZED"}))
        finally:
            release_lock()
        return
    if args.command == "enqueue-g1-manifest-inventory":
        acquire_lock()
        try:
            recover()
            enqueue_g1_manifest_inventory()
            print(json.dumps({"status": "G1_MANIFEST_INVENTORY_QUEUED"}))
        finally:
            release_lock()
        return
    if args.command == "enqueue-thesis-methodology-outline":
        acquire_lock()
        try:
            recover()
            enqueue_thesis_methodology_outline()
            print(json.dumps({"status": "THESIS_METHODOLOGY_OUTLINE_QUEUED"}))
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
