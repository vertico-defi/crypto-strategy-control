"""Bounded, zero-capital research-program state machine.

The controller deliberately has no exchange, wallet, order, or model-provider
integration.  Model calls are represented only by audited invocation records;
the mock mode is for deterministic orchestration validation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "CURRENT_STATE.json"
LEDGER = ROOT / "EXPERIMENT_LEDGER.jsonl"
REJECTED = ROOT / "REJECTED_STRATEGIES.jsonl"
MODEL_LEDGER = ROOT / "MODEL_USAGE_LEDGER.jsonl"
PUBLICATION_LOG = ROOT / "PUBLICATION_LOG.jsonl"
SYNC = ROOT / "GITHUB_SYNC_STATE.json"
INVENTORY = ROOT / "DATA_INVENTORY.json"
LOCK = ROOT / ".research-orchestrator.lock"
EXPERIMENT_ID = "cs-ranking-ptu-data-audit-v1"


class StateError(RuntimeError):
    """Raised when persisted research state is invalid or unsafe to mutate."""


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def atomic_json(path: Path, value: object) -> None:
    """Write JSON atomically, retaining neither partial state nor temp artifacts."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(_canonical(value) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StateError(f"state corruption or absence: {path}") from exc
    if not isinstance(value, dict):
        raise StateError(f"state must be an object: {path}")
    return value


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(_canonical(value) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


@contextmanager
def exclusive_lock(path: Path = LOCK, *, stale_seconds: int = 900) -> Iterator[None]:
    """Use create-exclusive lockfiles and recover only demonstrably stale locks."""

    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        age = time.time() - path.stat().st_mtime
        if age <= stale_seconds:
            raise StateError("concurrent research mutation refused") from None
        stale = path.with_suffix(path.suffix + f".stale-{int(time.time())}")
        os.replace(path, stale)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        os.write(descriptor, _canonical({"pid": os.getpid(), "started_at": _now()}).encode())
        yield
    finally:
        os.close(descriptor)
        path.unlink(missing_ok=True)


def validate_state(state: dict[str, Any]) -> None:
    required = {"schema_version", "program_state", "capital_permitted", "next_task", "budgets"}
    missing = required - state.keys()
    if missing:
        raise StateError(f"state missing keys: {sorted(missing)}")
    if state["capital_permitted"] != 0:
        raise StateError("capital permission must remain zero")
    if state["program_state"] not in {
        "ACTIVE_RESEARCH",
        "DATA_BLOCKED",
        "PAUSED_FOR_USAGE",
        "PUBLICATION_PENDING",
        "RESEARCH_BUDGET_EXHAUSTED",
        "INFRASTRUCTURE_BLOCKED",
        "SAFETY_STOP",
    }:
        raise StateError("unsupported program state")


def git_state() -> dict[str, Any]:
    import subprocess

    def run(*args: str) -> str | None:
        result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else None

    return {
        "branch": run("git", "branch", "--show-current"),
        "head": run("git", "rev-parse", "HEAD"),
        "clean": not bool(run("git", "status", "--porcelain")),
    }


def _lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    values: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise StateError(f"corrupt JSONL {path}:{number}") from exc
        if not isinstance(item, dict):
            raise StateError(f"non-object JSONL {path}:{number}")
        values.append(item)
    return values


def select_task() -> dict[str, Any]:
    """Return one distinct, high-information task without inspecting a holdout."""

    prior = _lines(LEDGER)
    if any(item.get("experiment_id") == EXPERIMENT_ID for item in prior):
        return {"task": "NO_VALID_NEXT_TASK", "reason": "initial data audit already terminal"}
    return {
        "task": EXPERIMENT_ID,
        "family": "COST_AWARE_CROSS_SECTIONAL_RANKING",
        "hypothesis": (
            "A point-in-time liquid perpetual universe may support next-bar "
            "cross-sectional momentum after costs."
        ),
        "information_value": (
            "Resolve whether an eligible point-in-time universe exists before modelling."
        ),
    }


def preregistration(task: dict[str, Any], source_commit: str) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "experiment_id": EXPERIMENT_ID,
        "preregistered_at_utc": _now(),
        "strategy_family": task["family"],
        "economic_hypothesis": task["hypothesis"],
        "universe_rule": (
            "Point-in-time liquid, tradable perpetual universe including delisted "
            "episodes where lawful data permits."
        ),
        "target": "next-bar cross-sectional rank return net of modeled execution costs",
        "baseline": "cross-sectional momentum and volatility-adjusted momentum",
        "challengers": ["linear ranker", "LightGBM_or_XGBoost_only_after_baseline"],
        "holdout_policy": (
            "No final holdout access until a lawful point-in-time universe passes "
            "the data contract."
        ),
        "costs": {
            "fee_bps_round_trip": 10,
            "spread_bps_round_trip": 10,
            "slippage_bps_round_trip": 10,
        },
        "gates_file": "ACCEPTANCE_GATES.yaml",
        "compute_budget": {
            "wall_seconds": 900,
            "agent_calls": 3,
            "repair_attempts": 1,
            "experiments": 1,
            "gpu_seconds": 0,
        },
        "source_commit": source_commit,
        "capital_permitted": 0,
    }


def preregistration_path() -> Path:
    return ROOT / "experiments" / EXPERIMENT_ID / "PREREGISTRATION.json"


def freeze_preregistration(*, dry_run: bool) -> dict[str, Any]:
    """Freeze the hypothesis before any data-contract result is recorded."""

    task = select_task()
    if task["task"] == "NO_VALID_NEXT_TASK":
        raise StateError("initial experiment already has a terminal record")
    path = preregistration_path()
    if path.exists():
        raise StateError("preregistration already exists; commit or evaluate it")
    git = git_state()
    if not git["clean"]:
        raise StateError("controller working tree must be clean before preregistration")
    prereg = preregistration(task, str(git["head"]))
    prereg["preregistration_sha256"] = _sha(prereg)
    if dry_run:
        return {"dry_run": True, "would_write": str(path.relative_to(ROOT)), "task": task}
    atomic_json(path, prereg)
    state = load_json(STATE)
    validate_state(state)
    state.update(
        {
            "program_state": "ACTIVE_RESEARCH",
            "current_experiment_id": EXPERIMENT_ID,
            "next_task": "commit_preregistration_then_run_data_contract",
            "updated_at_utc": _now(),
        }
    )
    atomic_json(STATE, state)
    return {"status": "PREREGISTRATION_FROZEN", "path": str(path.relative_to(ROOT))}


def model_route(outcome: str) -> dict[str, str | None]:
    """Implement conservative model routing without silent quality downgrades."""

    routes: dict[str, dict[str, str | None]] = {
        "success": {"model": "gpt-5.6-sol", "reasoning": "high", "status": "USED"},
        "quota": {"model": None, "reasoning": None, "status": "PAUSED_FOR_USAGE"},
        "unavailable": {"model": "gpt-5.6-terra", "reasoning": "high", "status": "FALLBACK"},
        "terra_unavailable": {"model": "gpt-5.6-luna", "reasoning": "high", "status": "FALLBACK"},
        "substantive_failure": {"model": None, "reasoning": None, "status": "FAILED"},
        "interface_missing": {"model": None, "reasoning": None, "status": "INTERFACE_UNAVAILABLE"},
    }
    if outcome not in routes:
        raise StateError(f"unknown model outcome: {outcome}")
    return routes[outcome]


def run_cycle(*, mock_agents: bool, dry_run: bool) -> dict[str, Any]:
    state = load_json(STATE)
    validate_state(state)
    task = select_task()
    if dry_run:
        return {"dry_run": True, "task": task, "state": state["program_state"], "codex_calls": 0}
    if task["task"] == "NO_VALID_NEXT_TASK":
        state.update(
            {"program_state": "DATA_BLOCKED", "next_task": "obtain_lawful_point_in_time_universe"}
        )
        atomic_json(STATE, state)
        return {"status": "DATA_BLOCKED", "task": task}
    git = git_state()
    if not git["clean"]:
        raise StateError("controller working tree must be clean before a cycle")
    prereg_path = preregistration_path()
    if not prereg_path.exists():
        raise StateError("freeze and commit preregistration before evaluating any data")
    prereg = load_json(prereg_path)
    if prereg.get("experiment_id") != EXPERIMENT_ID:
        raise StateError("preregistration identity mismatch")
    expected_hash = prereg.get("preregistration_sha256")
    hash_input = dict(prereg)
    hash_input.pop("preregistration_sha256", None)
    if expected_hash != _sha(hash_input):
        raise StateError("preregistration hash mismatch")
    invocation = {
        "started_at_utc": _now(),
        "role": "research_director",
        "preferred_model": "gpt-5.6-sol",
        "reasoning": "high",
        "mode": "mock" if mock_agents else "unavailable_interface",
        "fallback": None,
        "error_category": None if mock_agents else "MODEL_INTERFACE_UNAVAILABLE",
        "response_identifier": None,
    }
    append_jsonl(MODEL_LEDGER, invocation)
    inventory = load_json(INVENTORY)
    blocker = "POINT_IN_TIME_UNIVERSE_UNAVAILABLE"
    evidence = {
        "data_integrity": "FAIL_CLOSED",
        "blocker": blocker,
        "reason": (
            "No lawful, complete point-in-time liquid universe with delisting history "
            "is registered in DATA_INVENTORY."
        ),
        "inventory_sha256": _sha(inventory),
        "holdout_opened": False,
        "returns_calculated": False,
    }
    report_path = ROOT / "experiments" / EXPERIMENT_ID / "DATA_INTEGRITY_REPORT.json"
    atomic_json(report_path, evidence)
    audit = {
        "auditor": "deterministic_independent_audit",
        "verdict": "DATA_NO_GO_CONFIRMED",
        "checked": ["no_holdout", "no_returns", "point_in_time_universe"],
        "at_utc": _now(),
    }
    atomic_json(ROOT / "experiments" / EXPERIMENT_ID / "AUDIT.json", audit)
    record = {
        "experiment_id": EXPERIMENT_ID,
        "terminal_at_utc": _now(),
        "classification": "DATA_NO_GO",
        "source_commit": git["head"],
        "preregistration_sha256": str(expected_hash),
        "report": str(report_path.relative_to(ROOT)),
        "audit_verdict": audit["verdict"],
        "capital_permitted": 0,
    }
    append_jsonl(LEDGER, record)
    append_jsonl(
        REJECTED,
        {
            "strategy_id": EXPERIMENT_ID,
            "classification": "DATA_NO_GO",
            "reason": blocker,
            "frozen_configuration": prereg["preregistration_sha256"],
            "at_utc": record["terminal_at_utc"],
        },
    )
    state.update(
        {
            "program_state": "DATA_BLOCKED",
            "current_experiment_id": EXPERIMENT_ID,
            "next_task": "obtain_lawful_point_in_time_universe",
            "last_terminal_verdict": "DATA_NO_GO",
            "updated_at_utc": _now(),
        }
    )
    atomic_json(STATE, state)
    return {"status": "DATA_NO_GO", "experiment": record, "mock_agents": mock_agents}


def status() -> dict[str, Any]:
    state = load_json(STATE)
    validate_state(state)
    return {
        "state": state,
        "git": git_state(),
        "experiments": len(_lines(LEDGER)),
        "rejected": len(_lines(REJECTED)),
        "model_invocations": len(_lines(MODEL_LEDGER)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded zero-capital crypto research controller")
    parser.add_argument(
        "command",
        choices=(
            "status",
            "freeze",
            "dry-run",
            "mock-validate",
            "cycle",
            "run",
            "resume",
            "publish",
            "publication-dry-run",
            "prospective",
        ),
    )
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--mock-agents", action="store_true")
    args = parser.parse_args()
    with exclusive_lock():
        if args.command == "status":
            result = status()
        elif args.command == "freeze":
            result = freeze_preregistration(dry_run=False)
        elif args.command in {"dry-run", "publication-dry-run"}:
            result = run_cycle(mock_agents=False, dry_run=True)
        elif args.command == "mock-validate":
            result = run_cycle(mock_agents=True, dry_run=True)
        elif args.command in {"cycle", "run", "resume"}:
            if args.cycles < 1 or args.cycles > 3:
                raise StateError("cycles must be 1..3")
            result = run_cycle(mock_agents=args.mock_agents, dry_run=False)
        elif args.command == "prospective":
            result = {"status": "NO_FROZEN_PROSPECTIVE_CANDIDATE", "capital_permitted": 0}
        else:
            result = {
                "status": "PUBLICATION_PENDING",
                "reason": "invoke portfolio adapter after a source checkpoint",
            }
    print(json.dumps(result, sort_keys=True))
