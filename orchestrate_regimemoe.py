#!/usr/bin/env python3
"""Lean, persistent Phase-0 controller for the RegimeMoE program.

It plans bounded work; it never acquires data, trains models, evaluates a strategy,
opens a holdout, routes orders, or starts a timer.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parent / "regime_moe_program"
QUEUE = ROOT / "REGIMEMOE_WORK_QUEUE.json"
STATE = ROOT / "REGIMEMOE_STATE.json"
SCORECARD = ROOT / "REGIMEMOE_SCORECARD.json"
LOCK = ROOT / ".orchestrator.lock"
STATES = {
    "READY",
    "RUNNING",
    "WAITING_EXTERNAL",
    "BLOCKED_DEPENDENCY",
    "HUMAN_APPROVAL",
    "DEFERRED",
    "TERMINAL",
}
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


def now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("controller JSON root must be an object")
    return cast(dict[str, Any], value)


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as file:
        json.dump(value, file, indent=2, sort_keys=True)
        file.write("\n")
        temporary = Path(file.name)
    os.replace(temporary, path)


def validate(queue: dict[str, Any]) -> None:
    seen: set[str] = set()
    for task in queue["tasks"]:
        if task["id"] in seen or task["state"] not in STATES:
            raise ValueError("invalid queue task")
        seen.add(task["id"])
        if task["workstream"] not in queue["workstreams"] or task["role"] not in ROLES:
            raise ValueError("invalid workstream or role")


def acquire_lock() -> None:
    try:
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        age = datetime.now().timestamp() - LOCK.stat().st_mtime
        if age < 3600:
            raise RuntimeError("exclusive lock held") from error
        LOCK.unlink()
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        json.dump({"pid": os.getpid(), "created_at": now()}, file)


def release_lock() -> None:
    LOCK.unlink(missing_ok=True)


def select(queue: dict[str, Any]) -> dict[str, Any] | None:
    """Choose one independent READY task; waiting work is intentionally ignored."""
    completed = {task["id"] for task in queue["tasks"] if task["state"] == "TERMINAL"}
    candidates = [
        task
        for task in queue["tasks"]
        if task["state"] == "READY" and set(task.get("depends_on", [])) <= completed
    ]
    if not candidates:
        return None
    recent = queue.get("weekly_completed_by_workstream", {})
    return cast(
        dict[str, Any],
        min(
            candidates,
            key=lambda task: (recent.get(task["workstream"], 0), task["priority"], task["id"]),
        ),
    )


def checkpoint(task: dict[str, Any], status: str, blocker: str | None = None) -> dict[str, Any]:
    return {
        "at_utc": now(),
        "task_id": task["id"],
        "workstream": task["workstream"],
        "expected_artifact": task["expected_artifact"],
        "commands": task["commands"],
        "model_role": task["role"],
        "model_route": ROLES[task["role"]],
        "deterministic_validation": task["validation"],
        "status": status,
        "source_commit_or_blocker": blocker or "NO_MUTATION_DRY_OR_PHASE_0_CHECKPOINT",
    }


def cycle(mutate: bool) -> dict[str, Any]:
    queue, state, scorecard = read(QUEUE), read(STATE), read(SCORECARD)
    validate(queue)
    if state["usage_paused"]:
        return {"status": "USAGE_PAUSED", "checkpoint": None}
    task = select(queue)
    if task is None:
        return {"status": "NO_OP", "checkpoint": None}
    report = checkpoint(task, "PLANNED" if not mutate else "COMPLETED_PHASE_0_HARMLESS")
    if mutate:
        task["state"] = "TERMINAL"
        task["last_checkpoint"] = report
        queue.setdefault("weekly_completed_by_workstream", {})[task["workstream"]] = (
            queue.get("weekly_completed_by_workstream", {}).get(task["workstream"], 0) + 1
        )
        scorecard["completed_checkpoints"].append(report)
        state["last_cycle_at_utc"] = now()
        atomic_write(QUEUE, queue)
        atomic_write(SCORECARD, scorecard)
        atomic_write(STATE, state)
    return {"status": report["status"], "checkpoint": report}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=("status", "dry-run", "one-cycle", "multi-cycle", "resume", "weekly-report"),
    )
    parser.add_argument("--max-cycles", type=int, default=1)
    args = parser.parse_args()
    if args.command == "status":
        queue = read(QUEUE)
        validate(queue)
        print(
            json.dumps(
                {
                    "state": read(STATE),
                    "task_counts": {
                        s: sum(t["state"] == s for t in queue["tasks"]) for s in STATES
                    },
                },
                indent=2,
            )
        )
        return
    if args.command == "weekly-report":
        print(json.dumps(read(SCORECARD), indent=2))
        return
    if args.command == "resume":
        state = read(STATE)
        state["usage_paused"] = False
        atomic_write(STATE, state)
        print(json.dumps({"status": "RESUMED"}))
        return
    acquire_lock()
    try:
        if args.command == "dry-run":
            result = cycle(False)
        elif args.command == "one-cycle":
            result = cycle(True)
        else:
            reports = [cycle(True) for _ in range(max(0, args.max_cycles))]
            result = {"status": "MULTI_CYCLE", "reports": reports}
        print(json.dumps(result, indent=2))
    finally:
        release_lock()


if __name__ == "__main__":
    main()
