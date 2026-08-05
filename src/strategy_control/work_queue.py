"""Minimal deterministic non-blocking Phase 3 work queue."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class WorkQueueError(ValueError):
    """Raised when a persisted queue is malformed or unsafe."""


def _time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WorkQueueError(f"invalid UTC timestamp: {value}") from exc
    if parsed.tzinfo is None:
        raise WorkQueueError("queue timestamps must include a timezone")
    return parsed.astimezone(UTC)


def load_queue(path: Path) -> dict[str, Any]:
    try:
        queue = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkQueueError(f"cannot load queue: {path}") from exc
    if not isinstance(queue, dict) or not isinstance(queue.get("tasks"), list):
        raise WorkQueueError("queue must contain a task list")
    if queue.get("writer_lock", {}).get("max_mutating_tasks") != 1:
        raise WorkQueueError("queue requires one exclusive writer")
    return queue


def _eligible(task: dict[str, Any], now: datetime) -> bool:
    if task.get("state") == "READY":
        return True
    if task.get("state") != "WAITING_EXTERNAL":
        return False
    timestamp = task.get("next_eligible_timestamp")
    return isinstance(timestamp, str) and now >= _time(timestamp)


def select_task(queue: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """Select one due task; future external waits never mask READY work."""

    current = (now or datetime.now(UTC)).astimezone(UTC)
    tasks = [task for task in queue["tasks"] if isinstance(task, dict) and _eligible(task, current)]
    if not tasks:
        return {"state": "WAITING_EXTERNAL", "task": None}
    selected = min(
        tasks,
        key=lambda task: (int(task.get("priority", 999)), str(task.get("task_id"))),
    )
    return dict(selected)


def ready_successor_after_mean_terminal(
    queue: dict[str, Any], terminal_state: str
) -> dict[str, Any]:
    """Return a copy with relative-v3 ready only for an allowed terminal result."""

    allowed = {"NO_GO", "IMPLEMENTATION_INCONCLUSIVE", "NONCANDIDATE_TERMINAL"}
    updated = json.loads(json.dumps(queue))
    if terminal_state in allowed:
        for task in updated["tasks"]:
            if task.get("task_id") in {"relative-value-v3-completion", "relative"}:
                task["state"] = "READY"
                task["unblocked_by"] = terminal_state
    return updated
