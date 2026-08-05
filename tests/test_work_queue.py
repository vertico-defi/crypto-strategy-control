import json
from datetime import UTC, datetime

from strategy_control.work_queue import (
    load_queue,
    ready_successor_after_mean_terminal,
    select_task,
)


def _queue(tmp_path):
    source = {
        "writer_lock": {"max_mutating_tasks": 1},
        "tasks": [
            {"task_id": "funding", "state": "WAITING_EXTERNAL", "priority": 3,
             "next_eligible_timestamp": "2026-08-07T14:00:10Z"},
            {"task_id": "mean", "experiment_id": "mean-v3", "state": "READY", "priority": 1},
            {
                "task_id": "relative",
                "state": "BLOCKED_ON_MEAN_REVERSION_V3_TERMINAL",
                "priority": 2,
            },
        ],
    }
    path = tmp_path / "queue.json"
    path.write_text(json.dumps(source))
    return load_queue(path)


def test_ready_route_selected_while_external_waits(tmp_path):
    queue = _queue(tmp_path)
    selected = select_task(queue, datetime(2026, 8, 5, tzinfo=UTC))
    assert selected["task_id"] == "mean"
    assert selected["state"] == "READY"


def test_funding_is_not_eligible_before_exact_timestamp(tmp_path):
    queue = _queue(tmp_path)
    queue["tasks"][1]["state"] = "BLOCKED"
    selected = select_task(queue, datetime(2026, 8, 7, 14, 0, 9, tzinfo=UTC))
    assert selected["state"] == "WAITING_EXTERNAL"
    assert selected["task"] is None


def test_funding_is_eligible_at_exact_timestamp(tmp_path):
    queue = _queue(tmp_path)
    queue["tasks"][1]["state"] = "BLOCKED"
    selected = select_task(queue, datetime(2026, 8, 7, 14, 0, 10, tzinfo=UTC))
    assert selected["task_id"] == "funding"


def test_relative_successor_unblocks_only_on_terminal_noncandidate(tmp_path):
    queue = _queue(tmp_path)
    assert (
        ready_successor_after_mean_terminal(queue, "AUDIT_PENDING")["tasks"][2]["state"]
        != "READY"
    )
    updated = ready_successor_after_mean_terminal(queue, "IMPLEMENTATION_INCONCLUSIVE")
    assert updated["tasks"][2]["state"] == "READY"
