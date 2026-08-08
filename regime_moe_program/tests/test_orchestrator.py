import json
import os
import subprocess
import sys
import time
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
