"""Read-only repository and systemd inspection adapters."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from strategy_control.model import StrategyConfig


def _run(*args: str) -> str | None:
    try:
        result = subprocess.run(args, text=True, capture_output=True, check=False, timeout=10)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _unit_state(unit: str | None) -> str:
    if unit is None:
        return "not_configured"
    value = _run("systemctl", "--user", "is-active", unit)
    return value if value is not None else "unknown_or_inactive"


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _perp_carry_audit_state(audit_root: Path | None = None) -> dict[str, Any] | None:
    """Read the newest nonterminal Perp Carry audit without touching its namespace."""
    root = audit_root or Path("/home/vertico/.local/share/perp-carry-lab/audits")
    candidates = sorted(root.glob("*/operations/audits/*/audit-start.json"))
    active: list[tuple[str, Path, dict[str, Any]]] = []
    for start_path in candidates:
        record = _load_json(start_path)
        if record is None or record.get("status") != "active":
            continue
        audit_root = start_path.parents[3]
        audit_id = str(record.get("audit_id", ""))
        if (
            not audit_id
            or (
                audit_root / "operations" / "audits" / audit_id / "collector-complete.json"
            ).exists()
        ):
            continue
        active.append((str(record.get("recorded_at_utc", "")), audit_root, record))
    if not active:
        return None
    _, audit_root, record = max(active, key=lambda item: item[0])
    audit_id = str(record["audit_id"])
    health = _load_json(audit_root / "state" / "health.json") or {}
    return {
        "audit_id": audit_id,
        "audit_root": audit_root,
        "record": record,
        "health": health,
        "service_state": _unit_state(f"perp-carry-lab-audit@{audit_id}.service"),
        "timer_state": _unit_state(f"perp-carry-lab-audit-clock@{audit_id}.timer"),
    }


def inspect(config: StrategyConfig) -> dict[str, Any]:
    """Return a snapshot assembled exclusively from reads and subprocess queries."""
    repo = Path(config.repository)
    warnings: list[str] = []
    if not repo.is_dir():
        warnings.append("registered repository is absent")
    branch = _run("git", "-C", str(repo), "branch", "--show-current") if repo.is_dir() else None
    commit = _run("git", "-C", str(repo), "rev-parse", "HEAD") if repo.is_dir() else None
    porcelain = _run("git", "-C", str(repo), "status", "--porcelain") if repo.is_dir() else None
    if porcelain:
        warnings.append("registered repository working tree is not clean")
    hashes: dict[str, str] = {}
    for relative in config.artifact_files:
        candidate = repo / relative
        if candidate.is_file():
            hashes[relative] = _sha256(candidate)
        else:
            warnings.append(f"configured artifact absent: {relative}")
    audit = _perp_carry_audit_state() if config.strategy_id == "perp-carry-v1" else None
    if config.strategy_id == "perp-carry-v1" and audit is None:
        warnings.append("clean bounded post-lifecycle-repair 24-hour audit has not been evidenced")
    if audit is not None:
        warnings.append(
            "clean bounded 24-hour audit is active; all reliability results are provisional"
        )
    if config.strategy_id.startswith("ctrend-"):
        warnings.append("capital is zero; this strategy is research-only")
    return {
        "strategy_id": config.strategy_id,
        "strategy_class": config.strategy_class,
        "repository": str(repo),
        "branch": branch,
        "current_commit": commit,
        "dataset_id": str(audit["audit_root"]) if audit is not None else config.dataset_id,
        "experiment_or_run_id": audit["audit_id"]
        if audit is not None
        else config.experiment_or_run_id,
        "artifact_hashes": hashes,
        "stage": config.stage,
        "latest_verdict": config.latest_verdict,
        "historical_start": (
            audit["record"].get("audit_start_utc") if audit is not None else config.historical_start
        ),
        "historical_end": (
            audit["record"].get("earliest_valid_completion_utc")
            if audit is not None
            else config.historical_end
        ),
        "shadow_start": config.shadow_start,
        "shadow_end": config.shadow_end,
        "service_state": audit["service_state"]
        if audit is not None
        else _unit_state(config.service_unit),
        "timer_state": audit["timer_state"]
        if audit is not None
        else _unit_state(config.timer_unit),
        "last_successful_update": (
            audit["health"].get("last_success_at") if audit is not None else None
        ),
        "last_error": None,
        "observation_count": (
            audit["health"].get("successful_collections") if audit is not None else None
        ),
        "prediction_or_trade_count": None,
        "gross_return": None,
        "net_return": None,
        "fees": None,
        "funding": None,
        "spread_and_slippage": None,
        "maximum_drawdown": None,
        "volatility": None,
        "Sharpe": None,
        "current_exposure": "0.0; no capital permitted",
        "risk_budget": config.risk_budget,
        "next_gate": (
            "Await immutable collector completion, postflight, and read-only finalization"
            if audit is not None
            else config.next_gate
        ),
        "latest_report": config.latest_report,
        "integrity_warnings": warnings,
        "snapshot_observed_at": datetime.now(UTC).isoformat(),
    }
