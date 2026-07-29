"""Read-only repository and systemd inspection adapters."""

from __future__ import annotations

import hashlib
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
    if config.strategy_id == "perp-carry-v1":
        warnings.append("clean bounded post-lifecycle-repair 24-hour audit has not been evidenced")
    if config.strategy_id.startswith("ctrend-"):
        warnings.append("capital is zero; this strategy is research-only")
    return {
        "strategy_id": config.strategy_id,
        "strategy_class": config.strategy_class,
        "repository": str(repo),
        "branch": branch,
        "current_commit": commit,
        "dataset_id": config.dataset_id,
        "experiment_or_run_id": config.experiment_or_run_id,
        "artifact_hashes": hashes,
        "stage": config.stage,
        "latest_verdict": config.latest_verdict,
        "historical_start": config.historical_start,
        "historical_end": config.historical_end,
        "shadow_start": config.shadow_start,
        "shadow_end": config.shadow_end,
        "service_state": _unit_state(config.service_unit),
        "timer_state": _unit_state(config.timer_unit),
        "last_successful_update": None,
        "last_error": None,
        "observation_count": None,
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
        "next_gate": config.next_gate,
        "latest_report": config.latest_report,
        "integrity_warnings": warnings,
        "snapshot_observed_at": datetime.now(UTC).isoformat(),
    }
