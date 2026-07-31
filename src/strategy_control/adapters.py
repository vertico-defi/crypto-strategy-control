"""Read-only repository and systemd inspection adapters."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tomllib
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


def _perp_carry_completed_audit_state(audit_root: Path | None = None) -> dict[str, Any] | None:
    """Read the newest finalized Perp Carry audit without modifying evidence."""
    root = audit_root or Path("/home/vertico/.local/share/perp-carry-lab/audits")
    candidates: list[tuple[str, Path, dict[str, Any]]] = []
    for final_path in root.glob("*/operations/audits/*/final/final-audit.json"):
        record = _load_json(final_path)
        if record is None:
            continue
        audit_id = str(record.get("audit_id", ""))
        if not audit_id:
            continue
        candidates.append((str(record.get("generated_at_utc", "")), final_path, record))
    if not candidates:
        return None
    _, final_path, record = max(candidates, key=lambda item: item[0])
    audit_id = str(record["audit_id"])
    root_path = final_path.parents[5]
    metrics = record.get("exact_window_metrics", {})
    quality = metrics.get("quality", {}) if isinstance(metrics, dict) else {}
    clock = metrics.get("clock_health", {}) if isinstance(metrics, dict) else {}
    lifecycle = record.get("lifecycle", {}) if isinstance(record.get("lifecycle"), dict) else {}
    postflight = lifecycle.get("postflight", {}) if isinstance(lifecycle, dict) else {}
    return {
        "audit_id": audit_id,
        "audit_root": root_path,
        "record": record,
        "quality": quality,
        "clock": clock,
        "postflight": postflight,
        "service_state": _unit_state(f"perp-carry-lab-audit@{audit_id}.service"),
        "timer_state": _unit_state(f"perp-carry-lab-audit-clock@{audit_id}.timer"),
        "final_artifact": final_path,
    }


def _ctrend_executable_state(repository: Path) -> dict[str, Any] | None:
    """Read the CTREND evidence ledger without changing its source repository."""
    evidence = _load_json(repository / "reports" / "binance_usdm_instrument_evidence.json")
    if evidence is None:
        return None
    return {
        "retrieval_status": evidence.get("retrieval_status"),
        "catalog_pages_cached": evidence.get("catalog_pages_cached"),
        "candidate_articles": evidence.get("candidate_articles"),
        "instrument_master_sha256": evidence.get("instrument_master_sha256"),
        "generated_at_utc": evidence.get("generated_at_utc"),
    }


def _ctrend_liquidity_state(repository: Path) -> dict[str, Any] | None:
    """Read CTREND liquidity net evidence without changing its repository."""
    summary = _load_json(repository / "reports" / "ctrend_liquidity_net_evaluation_summary.json")
    manifest = _load_json(repository / "reports" / "ctrend_liquidity_net_evaluation_manifest.json")
    gaps = _load_json(repository / "reports" / "ctrend_liquidity_funding_gap_report.json")
    gross = _load_json(
        repository / "reports" / "binance_usdm_liquidity_gross_performance_summary.json"
    )
    if summary is None or manifest is None or gaps is None or gross is None:
        return None
    primary = summary.get("cost_scenarios", {}).get("execution_25bp", {})
    funding = summary.get("funding", {})
    return {
        "primary": primary,
        "funding": funding,
        "coverage_counts": gaps.get("coverage_counts", {}),
        "semantic_hash": manifest.get("semantic_hash"),
        "bootstrap": summary.get("bootstrap", {}).get("primary_net_mean", {}),
        "overlays": summary.get("risk_overlays", {}),
        "primary_gross": gross.get("primary_liquidity_1g", {}),
    }


def _perp_carry_v2b_availability_state(repository: Path) -> dict[str, Any] | None:
    """Read frozen collection progress without operating the collector."""
    config_path = repository / "config" / "perp-carry-v2b-availability.toml"
    root = repository / "data" / "v2b" / "prospective_funding_availability"
    try:
        with config_path.open("rb") as stream:
            config = tomllib.load(stream)["collection"]
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        return None
    start, end = str(config["start_utc"]), str(config["end_exclusive_utc"])
    canonical = 0
    for path in (root / "raw").rglob("*.json"):
        row = _load_json(path)
        if row is not None and start <= str(row.get("scheduled_minute_utc", "")) < end:
            canonical += 1
    state = _load_json(root / "state.json") or {}
    return {
        "start_utc": start,
        "day7_utc": config.get("day7_utc"),
        "end_exclusive_utc": end,
        "canonical_records": canonical,
        "expected_records": 432_000,
        "health": state.get("health", "not_started"),
        "timer_state": _unit_state("perp-carry-v2b-funding-availability.timer"),
        "profitability": "NOT_TESTED",
        "capital_permitted": 0,
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
    completed_audit = (
        _perp_carry_completed_audit_state() if config.strategy_id == "perp-carry-v1" else None
    )
    if audit is not None and completed_audit is not None:
        active_recorded_at = str(audit["record"].get("recorded_at_utc", ""))
        completed_at = str(completed_audit["record"].get("generated_at_utc", ""))
        if active_recorded_at <= completed_at:
            audit = None
    ctrend = _ctrend_executable_state(repo) if config.strategy_id == "ctrend-executable" else None
    liquidity = (
        _ctrend_liquidity_state(repo)
        if config.strategy_id == "ctrend-binance-usdm-liquidity-v1"
        else None
    )
    availability = (
        _perp_carry_v2b_availability_state(repo)
        if config.strategy_id == "perp-carry-v2b-funding-availability"
        else None
    )
    if config.strategy_id == "perp-carry-v1" and audit is None and completed_audit is None:
        warnings.append("clean bounded post-lifecycle-repair 24-hour audit has not been evidenced")
    if audit is not None:
        warnings.append(
            "clean bounded 24-hour audit is active; all reliability results are provisional"
        )
    if completed_audit is not None:
        warnings.append(
            "completed audit is an infrastructure integrity failure: clock timing gate failed; "
            "profitability remains untested and capital permission is zero"
        )
    if config.strategy_id.startswith("ctrend-"):
        warnings.append("capital is zero; this strategy is research-only")
    if config.strategy_id == "ctrend-executable" and ctrend is None:
        warnings.append("CTREND executable evidence ledger is absent or unreadable")
    if ctrend is not None:
        warnings.append(
            "CTREND executable universe is not reconstructed: official catalog retrieval stopped "
            "on HTTP 429 and point-in-time market-cap automation is unauthorized"
        )
    if config.strategy_id == "ctrend-binance-usdm-liquidity-v1" and liquidity is None:
        warnings.append("CTREND liquidity net-evaluation evidence is absent or unreadable")
    if liquidity is not None:
        warnings.append(
            "funding-inclusive all-history result is incomplete; 22 weekly records have partial "
            "funding coverage"
        )
    return {
        "strategy_id": config.strategy_id,
        "strategy_class": config.strategy_class,
        "repository": str(repo),
        "branch": branch,
        "current_commit": commit,
        "dataset_id": (
            str(audit["audit_root"])
            if audit is not None
            else str(completed_audit["audit_root"])
            if completed_audit is not None
            else config.dataset_id
        ),
        "experiment_or_run_id": audit["audit_id"]
        if audit is not None
        else completed_audit["audit_id"]
        if completed_audit is not None
        else config.experiment_or_run_id,
        "artifact_hashes": hashes,
        "stage": config.stage,
        "latest_verdict": config.latest_verdict,
        "historical_start": (
            audit["record"].get("audit_start_utc")
            if audit is not None
            else completed_audit["record"].get("exact_window_metrics", {}).get("window_start_utc")
            if completed_audit is not None
            else config.historical_start
        ),
        "historical_end": (
            audit["record"].get("earliest_valid_completion_utc")
            if audit is not None
            else completed_audit["record"].get("exact_window_metrics", {}).get("window_end_utc")
            if completed_audit is not None
            else config.historical_end
        ),
        "shadow_start": config.shadow_start,
        "shadow_end": config.shadow_end,
        "service_state": audit["service_state"]
        if audit is not None
        else completed_audit["service_state"]
        if completed_audit is not None
        else _unit_state(config.service_unit),
        "timer_state": audit["timer_state"]
        if audit is not None
        else completed_audit["timer_state"]
        if completed_audit is not None
        else _unit_state(config.timer_unit),
        "last_successful_update": (
            audit["health"].get("last_success_at")
            if audit is not None
            else completed_audit["record"]
            .get("finalization_state", {})
            .get("health", {})
            .get("last_success_at")
            if completed_audit is not None
            else None
        ),
        "last_error": None,
        "observation_count": (
            audit["health"].get("successful_collections")
            if audit is not None
            else completed_audit["quality"].get("observed_collection_events")
            if completed_audit is not None
            else None
        ),
        "instrument_master_status": ctrend["retrieval_status"] if ctrend is not None else None,
        "instrument_master_catalog_pages_cached": (
            ctrend["catalog_pages_cached"] if ctrend is not None else None
        ),
        "weekly_coverage_status": "NOT_COMPUTED_NO_VERIFIED_EPISODES"
        if ctrend is not None
        else None,
        "market_cap_access_status": "BLOCKED_NO_AUTHORIZED_AUTOMATION"
        if ctrend is not None
        else None,
        "instrument_master_evidence_sha256": (
            ctrend["instrument_master_sha256"] if ctrend is not None else None
        ),
        "prediction_or_trade_count": None,
        "gross_return": (
            liquidity["primary_gross"].get("cumulative_compounded_return") if liquidity else None
        ),
        "net_return": liquidity["primary"].get("cumulative_return") if liquidity else None,
        "fees": liquidity["primary"].get("total_fees") if liquidity else None,
        "funding": liquidity["primary"].get("total_funding") if liquidity else None,
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
        "ctrend_liquidity": liquidity,
        "availability_collector": availability,
        "perp_carry_audit": (
            {
                "state": "ACTIVE",
                "audit_id": audit["audit_id"],
            }
            if audit is not None
            else {
                "state": "COMPLETED",
                "audit_id": completed_audit["audit_id"],
                "audit_status": completed_audit["record"].get("audit_status"),
                "collection_counts": completed_audit["quality"],
                "timing_verdict": "PASS" if completed_audit["clock"].get("passed") else "FAIL",
                "lifecycle_verdict": (
                    "FAIL"
                    if completed_audit["record"]
                    .get("finalization_state", {})
                    .get("includes_post_window_collection")
                    else "PASS"
                ),
                "final_artifact": str(completed_audit["final_artifact"]),
                "capital_permission": 0,
                "profitability_status": "NOT_TESTED",
            }
            if completed_audit is not None
            else None
        ),
    }
