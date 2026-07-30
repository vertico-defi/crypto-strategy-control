from __future__ import annotations

import json
from pathlib import Path

from strategy_control.adapters import (
    _ctrend_executable_state,
    _ctrend_liquidity_state,
    _perp_carry_audit_state,
    inspect,
)
from strategy_control.model import StrategyConfig
from strategy_control.render import write_artifacts


def test_adapter_is_read_only_and_hashes_known_artifact(tmp_path: Path) -> None:
    repo = tmp_path / "source"
    repo.mkdir()
    (repo / "artifact.md").write_text("evidence\n")
    config = StrategyConfig.from_mapping(
        {
            "strategy_id": "ctrend-academic",
            "strategy_class": "test",
            "repository": str(repo),
            "dataset_id": None,
            "experiment_or_run_id": None,
            "stage": "DESIGN",
            "latest_verdict": "test",
            "historical_start": None,
            "historical_end": None,
            "shadow_start": None,
            "shadow_end": None,
            "risk_budget": "0",
            "next_gate": "test",
            "latest_report": "artifact.md",
            "artifact_files": ["artifact.md"],
            "service_unit": None,
            "timer_unit": None,
        }
    )

    result = inspect(config)

    assert (repo / "artifact.md").read_text() == "evidence\n"
    assert result["artifact_hashes"]["artifact.md"]
    assert result["current_exposure"].startswith("0.0")


def test_artifact_rendering_writes_json_markdown_and_html(tmp_path: Path) -> None:
    source = {
        "generated_at": "2026-01-01T00:00:00+00:00",
        "strategies": [
            {
                "strategy_id": "x<y",
                "stage": "DESIGN",
                "latest_verdict": "research",
                "repository": "/tmp/x",
                "current_commit": None,
                "service_state": "inactive",
                "timer_state": "inactive",
                "current_exposure": "0.0",
                "risk_budget": "0",
                "next_gate": "next",
                "integrity_warnings": [],
            }
        ],
    }

    paths = write_artifacts(tmp_path / "reports", source)

    assert json.loads(paths["json"].read_text())["strategies"][0]["strategy_id"] == "x<y"
    assert "x&lt;y" in paths["html"].read_text()
    assert "# Crypto strategy control report" in paths["markdown"].read_text()


def test_perp_audit_adapter_reads_active_start_without_writing(tmp_path: Path) -> None:
    audit_root = tmp_path / "audit-live"
    start = audit_root / "operations" / "audits" / "audit-live" / "audit-start.json"
    start.parent.mkdir(parents=True)
    start.write_text(
        json.dumps(
            {
                "status": "active",
                "audit_id": "audit-live",
                "recorded_at_utc": "2026-07-30T00:00:00Z",
                "audit_start_utc": "2026-07-30T00:01:00Z",
                "earliest_valid_completion_utc": "2026-07-31T00:01:00Z",
            }
        )
    )

    result = _perp_carry_audit_state(tmp_path)

    assert result is not None
    assert result["audit_id"] == "audit-live"
    assert start.exists()


def test_ctrend_adapter_reads_evidence_without_writing(tmp_path: Path) -> None:
    evidence = tmp_path / "reports" / "binance_usdm_instrument_evidence.json"
    evidence.parent.mkdir()
    evidence.write_text(
        json.dumps(
            {
                "retrieval_status": "BLOCKED_RATE_LIMIT",
                "catalog_pages_cached": 118,
                "candidate_articles": [],
                "instrument_master_sha256": "abc",
                "generated_at_utc": "2026-07-30T00:00:00Z",
            }
        )
    )

    result = _ctrend_executable_state(tmp_path)

    assert result is not None
    assert result["retrieval_status"] == "BLOCKED_RATE_LIMIT"
    assert evidence.exists()


def test_ctrend_liquidity_adapter_reads_net_evidence_without_writing(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "ctrend_liquidity_net_evaluation_summary.json").write_text(
        json.dumps(
            {
                "cost_scenarios": {"execution_25bp": {"cumulative_return": -0.5}},
                "funding": {"net": -0.01},
                "bootstrap": {"primary_net_mean": {"excludes_zero": False}},
                "risk_overlays": {},
            }
        )
    )
    (reports / "ctrend_liquidity_net_evaluation_manifest.json").write_text(
        json.dumps({"semantic_hash": "semantic"})
    )
    (reports / "ctrend_liquidity_funding_gap_report.json").write_text(
        json.dumps({"coverage_counts": {"PARTIAL": 1}})
    )

    result = _ctrend_liquidity_state(tmp_path)

    assert result is not None
    assert result["primary"]["cumulative_return"] == -0.5
    assert result["coverage_counts"] == {"PARTIAL": 1}
