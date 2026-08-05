from __future__ import annotations

from pathlib import Path

from strategy_control.route4_preflight import validate_route4_content


def test_route4_content_preflight_passes_without_network_or_data() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    report = validate_route4_content(repo_root)
    assert report["content_verdict"] == "PASS"
    assert report["prohibited_action_counters"] == {
        "network_acquisition_attempts": 0,
        "market_data_rows_accessed": 0,
        "model_training_runs": 0,
        "backtests": 0,
        "return_calculations": 0,
        "holdout_paths_resolved": 0,
        "holdout_accesses": 0,
    }
    traceability = report["review_check_traceability"]
    assert isinstance(traceability, list)
    assert [row["check"] for row in traceability] == list(range(19))
