from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.relative_value_v2 import DecisionTrace, RelativeValueV2Error
from strategy_control.relative_value_v2_pipeline import (
    GATE_NAMES,
    gate_map,
    reconcile_traces,
    reject_preapproval_path,
    strict_prefix,
)


def test_future_or_holdout_path_rejected_before_resolution_stat_footer_schema_or_value_access() -> (
    None
):
    with pytest.raises(RelativeValueV2Error, match="before resolution"):
        reject_preapproval_path("spot/year=2026/month=01/file")
    reject_preapproval_path("spot/year=2025/month=12/file")


def test_strict_prefix_and_trace_schema_reconciliation_are_fail_closed() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    assert strict_prefix(
        (1, 2, 3), tuple(start + timedelta(days=i) for i in range(3)), start + timedelta(days=2)
    ) == (1, 2)
    trace = DecisionTrace("s", start, start, "CASH", "CASH", "CASH", None, None, "terminal_cash")
    reconcile_traces((trace,), (trace,))
    with pytest.raises(RelativeValueV2Error):
        reconcile_traces((trace,), ())


def test_nineteen_gate_map_is_exact_and_unknown_or_missing_gate_fails() -> None:
    requirements = {
        name: (True if name == "no_material_leakage" else "pass" if name.endswith("gate") else 0)
        for name in GATE_NAMES
    }
    metrics = {
        name: (
            True if name in {"no_material_leakage", "exceptional_profit_gate", "regime_gate"} else 1
        )
        for name in GATE_NAMES
    }
    assert len(gate_map(metrics, requirements)) == 19
    with pytest.raises(RelativeValueV2Error):
        gate_map(metrics, {"unknown": True})
