from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.relative_value_v2 import RelativeValueV2Error
from strategy_control.relative_value_v2_pipeline import GATE_NAMES, gate_map, strict_prefix


def test_strict_prefix_rejects_nonmonotonic_before_filtering():
    end = datetime(2025, 1, 2, tzinfo=UTC)
    with pytest.raises(RelativeValueV2Error):
        strict_prefix((1, 2), (end + timedelta(days=1), end - timedelta(days=1)), end)


def test_nonfinite_numerical_gates_fail_closed():
    req = {
        name: (
            0.0
            if name.endswith(("_gt", "_gte", "_lte"))
            else 0
            if name
            not in {
                "baseline_superiority",
                "exceptional_profit_gate",
                "regime_gate",
                "no_material_leakage",
            }
            else True
        )
        for name in GATE_NAMES
    }
    metrics = {
        name: (
            1.0
            if name.endswith(("_gt", "_gte", "_lte"))
            else 1
            if isinstance(req[name], int)
            else True
        )
        for name in GATE_NAMES
    }
    metrics["aggregate_net_return_gt"] = float("inf")
    assert gate_map(metrics, req)["aggregate_net_return_gt"] is False
