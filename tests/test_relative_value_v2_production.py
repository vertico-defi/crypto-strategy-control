from datetime import UTC, datetime, timedelta

from strategy_control.relative_value_v2_pipeline import GATE_NAMES, gate_map, strict_prefix


def test_strict_prefix_isolates_nonmonotonic_suffix_before_validation():
    end = datetime(2025, 1, 2, tzinfo=UTC)
    clean = strict_prefix((1,), (end - timedelta(days=1),), end)
    corrupt_suffix = strict_prefix(
        (1, 2, 3), (end - timedelta(days=1), end + timedelta(days=2), end + timedelta(days=1)), end
    )
    assert clean == corrupt_suffix == (1,)


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
