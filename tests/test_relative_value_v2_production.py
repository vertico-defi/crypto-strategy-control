from datetime import UTC, datetime, timedelta
from types import MappingProxyType

import pytest

from strategy_control.mean_reversion_v2_pipeline import (
    FillIdentity,
    JointSession,
    ProductionRowIndex,
)
from strategy_control.mean_reversion_v2_pipeline import MinuteRow as ProductionMinuteRow
from strategy_control.relative_value_v2 import SYMBOLS, RelativeValueV2Error
from strategy_control.relative_value_v2_pipeline import (
    GATE_NAMES,
    build_production_bindings,
    gate_map,
    strict_prefix,
)


def _adapter_fixture():
    start = datetime(2025, 1, 1, tzinfo=UTC)
    boundary = start + timedelta(days=153)
    rows: dict[str, dict[datetime, ProductionMinuteRow]] = {asset: {} for asset in SYMBOLS}
    sessions = []
    for day in range(150):
        session = start + timedelta(days=day)
        close_at = session + timedelta(days=1)
        closes = {}
        for number, asset in enumerate(SYMBOLS):
            row = ProductionMinuteRow(
                "fixture", "a" * 64, day * 4 + number, "fixture", close_at, close_at,
                100.0 + day + number, 100.0 + day + number, 100.0 + day + number,
                100.0 + day + number, 1.0,
            )
            rows[asset][close_at] = row
            closes[asset] = row.close
        sessions.append(JointSession(session, True, close_at, closes, 0))
    final_session = sessions[-1].session
    base_at = sessions[-1].information_cutoff + timedelta(minutes=1)  # type: ignore[operator]
    base_prices, base_ids = {}, {}
    for number, asset in enumerate(SYMBOLS):
        row = ProductionMinuteRow(
            "fixture", "b" * 64, 1000 + number, "fixture", base_at + timedelta(minutes=1),
            base_at + timedelta(minutes=1), 250.0 + number, 250.0 + number, 250.0 + number,
            250.0 + number, 1.0,
        )
        rows[asset][row.event_timestamp] = row
        base_prices[asset], base_ids[asset] = row.open, row.identity
    index = ProductionRowIndex(
        boundary,
        MappingProxyType({asset: MappingProxyType(value) for asset, value in rows.items()}),
        MappingProxyType({asset: MappingProxyType({}) for asset in SYMBOLS}),
        sum(len(value) for value in rows.values()),
    )
    fills = (
        FillIdentity(final_session, 0, base_at, None, base_prices, base_ids, {}, {}),
    )
    return tuple(sessions), index, fills, boundary


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


def test_production_adapter_uses_exact_retained_close_identities_and_one_terminal_fill():
    sessions, index, fills, boundary = _adapter_fixture()
    bindings = build_production_bindings(sessions, index, fills, end=boundary)
    final = bindings[-1]
    assert final.eligible and final.terminal_fill is not None
    assert final.terminal_fill.row_ids == tuple(
        fills[0].base_row_identities[asset] for asset in SYMBOLS
    )
    for binding, source in zip(bindings, sessions, strict=True):
        assert binding.observations is not None
        for observation in binding.observations:
            retained = index.rows_by_asset[observation.asset][source.session + timedelta(days=1)]
            assert observation.identity == retained.identity
            assert observation.event_at == retained.event_timestamp
            assert observation.available_at == source.information_cutoff


def test_production_adapter_rejects_boundary_mismatch_before_binding():
    sessions, index, fills, boundary = _adapter_fixture()
    with pytest.raises(RelativeValueV2Error, match="boundary"):
        build_production_bindings(sessions, index, fills, end=boundary + timedelta(days=1))
