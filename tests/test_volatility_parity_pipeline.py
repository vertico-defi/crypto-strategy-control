from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from strategy_control.volatility_parity import (
    SYMBOLS,
    TRIAL_ORDER,
    MinuteBar,
    VolatilityParityError,
)
from strategy_control.volatility_parity_pipeline import (
    InMemoryMarket,
    common_panel,
    daily_returns_from_wealth,
    development_partitions,
    exact_daily_endpoint,
    fair_benchmark_entry,
    guarded_open,
    holdout_gate_map,
    prepare_market,
    prospective_warmup_complete,
    reject_holdout_path,
    scheduled_sunday,
    terminal_timestamp,
    verify_frozen_contract,
)

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT = ROOT / "experiments" / "btc-eth-causal-volatility-parity-rebalancing-v1"


def _bars(start: datetime, days: int = 1) -> list[MinuteBar]:
    return [
        MinuteBar(
            start + timedelta(minutes=index),
            start + timedelta(minutes=index),
            1,
            1,
            1,
            1,
            f"row-{index}",
        )
        for index in range(1, 1440 * days + 1)
    ]


def _contracts() -> tuple[dict[str, object], dict[str, object], bytes]:
    wrapper_path = EXPERIMENT / "PREREGISTRATION.json"
    effective_path = EXPERIMENT / "PREREGISTRATION_REVISED_DRAFT.json"
    return (
        json.loads(wrapper_path.read_text()),
        json.loads(effective_path.read_text()),
        effective_path.read_bytes(),
    )


def test_exact_2359_daily_endpoint_no_substitute() -> None:
    day = datetime(2025, 1, 1, tzinfo=UTC)
    endpoint = day.replace(hour=23, minute=59)
    bar = MinuteBar(
        endpoint + timedelta(minutes=1),
        endpoint + timedelta(minutes=1),
        2,
        2,
        2,
        2,
    )
    assert exact_daily_endpoint({endpoint: bar}, {endpoint: bar}, day)[SYMBOLS[0]] == 2
    with pytest.raises(VolatilityParityError, match="missing exact"):
        exact_daily_endpoint(
            {endpoint + timedelta(minutes=1): bar},
            {endpoint + timedelta(minutes=1): bar},
            day,
        )


def test_intraday_liquidation_not_extra_daily_observation() -> None:
    first = datetime(2025, 1, 1, 23, 59, tzinfo=UTC)
    second = first + timedelta(days=1)
    returns = daily_returns_from_wealth(
        {first: 100.0, second: 99.0}, exposed={first: True, second: False}
    )
    assert tuple(returns) == (second,)
    assert returns[second] == pytest.approx(-0.01)


def test_exposed_unpriceable_endpoint_fails() -> None:
    endpoint = datetime(2025, 1, 1, 23, 59, tzinfo=UTC)
    with pytest.raises(VolatilityParityError, match="exposed endpoint"):
        daily_returns_from_wealth({endpoint: None}, exposed={endpoint: True})


def test_cash_unavailable_span_absent_not_zero() -> None:
    first = datetime(2025, 1, 1, 23, 59, tzinfo=UTC)
    wealth = {first: 100.0, first + timedelta(days=1): None, first + timedelta(days=2): 100.0}
    assert daily_returns_from_wealth(wealth, exposed={}) == {}


def test_seven_trial_common_panel_minima() -> None:
    start = datetime(2025, 1, 1, 23, 59, tzinfo=UTC)
    panels = {
        name: {start + timedelta(days=index): 1.0 for index in range(320)} for name in TRIAL_ORDER
    }
    assert len(common_panel(panels, minimum_days=320)) == 320
    panels[TRIAL_ORDER[-1]].pop(start)
    with pytest.raises(VolatilityParityError, match="undersized"):
        common_panel(panels, minimum_days=320)


def test_ex_ante_terminal_T_E_exact_and_no_earlier_fallback() -> None:
    end = datetime(2025, 4, 1, tzinfo=UTC)
    assert terminal_timestamp(end) == datetime(2025, 3, 31, 23, 59, tzinfo=UTC)
    # The function accepts no observations from which an earlier timestamp could be selected.


def test_trial_inheritance_and_equal_weight_clock() -> None:
    wrapper, effective, effective_bytes = _contracts()
    verify_frozen_contract(wrapper, effective, effective_bytes)
    sunday = datetime(2025, 1, 5, tzinfo=UTC)
    assert scheduled_sunday(sunday)
    assert fair_benchmark_entry(
        primary_eligible=scheduled_sunday(sunday), actual_cash=True, quarantined=False
    )
    assert not scheduled_sunday(sunday + timedelta(days=1))


def test_buy_and_hold_entry_quarantine_reentry_and_terminal_fairness() -> None:
    assert fair_benchmark_entry(primary_eligible=True, actual_cash=True, quarantined=False)
    assert not fair_benchmark_entry(primary_eligible=True, actual_cash=True, quarantined=True)
    assert not fair_benchmark_entry(primary_eligible=False, actual_cash=True, quarantined=False)
    assert terminal_timestamp(datetime(2025, 4, 1, tzinfo=UTC)).minute == 59


def test_development_loader_rejects_2026_before_resolution() -> None:
    called = False

    def opener(_: object) -> str:
        nonlocal called
        called = True
        return "opened"

    with pytest.raises(VolatilityParityError, match="final-holdout"):
        guarded_open("canonical/year=2026/month=01/observations.parquet", opener)
    assert not called
    with pytest.raises(VolatilityParityError):
        reject_holdout_path("x/2026/y")
    data_contract = json.loads(
        (ROOT / "experiments" / "btc-eth-vol-targeted-trend-v1" / "DATA_CONTRACT.json").read_text()
    )
    assert len(development_partitions(data_contract)) == 36


def test_holdout_explicit_gate_map() -> None:
    _, effective, _ = _contracts()
    gates = holdout_gate_map(effective)
    assert len(gates) == 24
    assert gates["positive_folds_minimum"] == 2
    assert gates["capital_permitted"] == 0
    assert gates["probability_of_backtest_overfitting_lte"] == 0.2


def test_prospective_postfreeze_warmup_and_sunday_schedule() -> None:
    freeze = datetime(2026, 8, 3, tzinfo=UTC)
    starts = [freeze + timedelta(days=index + 1) for index in range(60)]
    assert prospective_warmup_complete(starts, freeze)
    assert not prospective_warmup_complete(starts[:59], freeze)
    starts[30] += timedelta(days=1)
    assert not prospective_warmup_complete(starts, freeze)


def test_in_memory_market_rejects_duplicates_and_holdout_flag() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    bars = _bars(start)
    prepared = prepare_market(InMemoryMarket({symbol: bars for symbol in SYMBOLS}))
    assert prepared.holdout_values_read is False
    with pytest.raises(VolatilityParityError, match="holdout values"):
        prepare_market(
            InMemoryMarket({symbol: bars for symbol in SYMBOLS}, holdout_values_read=True)
        )
    with pytest.raises(VolatilityParityError, match="duplicate"):
        prepare_market(InMemoryMarket({symbol: [*bars, bars[-1]] for symbol in SYMBOLS}))
