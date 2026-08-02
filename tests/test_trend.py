from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.trend import (
    DailyBar,
    Fill,
    IntervalResult,
    MinuteBar,
    TrendError,
    aggregate_daily_sessions,
    aggregate_return,
    buy_and_hold,
    cscv_pbo,
    deflated_sharpe_probability,
    delayed_fills,
    donchian_ensemble,
    evaluate_gates,
    exceptional_trade_concentration,
    first_strictly_causal_fill,
    fold_intervals,
    post_information_eligible,
    primary_exposure,
    realized_volatility,
    self_financing,
    stationary_bootstrap,
    time_series_momentum,
)


def _day(i: int, close: float, high: float | None = None, low: float | None = None) -> DailyBar:
    return DailyBar(
        datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=i),
        datetime(2025, 1, 2, tzinfo=UTC),
        close,
        high or close,
        low or close,
        close,
        True,
    )


def test_daily_aggregation_and_duplicate_rejected() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    bars = [
        MinuteBar(start + timedelta(minutes=i), start + timedelta(minutes=i), 1, 1, 1, 1)
        for i in range(1, 1441)
    ]
    assert aggregate_daily_sessions(bars)[0].complete
    with pytest.raises(TrendError, match="duplicate"):
        aggregate_daily_sessions([*bars, bars[-1]])


def test_fill_open_must_be_strictly_after_bar_end_information() -> None:
    session = datetime(2025, 1, 1, tzinfo=UTC)
    day = _day(0, 100)
    candidates = [
        MinuteBar(
            session + timedelta(days=1, minutes=1),
            session + timedelta(days=1, minutes=1),
            100,
            100,
            100,
            100,
        ),
        MinuteBar(
            session + timedelta(days=1, minutes=2),
            session + timedelta(days=1, minutes=2),
            101,
            101,
            101,
            101,
        ),
    ]
    assert first_strictly_causal_fill(day, candidates) == candidates[1]


def test_missing_session_resets_and_requires_recovery() -> None:
    days = [_day(i, 100 + i) for i in range(4)]
    days[1] = DailyBar(days[1].session, days[1].available_at, 1, 1, 1, 1, False)
    assert post_information_eligible(days, 2) == [False, False, False, True]


def test_signals_are_prior_only_and_volatility_is_lagged() -> None:
    days = [_day(i, float(i + 1)) for i in range(160)]
    assert donchian_ensemble(days, (2,))[2] == 1.0
    assert time_series_momentum(days, (2,))[2] == 1.0
    assert realized_volatility(days, 2)[1] is None
    # The 120-session signal warm-up cannot be bypassed by already-computed volatility.
    assert primary_exposure(days)[119] is None
    assert primary_exposure(days)[148] is None
    assert primary_exposure(days)[149] is not None


def test_incomplete_session_resets_signal_and_volatility_state() -> None:
    days = [_day(i, 100 + i) for i in range(310)]
    broken = days[155]
    days[155] = DailyBar(
        broken.session,
        broken.available_at,
        broken.open,
        broken.high,
        broken.low,
        broken.close,
        False,
    )
    exposure = primary_exposure(days)
    assert exposure[154] is not None
    assert all(value is None for value in exposure[155:305])
    assert exposure[305] is not None


def test_self_financing_costs_delay_and_terminal_liquidation() -> None:
    t = datetime(2025, 1, 1, tzinfo=UTC)
    fills = [
        Fill(t, {"BTC": 100.0}, {"BTC": 1.0}),
        Fill(t + timedelta(days=1), {"BTC": 110.0}, {"BTC": 0.0}),
    ]
    output = self_financing(fills, 100)
    assert len(output) == 1 and output[0].equity < 1.1
    assert delayed_fills(fills)[0].targets == {"BTC": 0.0}
    held = buy_and_hold(fills, {"BTC": 1.0}, 100)
    assert held[0].equity == pytest.approx(output[0].equity)


def test_half_open_fold_excludes_endpoint() -> None:
    t = datetime(2025, 1, 1, tzinfo=UTC)
    rows = self_financing(
        [
            Fill(t, {"A": 1.0}, {"A": 1.0}),
            Fill(t + timedelta(days=1), {"A": 1.0}, {"A": 1.0}),
            Fill(t + timedelta(days=2), {"A": 1.0}, {"A": 0.0}),
        ],
        0,
    )
    assert len(fold_intervals(rows, t, t + timedelta(days=1))) == 0


def test_bootstrap_reproducible_and_statistics_finite() -> None:
    class FakePCG:
        def __init__(self) -> None:
            self.state = 1

        def integers(self, high: int) -> int:
            self.state = (self.state * 6364136223846793005 + 1) % (2**64)
            return self.state % high

        def random(self) -> float:
            self.state = (self.state * 6364136223846793005 + 1) % (2**64)
            return self.state / 2**64

    values = [0.01, -0.005, 0.002, 0.003] * 4
    assert stationary_bootstrap(values, 20, rng=FakePCG()) == stationary_bootstrap(
        values, 20, rng=FakePCG()
    )
    alternatives = [values[i:] + values[:i] for i in range(7)]
    assert 0 <= deflated_sharpe_probability(values, alternatives) <= 1
    assert 0 <= cscv_pbo(alternatives) <= 1


def test_aggregate_return_and_exceptional_trade_gate() -> None:
    assert aggregate_return([0.1, -0.05]) == pytest.approx(0.045)
    t = datetime(2025, 1, 1, tzinfo=UTC)
    intervals = [
        IntervalResult(
            t + timedelta(days=index),
            t + timedelta(days=index + 1),
            0.03,
            equity,
            0.0,
            0.0,
        )
        for index, equity in enumerate((1.03, 1.06, 1.09, 1.12, 1.15, 1.18, 1.21, 1.24))
    ]
    result = exceptional_trade_concentration(intervals)
    assert result["pass"]


def test_gate_logic_fails_closed() -> None:
    gates = {
        "aggregate_net_return_gt": 0.0,
        "maximum_drawdown_lte": 0.2,
        "no_material_leakage": True,
    }
    assert evaluate_gates(
        {"aggregate_net_return_gt": 0.1, "maximum_drawdown_lte": 0.1, "no_material_leakage": True},
        gates,
    ) == {key: True for key in gates}
    assert not evaluate_gates({}, gates)["aggregate_net_return_gt"]
