"""Deterministic primitives for the frozen BTC/ETH bounded inverse-volatility study.

The module has no data-discovery, exchange, network, credential, order, GPU, or
market-archive dependency.  Every function consumes already supplied values and
fails closed when a frozen timing, numerical, accounting, or one-shot invariant
cannot be proved.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import NormalDist, median
from typing import Any

SYMBOLS = ("BTCUSDT", "ETHUSDT")
CASH = "CASH"
EXPERIMENT_ID = "btc-eth-causal-volatility-parity-rebalancing-v1"
BASE_COST_RATE = 0.0014
DOUBLED_COST_RATE = 0.0028
RECOVERY_SESSIONS = 150


class VolatilityParityError(ValueError):
    """A frozen invariant cannot be satisfied and must not be repaired in place."""


@dataclass(frozen=True)
class Trial:
    name: str
    lookback: int
    lower: float
    upper: float
    biweekly: bool = False
    equal_weight: bool = False

    def __post_init__(self) -> None:
        if self.lookback not in (30, 60, 90):
            raise VolatilityParityError("invalid frozen lookback")
        if not 0 <= self.lower <= self.upper <= 1:
            raise VolatilityParityError("invalid frozen bounds")


TRIALS = (
    Trial("primary_60d_bounds_20_80_weekly", 60, 0.2, 0.8),
    Trial("short_30d_bounds_20_80_weekly", 30, 0.2, 0.8),
    Trial("long_90d_bounds_20_80_weekly", 90, 0.2, 0.8),
    Trial("tight_bounds_30_70_60d_weekly", 60, 0.3, 0.7),
    Trial("wide_bounds_10_90_60d_weekly", 60, 0.1, 0.9),
    Trial("biweekly_60d_bounds_20_80", 60, 0.2, 0.8, biweekly=True),
    Trial("equal_weight_weekly_rebalanced", 60, 0.5, 0.5, equal_weight=True),
)
TRIAL_ORDER = tuple(trial.name for trial in TRIALS)
PRIMARY = TRIALS[0]


@dataclass(frozen=True)
class MinuteBar:
    """One canonical minute bar whose timestamp denotes the minute end."""

    event_timestamp: datetime
    available_timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    source_record_id: str = ""


@dataclass(frozen=True)
class Session:
    """A UTC session from 00:01 minute-end through the following 00:00."""

    start: datetime
    available_timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    complete: bool
    rows: tuple[MinuteBar, ...] = ()


@dataclass(frozen=True)
class Estimate:
    raw: Mapping[str, float]
    bounded: Mapping[str, float]
    covariance: tuple[tuple[float, float], tuple[float, float]]
    raw_component_risks: tuple[float, float]
    bounded_component_risks: tuple[float, float]
    raw_portfolio_risk: float
    bounded_portfolio_risk: float


@dataclass(frozen=True)
class Target:
    trial: str
    signal_session_end: datetime
    information_time: datetime
    expected_open: datetime
    weights: Mapping[str, float]
    diagnostics: Estimate
    input_ids: tuple[str, ...]
    canonical_hash: str


@dataclass(frozen=True)
class Trade:
    timestamp: datetime
    target: Mapping[str, float]
    turnover: float
    gross_risky_fraction: float
    cost: float
    wealth_before: float
    wealth_after: float
    cost_by_asset: Mapping[str, float]


@dataclass(frozen=True)
class Account:
    """Self-financing BTC/ETH/cash state with quote-currency contributions."""

    units: Mapping[str, float]
    cash: float
    prices: Mapping[str, float]
    contributions: Mapping[str, float]


@dataclass(frozen=True)
class PendingOrder:
    target: Target
    execute_session_start: datetime


@dataclass(frozen=True)
class LifecycleEvent:
    quarantined: bool
    terminal: bool
    safety_liquidation: bool
    executed_pending: bool
    pending: PendingOrder | None
    event_order: tuple[str, ...]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise VolatilityParityError("timestamp must be explicitly UTC")
    return value.astimezone(UTC)


def _finite(value: float, name: str) -> float:
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        raise VolatilityParityError(f"{name} must be finite")
    return float(value)


def _positive(value: float, name: str) -> float:
    result = _finite(value, name)
    if result <= 0:
        raise VolatilityParityError(f"{name} must be positive")
    return result


def canonical_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def validate_bar(bar: MinuteBar) -> None:
    event = _utc(bar.event_timestamp)
    available = _utc(bar.available_timestamp)
    if event.second or event.microsecond:
        raise VolatilityParityError("minute end is off the whole-minute grid")
    if available < event:
        raise VolatilityParityError("availability precedes event timestamp")
    for value, name in (
        (bar.open, "open"),
        (bar.high, "high"),
        (bar.low, "low"),
        (bar.close, "close"),
    ):
        _positive(value, name)
    if bar.low > min(bar.open, bar.close) or bar.high < max(bar.open, bar.close):
        raise VolatilityParityError("invalid OHLC")


def aggregate_sessions(bars: Sequence[MinuteBar]) -> tuple[Session, ...]:
    """Build sessions without filling gaps or accepting duplicate/off-grid rows."""

    grouped: dict[datetime, list[MinuteBar]] = {}
    previous: datetime | None = None
    for bar in bars:
        validate_bar(bar)
        end = _utc(bar.event_timestamp)
        if previous is not None and end <= previous:
            raise VolatilityParityError("duplicate or nonmonotonic minute bar")
        previous = end
        session_start = (end - timedelta(microseconds=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        grouped.setdefault(session_start, []).append(bar)

    sessions: list[Session] = []
    for start, rows in sorted(grouped.items()):
        expected = tuple(start + timedelta(minutes=index) for index in range(1, 1441))
        observed = tuple(_utc(row.event_timestamp) for row in rows)
        complete = len(rows) == 1440 and observed == expected
        sessions.append(
            Session(
                start=start,
                available_timestamp=max(_utc(row.available_timestamp) for row in rows),
                open=rows[0].open,
                high=max(row.high for row in rows),
                low=min(row.low for row in rows),
                close=rows[-1].close,
                complete=complete,
                rows=tuple(rows),
            )
        )
    return tuple(sessions)


def paired_returns(
    btc: Sequence[Session | None], eth: Sequence[Session | None]
) -> tuple[tuple[float, float] | None, ...]:
    """Return only contiguous synchronized joint returns; a gap clears history."""

    if len(btc) != len(eth):
        raise VolatilityParityError("joint session count mismatch")
    output: list[tuple[float, float] | None] = []
    prior: tuple[Session, Session] | None = None
    for btc_session, eth_session in zip(btc, eth, strict=True):
        joint_complete = (
            btc_session is not None
            and eth_session is not None
            and btc_session.complete
            and eth_session.complete
            and _utc(btc_session.start) == _utc(eth_session.start)
        )
        if not joint_complete:
            output.append(None)
            prior = None
            continue
        assert btc_session is not None and eth_session is not None
        if prior is None or btc_session.start != prior[0].start + timedelta(days=1):
            output.append(None)
        else:
            output.append(
                (
                    btc_session.close / prior[0].close - 1,
                    eth_session.close / prior[1].close - 1,
                )
            )
        prior = (btc_session, eth_session)
    return tuple(output)


def contiguous_return_window(
    returns: Sequence[tuple[float, float] | None], end_index: int, lookback: int
) -> tuple[tuple[float, float], ...]:
    if lookback not in (30, 60, 90) or end_index < lookback - 1:
        raise VolatilityParityError("insufficient exact estimator window")
    selected = returns[end_index - lookback + 1 : end_index + 1]
    if len(selected) != lookback or any(row is None for row in selected):
        raise VolatilityParityError("estimator window is not contiguous")
    return tuple(row for row in selected if row is not None)


def _sample_covariance(
    matrix: Sequence[tuple[float, float]],
) -> tuple[tuple[float, float], tuple[float, float]]:
    if len(matrix) < 2:
        raise VolatilityParityError("covariance needs at least two observations")
    if any(len(row) != 2 or any(not math.isfinite(value) for value in row) for row in matrix):
        raise VolatilityParityError("nonfinite return matrix")
    count = len(matrix)
    means = tuple(sum(row[index] for row in matrix) / count for index in range(2))
    s00 = sum((row[0] - means[0]) ** 2 for row in matrix) / (count - 1)
    s11 = sum((row[1] - means[1]) ** 2 for row in matrix) / (count - 1)
    s01 = sum((row[0] - means[0]) * (row[1] - means[1]) for row in matrix) / (count - 1)
    return ((s00, s01), (s01, s11))


def validate_covariance(
    covariance: Sequence[Sequence[float]], sigmas: Sequence[float]
) -> tuple[tuple[float, float], tuple[float, float]]:
    """Apply every frozen scale-relative covariance tolerance."""

    if len(covariance) != 2 or any(len(row) != 2 for row in covariance) or len(sigmas) != 2:
        raise VolatilityParityError("covariance shape mismatch")
    s00, s01 = map(float, covariance[0])
    s10, s11 = map(float, covariance[1])
    sigma0, sigma1 = map(float, sigmas)
    values = (s00, s01, s10, s11, sigma0, sigma1)
    if any(not math.isfinite(value) for value in values):
        raise VolatilityParityError("nonfinite covariance quantity")
    scale = max(s00, s11)
    if scale <= 0 or sigma0 <= 0 or sigma1 <= 0:
        raise VolatilityParityError("covariance scale or marginal volatility degeneracy")
    if abs(s00 - sigma0**2) > 1e-12 * scale:
        raise VolatilityParityError("BTC variance and sigma disagree")
    if abs(s11 - sigma1**2) > 1e-12 * scale:
        raise VolatilityParityError("ETH variance and sigma disagree")
    if abs(s01 - s10) > 1e-12 * scale:
        raise VolatilityParityError("covariance is asymmetric")
    off_diagonal = (s01 + s10) / 2
    lambda_min = (s00 + s11 - math.sqrt((s00 - s11) ** 2 + 4 * off_diagonal**2)) / 2
    if lambda_min < -1e-12 * scale:
        raise VolatilityParityError("covariance is not positive semidefinite")
    correlation = off_diagonal / (sigma0 * sigma1)
    if not math.isfinite(correlation) or not -1 - 1e-12 <= correlation <= 1 + 1e-12:
        raise VolatilityParityError("correlation is outside tolerance")
    return ((s00, off_diagonal), (off_diagonal, s11))


def bounded_inverse_volatility(
    matrix: Sequence[tuple[float, float]], trial: Trial = PRIMARY
) -> Estimate:
    required = 60 if trial.equal_weight else trial.lookback
    if len(matrix) != required:
        raise VolatilityParityError("exact trial lookback required")
    covariance = _sample_covariance(matrix)
    sigmas = (math.sqrt(covariance[0][0]), math.sqrt(covariance[1][1]))
    covariance = validate_covariance(covariance, sigmas)
    scale = max(covariance[0][0], covariance[1][1])
    raw_btc = sigmas[1] / (sigmas[0] + sigmas[1])
    raw = (raw_btc, 1 - raw_btc)

    def risks(weights: tuple[float, float]) -> tuple[float, float, float]:
        sigma_weights = (
            covariance[0][0] * weights[0] + covariance[0][1] * weights[1],
            covariance[1][0] * weights[0] + covariance[1][1] * weights[1],
        )
        variance = sum(
            weight * covariance_weight
            for weight, covariance_weight in zip(weights, sigma_weights, strict=True)
        )
        if not math.isfinite(variance) or variance <= 1e-12 * scale:
            raise VolatilityParityError("portfolio variance degeneracy")
        portfolio_risk = math.sqrt(variance)
        component_risks = tuple(
            weight * covariance_weight / portfolio_risk
            for weight, covariance_weight in zip(weights, sigma_weights, strict=True)
        )
        if any(not math.isfinite(value) or value <= 0 for value in component_risks):
            raise VolatilityParityError("component risk degeneracy")
        return component_risks[0], component_risks[1], portfolio_risk

    raw_btc_risk, raw_eth_risk, raw_risk = risks(raw)
    if abs(raw_btc_risk - raw_eth_risk) > 1e-10 * raw_risk:
        raise VolatilityParityError("raw equal-risk-contribution check failed")
    if abs(raw_btc_risk + raw_eth_risk - raw_risk) > 1e-10 * raw_risk:
        raise VolatilityParityError("raw component-risk reconciliation failed")

    bounded_btc = 0.5 if trial.equal_weight else min(trial.upper, max(trial.lower, raw_btc))
    bounded = (bounded_btc, 1 - bounded_btc)
    if any(not math.isfinite(weight) or weight < 0 for weight in bounded):
        raise VolatilityParityError("bounded target is invalid")
    if abs(sum(bounded) - 1) > 1e-12:
        raise VolatilityParityError("bounded target does not sum to one")
    bounded_btc_risk, bounded_eth_risk, bounded_risk = risks(bounded)
    return Estimate(
        raw=dict(zip(SYMBOLS, raw, strict=True)),
        bounded=dict(zip(SYMBOLS, bounded, strict=True)),
        covariance=covariance,
        raw_component_risks=(raw_btc_risk, raw_eth_risk),
        bounded_component_risks=(bounded_btc_risk, bounded_eth_risk),
        raw_portfolio_risk=raw_risk,
        bounded_portfolio_risk=bounded_risk,
    )


def expected_whole_minute_open(information_time: datetime) -> datetime:
    information_time = _utc(information_time)
    result = information_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
    if result <= information_time:
        raise VolatilityParityError("expected open is not strictly after information time")
    return result


def materialize_target(
    trial: Trial,
    signal_session: Session,
    matrix: Sequence[tuple[float, float]],
    *,
    estimator_sessions: Sequence[Session] = (),
    input_ids: Sequence[str] = (),
) -> Target:
    estimate = bounded_inverse_volatility(matrix, trial)
    sessions = (*estimator_sessions, signal_session)
    timestamps: list[datetime] = []
    for session in sessions:
        timestamps.append(_utc(session.start) + timedelta(days=1))
        timestamps.append(_utc(session.available_timestamp))
        for row in session.rows:
            timestamps.extend((_utc(row.event_timestamp), _utc(row.available_timestamp)))
    information_time = max(timestamps)
    expected_open = expected_whole_minute_open(information_time)
    canonical_input_ids = tuple(str(value) for value in input_ids)
    payload: dict[str, Any] = {
        "experiment_id": EXPERIMENT_ID,
        "trial": trial.name,
        "signal_session_end": (_utc(signal_session.start) + timedelta(days=1)).isoformat(),
        "information_time": information_time.isoformat(),
        "expected_open": expected_open.isoformat(),
        "input_ids": canonical_input_ids,
        "weights": {symbol: estimate.bounded[symbol] for symbol in SYMBOLS},
        "covariance": estimate.covariance,
        "raw_component_risks": estimate.raw_component_risks,
        "bounded_component_risks": estimate.bounded_component_risks,
    }
    return Target(
        trial=trial.name,
        signal_session_end=_utc(signal_session.start) + timedelta(days=1),
        information_time=information_time,
        expected_open=expected_open,
        weights=estimate.bounded,
        diagnostics=estimate,
        input_ids=canonical_input_ids,
        canonical_hash=canonical_hash(payload),
    )


def index_open_vectors(bars: Sequence[MinuteBar]) -> Mapping[datetime, MinuteBar]:
    """Index exact minute opens and reject duplicates before vector selection."""

    result: dict[datetime, MinuteBar] = {}
    for bar in bars:
        validate_bar(bar)
        open_timestamp = _utc(bar.event_timestamp) - timedelta(minutes=1)
        if open_timestamp in result:
            raise VolatilityParityError("duplicate minute-open identity")
        result[open_timestamp] = bar
    return result


def canonical_vector(
    btc: Mapping[datetime, MinuteBar],
    eth: Mapping[datetime, MinuteBar],
    expected: datetime,
) -> Mapping[str, float]:
    """Select only the predeclared atomic BTC/ETH open vector."""

    expected = _utc(expected)
    rows = (btc.get(expected), eth.get(expected))
    if any(row is None for row in rows):
        raise VolatilityParityError("missing exact atomic execution vector")
    result: dict[str, float] = {}
    for symbol, row in zip(SYMBOLS, rows, strict=True):
        assert row is not None
        validate_bar(row)
        if _utc(row.event_timestamp) - timedelta(minutes=1) != expected:
            raise VolatilityParityError("off-grid execution vector")
        result[symbol] = row.open
    return result


def execute_trade(
    wealth: float,
    prior: Mapping[str, float],
    target: Mapping[str, float],
    cost_rate: float,
    timestamp: datetime,
) -> Trade:
    """Apply the distinct reporting-turnover and gross-risky cost formulas."""

    wealth = _positive(wealth, "wealth")
    keys = (*SYMBOLS, CASH)
    if set(prior) != set(keys) or set(target) != set(keys):
        raise VolatilityParityError("three-weight state required")
    if any(not math.isfinite(prior[key]) for key in keys):
        raise VolatilityParityError("nonfinite drifted weights")
    if any(not math.isfinite(target[key]) or target[key] < 0 for key in keys):
        raise VolatilityParityError("invalid target weights")
    if abs(sum(prior.values()) - 1) > 1e-12 or abs(sum(target.values()) - 1) > 1e-12:
        raise VolatilityParityError("weights do not sum to one")
    gross_risky = sum(abs(target[key] - prior[key]) for key in SYMBOLS)
    turnover = 0.5 * sum(abs(target[key] - prior[key]) for key in keys)
    if not math.isfinite(cost_rate) or cost_rate < 0 or cost_rate * gross_risky >= 1:
        raise VolatilityParityError("invalid cost rate or costed fraction")
    cost = wealth * cost_rate * gross_risky
    wealth_after = wealth - cost
    changes = {key: abs(target[key] - prior[key]) for key in SYMBOLS}
    total_change = sum(changes.values())
    cost_by_asset = {
        key: cost * changes[key] / total_change if total_change else 0.0 for key in SYMBOLS
    }
    return Trade(
        timestamp=_utc(timestamp),
        target=dict(target),
        turnover=turnover,
        gross_risky_fraction=gross_risky,
        cost=cost,
        wealth_before=wealth,
        wealth_after=wealth_after,
        cost_by_asset=cost_by_asset,
    )


def initial_account(initial_wealth: float = 1.0) -> Account:
    return Account(
        units={symbol: 0.0 for symbol in SYMBOLS},
        cash=_positive(initial_wealth, "initial wealth"),
        prices={},
        contributions={symbol: 0.0 for symbol in SYMBOLS},
    )


def mark_account(account: Account, prices: Mapping[str, float]) -> tuple[Account, float]:
    """Mark every held unit and retain currency PnL by asset."""

    if set(prices) != set(SYMBOLS):
        raise VolatilityParityError("exact BTC/ETH mark vector required")
    checked = {symbol: _positive(prices[symbol], f"{symbol} price") for symbol in SYMBOLS}
    contributions = dict(account.contributions)
    for symbol in SYMBOLS:
        if symbol in account.prices:
            contributions[symbol] += account.units[symbol] * (
                checked[symbol] - account.prices[symbol]
            )
    wealth = account.cash + sum(account.units[symbol] * checked[symbol] for symbol in SYMBOLS)
    _positive(wealth, "marked wealth")
    return (
        Account(
            units=dict(account.units),
            cash=account.cash,
            prices=checked,
            contributions=contributions,
        ),
        wealth,
    )


def trade_account(
    account: Account,
    prices: Mapping[str, float],
    target: Mapping[str, float],
    cost_rate: float,
    timestamp: datetime,
) -> tuple[Account, Trade]:
    marked, wealth = mark_account(account, prices)
    prior = {
        SYMBOLS[0]: marked.units[SYMBOLS[0]] * prices[SYMBOLS[0]] / wealth,
        SYMBOLS[1]: marked.units[SYMBOLS[1]] * prices[SYMBOLS[1]] / wealth,
        CASH: marked.cash / wealth,
    }
    trade = execute_trade(wealth, prior, target, cost_rate, timestamp)
    contributions = {
        symbol: marked.contributions[symbol] - trade.cost_by_asset[symbol] for symbol in SYMBOLS
    }
    units = {symbol: trade.wealth_after * target[symbol] / prices[symbol] for symbol in SYMBOLS}
    cash = trade.wealth_after * target[CASH]
    return Account(units, cash, dict(prices), contributions), trade


def reconcile_contributions(initial: float, terminal: float, btc: float, eth: float) -> None:
    difference = _finite(terminal, "terminal wealth") - _finite(initial, "initial wealth")
    tolerance = 1e-10 * max(1.0, abs(difference))
    if any(not math.isfinite(value) for value in (btc, eth)):
        raise VolatilityParityError("nonfinite asset contribution")
    if abs(btc + eth - difference) > tolerance:
        raise VolatilityParityError("currency contribution reconciliation failure")


def delayed_pending(target: Target, next_completed_session: datetime) -> PendingOrder:
    return PendingOrder(target, _utc(next_completed_session))


def order_lifecycle(
    pending: PendingOrder | None,
    *,
    quarantined: bool,
    terminal: bool,
    exposed: bool,
    exact_vector_available: bool,
    session: datetime,
) -> LifecycleEvent:
    """Enforce integrity, mark, safety, pending execution, then record ordering."""

    ordered = ["integrity_detection", "mark_existing_units"]
    if terminal:
        ordered.extend(("terminal_override_and_cancel", "record_state"))
        return LifecycleEvent(quarantined, True, False, False, None, tuple(ordered))
    if quarantined:
        if exposed and not exact_vector_available:
            raise VolatilityParityError("unpriceable exposed quarantine")
        ordered.extend(("safety_liquidation" if exposed else "cash_quarantine", "record_state"))
        return LifecycleEvent(True, False, exposed, False, None, tuple(ordered))
    execute = pending is not None and _utc(session) == pending.execute_session_start
    if execute and not exact_vector_available:
        raise VolatilityParityError("missing exact delayed vector")
    ordered.extend(("execute_pending" if execute else "no_execution", "record_state"))
    return LifecycleEvent(
        False,
        False,
        False,
        execute,
        None if execute else pending,
        tuple(ordered),
    )


def recovery_eligible(
    sessions: Sequence[Session | None], recovery: int = RECOVERY_SESSIONS
) -> tuple[bool, ...]:
    if recovery != RECOVERY_SESSIONS:
        raise VolatilityParityError("frozen recovery is 150 sessions")
    run = 0
    previous: datetime | None = None
    result: list[bool] = []
    for session in sessions:
        complete = session is not None and session.complete
        contiguous = False
        if session is not None and complete:
            contiguous = previous is None or _utc(session.start) == previous + timedelta(days=1)
        run = run + 1 if contiguous else 0
        result.append(run >= recovery)
        previous = _utc(session.start) if session is not None and complete else None
    return tuple(result)


def regime_labels(sessions: Sequence[Session | None]) -> tuple[str | None, ...]:
    """Label the next interval using only the preceding completed BTC session."""

    closes: list[float] = []
    returns: list[float] = []
    prior_volatilities: list[float] = []
    result: list[str | None] = []
    previous: datetime | None = None
    for session in sessions:
        complete = session is not None and session.complete
        contiguous = False
        if session is not None and complete:
            contiguous = previous is None or _utc(session.start) == previous + timedelta(days=1)
        if not contiguous:
            closes = []
            returns = []
            prior_volatilities = []
            result.append(None)
            previous = None
            continue
        assert session is not None
        closes.append(session.close)
        if len(closes) > 1:
            returns.append(closes[-1] / closes[-2] - 1)
        current_volatility: float | None = None
        if len(returns) >= 60:
            sample = returns[-60:]
            sample_mean = sum(sample) / len(sample)
            current_volatility = math.sqrt(sum((value - sample_mean) ** 2 for value in sample) / 59)
        if len(closes) >= 121 and current_volatility is not None and len(prior_volatilities) >= 120:
            trend = "up" if closes[-1] / closes[-121] - 1 > 0 else "down"
            volatility = "high" if current_volatility > median(prior_volatilities) else "low"
            result.append(f"{trend}_{volatility}")
        else:
            result.append(None)
        if current_volatility is not None:
            prior_volatilities.append(current_volatility)
        previous = _utc(session.start)
    return tuple(result)


def stationary_bootstrap(
    values: Sequence[float], block_length: int, *, resamples: int = 2000
) -> Mapping[str, float | int]:
    if (
        not values
        or block_length not in (10, 20, 40)
        or resamples < 1
        or any(not math.isfinite(value) for value in values)
    ):
        raise VolatilityParityError("invalid stationary bootstrap input")
    seed = int.from_bytes(
        hashlib.sha256(f"{EXPERIMENT_ID}|stationary-bootstrap|{block_length}".encode()).digest()[
            :8
        ],
        "big",
        signed=False,
    )
    generator = random.Random(seed)
    count = len(values)
    means: list[float] = []
    for _ in range(resamples):
        index = generator.randrange(count)
        sample: list[float] = []
        for _ in range(count):
            sample.append(values[index])
            if generator.random() < 1 / block_length:
                index = generator.randrange(count)
            else:
                index = (index + 1) % count
        means.append(sum(sample) / count)
    means.sort()

    def linear_percentile(probability: float) -> float:
        location = (len(means) - 1) * probability
        lower = math.floor(location)
        upper = math.ceil(location)
        return means[lower] + (means[upper] - means[lower]) * (location - lower)

    return {
        "seed": seed,
        "lower_95": linear_percentile(0.025),
        "upper_95": linear_percentile(0.975),
        "mean": sum(values) / count,
        "resamples": resamples,
    }


def daily_sharpe(values: Sequence[float], *, annualized: bool = False) -> float:
    if len(values) < 2 or any(not math.isfinite(value) for value in values):
        return math.nan
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    if variance == 0:
        if mean > 0:
            return math.inf
        return -math.inf
    result = mean / math.sqrt(variance)
    return result * math.sqrt(365) if annualized else result


def deflated_sharpe(
    primary: Sequence[float],
    prior_sharpes: Sequence[float],
    current_sharpes: Sequence[float],
) -> Mapping[str, float]:
    """Frozen 35-attempt DSR with 28 observed Sharpes and no calendar imputation."""

    observed = [*prior_sharpes, *current_sharpes]
    failure = {"probability": 0.0, "N": 35.0, "T_eff": 0.0, "VIF": math.inf}
    if (
        len(prior_sharpes) != 21
        or len(current_sharpes) != 7
        or len(observed) != 28
        or len(primary) < 29
        or any(not math.isfinite(value) for value in observed)
        or any(not math.isfinite(value) for value in primary)
    ):
        return failure
    observed_mean = sum(observed) / 28
    sigma_sr = math.sqrt(sum((value - observed_mean) ** 2 for value in observed) / 27)
    if not math.isfinite(sigma_sr) or sigma_sr <= 0:
        return failure
    primary_mean = sum(primary) / len(primary)
    autocorrelation_denominator = sum((value - primary_mean) ** 2 for value in primary)
    if autocorrelation_denominator <= 0 or not math.isfinite(autocorrelation_denominator):
        return failure
    autocorrelations = [
        sum(
            (primary[index] - primary_mean) * (primary[index - lag] - primary_mean)
            for index in range(lag, len(primary))
        )
        / autocorrelation_denominator
        for lag in range(1, 29)
    ]
    vif = max(
        1.0,
        1 + 2 * sum((1 - lag / 29) * autocorrelations[lag - 1] for lag in range(1, 29)),
    )
    effective_count = len(primary) / vif
    if not math.isfinite(effective_count) or effective_count < 30:
        return {**failure, "T_eff": effective_count, "VIF": vif}
    observed_sharpe = daily_sharpe(primary)
    if not math.isfinite(observed_sharpe):
        return {**failure, "T_eff": effective_count, "VIF": vif}
    gamma = 0.5772156649015329
    normal = NormalDist()
    expected_maximum = sigma_sr * (
        (1 - gamma) * normal.inv_cdf(1 - 1 / 35) + gamma * normal.inv_cdf(1 - 1 / (35 * math.e))
    )
    sample_sd = math.sqrt(autocorrelation_denominator / (len(primary) - 1))
    count = len(primary)
    skew = (
        count
        / ((count - 1) * (count - 2))
        * sum(((value - primary_mean) / sample_sd) ** 3 for value in primary)
    )
    excess_kurtosis = count * (count + 1) / ((count - 1) * (count - 2) * (count - 3)) * sum(
        ((value - primary_mean) / sample_sd) ** 4 for value in primary
    ) - 3 * (count - 1) ** 2 / ((count - 2) * (count - 3))
    nonexcess_kurtosis = excess_kurtosis + 3
    denominator = 1 - skew * observed_sharpe + (nonexcess_kurtosis - 1) * observed_sharpe**2 / 4
    if denominator <= 0 or not math.isfinite(denominator):
        return {**failure, "T_eff": effective_count, "VIF": vif}
    z_score = (observed_sharpe - expected_maximum) * math.sqrt(effective_count - 1)
    z_score /= math.sqrt(denominator)
    probability = normal.cdf(z_score)
    if not math.isfinite(probability):
        probability = 0.0
    return {
        "probability": probability,
        "N": 35.0,
        "T_eff": effective_count,
        "VIF": vif,
        "sigma_SR": sigma_sr,
        "SR0": expected_maximum,
    }


def cscv_pbo(matrix: Sequence[Sequence[float]]) -> float:
    """Exact seven-trial/eight-block/all-70-split within-family PBO."""

    if (
        len(matrix) != 7
        or not matrix
        or len(matrix[0]) < 8
        or any(len(row) != len(matrix[0]) for row in matrix)
        or any(not math.isfinite(value) for row in matrix for value in row)
    ):
        return 1.0
    observation_count = len(matrix[0])
    quotient, remainder = divmod(observation_count, 8)
    blocks: list[tuple[int, int]] = []
    cursor = 0
    for index in range(8):
        size = quotient + int(index < remainder)
        if size <= 0:
            return 1.0
        blocks.append((cursor, cursor + size))
        cursor += size
    events = 0
    split_count = 0
    for training_blocks in itertools.combinations(range(8), 4):
        testing_blocks = tuple(index for index in range(8) if index not in training_blocks)

        def subset(row: Sequence[float], indices: Sequence[int]) -> list[float]:
            return [
                value for block in indices for value in row[blocks[block][0] : blocks[block][1]]
            ]

        training_sharpes = [daily_sharpe(subset(row, training_blocks)) for row in matrix]
        if any(math.isnan(value) for value in training_sharpes):
            return 1.0
        selected = max(range(7), key=lambda index: (training_sharpes[index], -index))
        testing_sharpes = [daily_sharpe(subset(row, testing_blocks)) for row in matrix]
        if any(math.isnan(value) for value in testing_sharpes):
            return 1.0
        selected_score = testing_sharpes[selected]
        tied = sum(value == selected_score for value in testing_sharpes)
        lower = sum(value < selected_score for value in testing_sharpes)
        average_rank = lower + (tied + 1) / 2
        relative_rank = average_rank / 8
        if not 0 < relative_rank < 1:
            return 1.0
        if math.log(relative_rank / (1 - relative_rank)) <= 0:
            events += 1
        split_count += 1
    return events / 70 if split_count == 70 else 1.0


def event_drawdown(wealth_path: Sequence[float]) -> float:
    if not wealth_path or any(not math.isfinite(value) or value <= 0 for value in wealth_path):
        raise VolatilityParityError("invalid event-level wealth path")
    peak = wealth_path[0]
    maximum = 0.0
    for wealth in wealth_path:
        peak = max(peak, wealth)
        maximum = max(maximum, 1 - wealth / peak)
    return maximum


def exceptional_profit(pnls: Sequence[float]) -> Mapping[str, float | bool]:
    if any(not math.isfinite(value) for value in pnls):
        return {"pass": False, "largest": math.inf, "top_five": math.inf}
    positive = sorted((value for value in pnls if value > 0), reverse=True)
    denominator = sum(positive)
    if denominator <= 0 or not math.isfinite(denominator):
        return {"pass": False, "largest": math.inf, "top_five": math.inf}
    largest = positive[0] / denominator
    top_five = sum(positive[:5]) / denominator
    return {
        "pass": largest <= 0.5 and top_five <= 0.75,
        "largest": largest,
        "top_five": top_five,
    }


def regime_gate(
    returns_by_regime: Mapping[str, Sequence[float]],
    rebalances_by_regime: Mapping[str, int],
) -> Mapping[str, Any]:
    eligible = {
        regime: values
        for regime, values in returns_by_regime.items()
        if len(values) >= 45 and rebalances_by_regime.get(regime, 0) >= 5
    }
    results = {
        regime: math.prod(1 + value for value in values) - 1 for regime, values in eligible.items()
    }
    passed = len(results) >= 3 and all(value > 0 and value >= -0.05 for value in results.values())
    return {"pass": passed, "eligible": tuple(sorted(results)), "returns": results}


def rebalance_minima(count: int, fold_counts: Sequence[int], *, holdout: bool = False) -> bool:
    required_total = 20 if holdout else 40
    required_folds = 2 if holdout else 4
    return (
        count >= required_total
        and len(fold_counts) == required_folds
        and all(value >= 8 for value in fold_counts)
    )


def create_holdout_latch(path: Path, payload: Mapping[str, Any]) -> None:
    """Exclusive-create and durably persist an unarmed one-shot latch."""

    invocation_id = payload.get("invocation_id")
    authorization_hashes = payload.get("authorization_hashes")
    if not isinstance(invocation_id, str) or not invocation_id:
        raise VolatilityParityError("latch invocation_id missing")
    if not isinstance(authorization_hashes, Mapping) or not authorization_hashes:
        raise VolatilityParityError("latch authorization hashes missing")
    value = {**payload, "accessed": False}
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise VolatilityParityError("pre-existing holdout latch") from exc
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def arm_holdout_latch(
    path: Path,
    expected_authorization_hashes: Mapping[str, str],
    *,
    first_access_at_utc: str | None = None,
) -> None:
    """Validate and irreversibly arm immediately before the first path resolution."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VolatilityParityError("malformed holdout latch") from exc
    if not isinstance(value, dict) or value.get("accessed") is not False:
        raise VolatilityParityError("holdout latch is malformed or already armed")
    if value.get("authorization_hashes") != dict(expected_authorization_hashes):
        raise VolatilityParityError("holdout latch authorization mismatch")
    timestamp = first_access_at_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    if not isinstance(timestamp, str) or not timestamp:
        raise VolatilityParityError("first access timestamp missing")
    armed = {**value, "accessed": True, "first_access_at_utc": timestamp}
    temporary = path.with_suffix(path.suffix + ".arming")
    encoded = json.dumps(armed, sort_keys=True, separators=(",", ":")).encode("utf-8")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise VolatilityParityError("pre-existing latch arming artifact") from exc
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def prospective_decision_key(signal_session_end: datetime) -> str:
    return f"{EXPERIMENT_ID}|{_utc(signal_session_end).isoformat()}"


def deduplicate_prospective_keys(keys: Sequence[str]) -> tuple[str, ...]:
    result: list[str] = []
    observed: set[str] = set()
    for key in keys:
        if key not in observed:
            result.append(key)
            observed.add(key)
    return tuple(result)
