"""Narrow clean-room primitives for the frozen BTC/ETH rotation contract.

This module deliberately owns the score, decision, and execution-clock logic;
it does not import the prior relative-value state machine or accounting code.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast

from .mean_reversion_v5_cleanroom import ASSETS, CleanSession

Asset = Literal["BTCUSDT", "ETHUSDT"]
Target = Literal["CASH", "BTCUSDT", "ETHUSDT"]
HORIZONS = (20, 60, 120)
VOL_LOOKBACK = 20
GAP = 0.25


class RelativeCleanRoomError(ValueError):
    """Raised when a frozen relative-value invariant is violated."""


@dataclass(frozen=True)
class RelativeDecision:
    session_timestamp: datetime
    information_timestamp: datetime
    target: Target
    scores: tuple[tuple[Asset, float], ...]


@dataclass(frozen=True)
class RelativeFill:
    target_timestamp: datetime
    execution_timestamp: datetime
    target: Target
    cost: float


@dataclass(frozen=True)
class RelativeResult:
    decisions: tuple[RelativeDecision, ...]
    fills: tuple[RelativeFill, ...]
    interval_returns: tuple[float, ...]
    terminal_cash: bool


@dataclass(frozen=True)
class RelativeEconomicResult:
    net_return: float
    costs: float
    interval_returns: tuple[float, ...]
    decisions: tuple[RelativeDecision, ...]
    fills: tuple[RelativeFill, ...]
    terminal_cash: bool


def _log_return(closes: list[float], horizon: int) -> float:
    if len(closes) <= horizon:
        raise RelativeCleanRoomError("insufficient causal lookback")
    value = math.log(closes[-1] / closes[-1 - horizon])
    if not math.isfinite(value):
        raise RelativeCleanRoomError("nonfinite return")
    return value


def _score(
    closes: list[float], horizons: tuple[int, ...] = HORIZONS
) -> tuple[float, tuple[float, ...]]:
    raw = tuple(_log_return(closes, horizon) for horizon in horizons)
    if len(closes) <= VOL_LOOKBACK:
        raise RelativeCleanRoomError("insufficient volatility lookback")
    returns = [
        math.log(closes[i] / closes[i - 1])
        for i in range(len(closes) - VOL_LOOKBACK + 1, len(closes))
    ]
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    volatility = math.sqrt(variance)
    if not math.isfinite(volatility) or volatility <= 0:
        raise RelativeCleanRoomError("invalid volatility")
    standardized = tuple(
        value / (volatility * math.sqrt(horizon))
        for value, horizon in zip(raw, horizons, strict=True)
    )
    return sum(standardized) / len(standardized), raw


def decide(
    sessions: tuple[CleanSession, ...],
    index: int,
    actual: Target,
    horizons: tuple[int, ...] = HORIZONS,
) -> RelativeDecision | None:
    """Create one causal decision from complete synchronized session closes."""
    if index < max(max(horizons), VOL_LOOKBACK) or not sessions[index].complete:
        return None
    for session in sessions[index - max(max(horizons), VOL_LOOKBACK) : index + 1]:
        if not session.complete or set(session.rows) != set(ASSETS):
            return None
    closes = {
        asset: [session.rows[asset].close for session in sessions[: index + 1]] for asset in ASSETS
    }
    btc_score, btc_raw = _score(closes[ASSETS[0]], horizons)
    eth_score, eth_raw = _score(closes[ASSETS[1]], horizons)
    winner: Asset = cast(Asset, ASSETS[0] if btc_score >= eth_score else ASSETS[1])
    winner_raw = btc_raw if winner == ASSETS[0] else eth_raw
    loser_raw = eth_raw if winner == ASSETS[0] else btc_raw
    target: Target = (
        winner
        if (winner_raw[1] - loser_raw[1] >= GAP and sum(winner_raw) / len(winner_raw) > 0)
        else "CASH"
    )
    return RelativeDecision(
        sessions[index].timestamp,
        sessions[index].rows[ASSETS[0]].timestamp,
        target,
        ((cast(Asset, ASSETS[0]), btc_score), (cast(Asset, ASSETS[1]), eth_score)),
    )


def evaluate_fixture(sessions: tuple[CleanSession, ...], cost_bps: float = 14.0) -> RelativeResult:
    """Small deterministic reference-clock evaluator for known-answer fixtures."""
    if not sessions:
        raise RelativeCleanRoomError("empty sessions")
    actual: Target = "CASH"
    pending: tuple[int, RelativeDecision] | None = None
    decisions: list[RelativeDecision] = []
    fills: list[RelativeFill] = []
    intervals: list[float] = []
    wealth = 1.0
    for index, session in enumerate(sessions):
        if not session.complete:
            if actual != "CASH" or pending is not None:
                raise RelativeCleanRoomError("exposure crosses incomplete session")
            continue
        if pending is not None and pending[0] == index:
            due_decision = pending[1]
            fee = (
                abs(
                    (1.0 if due_decision.target != "CASH" else 0.0)
                    - (1.0 if actual != "CASH" else 0.0)
                )
                * cost_bps
                / 10_000
            )
            wealth *= 1.0 - fee
            fills.append(
                RelativeFill(
                    due_decision.session_timestamp,
                    session.rows[ASSETS[0]].timestamp,
                    due_decision.target,
                    fee,
                )
            )
            actual = due_decision.target
            pending = None
        new_decision = decide(sessions, index, actual)
        if new_decision is not None:
            decisions.append(new_decision)
            if index + 1 < len(sessions):
                pending = (index + 1, new_decision)
        if index:
            intervals.append(0.0)
    if actual != "CASH":
        fee = cost_bps / 10_000
        wealth *= 1.0 - fee
        fills.append(
            RelativeFill(
                sessions[-1].timestamp, sessions[-1].rows[ASSETS[0]].timestamp, "CASH", fee
            )
        )
    return RelativeResult(tuple(decisions), tuple(fills), tuple(intervals), True)


def evaluate_development(
    sessions: tuple[CleanSession, ...],
    *,
    cost_bps: float = 14.0,
    delay_sessions: int = 0,
    start_timestamp: datetime | None = None,
) -> RelativeEconomicResult:
    """Evaluate one frozen trial on complete synchronized daily sessions."""
    if cost_bps not in (0.0, 14.0, 28.0) or delay_sessions not in (0, 1):
        raise RelativeCleanRoomError("undeclared stress")
    if not sessions:
        raise RelativeCleanRoomError("empty sessions")
    history: dict[str, list[float]] = {asset: [] for asset in ASSETS}
    actual: Target = "CASH"
    pending: tuple[int, RelativeDecision] | None = None
    decisions: list[RelativeDecision] = []
    fills: list[RelativeFill] = []
    intervals: list[float] = []
    equity = 1.0
    costs = 0.0
    previous_prices: dict[str, float] | None = None
    started = False
    causal_sessions: list[CleanSession] = []
    for index, session in enumerate(sessions):
        if not session.complete:
            if actual != "CASH" or pending is not None:
                raise RelativeCleanRoomError("exposure crosses quarantine")
            history = {asset: [] for asset in ASSETS}
            causal_sessions = []
            previous_prices = None
            continue
        for asset in ASSETS:
            history[asset].append(session.rows[asset].close)
        causal_sessions.append(session)
        if start_timestamp is not None and session.timestamp < start_timestamp:
            continue
        if not started:
            started = True
            actual = "CASH"
            pending = None
            previous_prices = None
        prices = {
            asset: session.execution_rows[asset].close
            for asset in ASSETS
            if asset in session.execution_rows
        }
        if set(prices) != set(ASSETS):
            raise RelativeCleanRoomError("missing synchronized exact execution rows")
        if previous_prices is not None and actual != "CASH":
            asset = actual
            intervals.append(prices[asset] / previous_prices[asset] - 1.0)
            equity *= 1.0 + intervals[-1]
        elif previous_prices is not None:
            intervals.append(0.0)
        if pending is not None and pending[0] == index:
            target = pending[1].target
            old_exposure = 0.0 if actual == "CASH" else 1.0
            new_exposure = 0.0 if target == "CASH" else 1.0
            fee = equity * abs(new_exposure - old_exposure) * cost_bps / 10_000
            equity -= fee
            costs += fee
            fills.append(
                RelativeFill(
                    pending[1].session_timestamp,
                    session.execution_rows[ASSETS[0]].timestamp,
                    target,
                    fee,
                )
            )
            actual = target
            pending = None
        if index == len(sessions) - 1:
            pending = None
            if actual != "CASH":
                fee = equity * cost_bps / 10_000
                equity -= fee
                costs += fee
                fills.append(
                    RelativeFill(
                        session.timestamp,
                        session.execution_rows[ASSETS[0]].timestamp,
                        "CASH",
                        fee,
                    )
                )
                intervals.append(-fee / (equity + fee))
                actual = "CASH"
            previous_prices = prices
            continue
        decision = decide(tuple(causal_sessions), len(causal_sessions) - 1, actual)
        if decision is not None:
            decisions.append(decision)
            if pending is None:
                pending = (index + 1 + delay_sessions, decision)
        previous_prices = prices
    if actual != "CASH" or pending is not None:
        raise RelativeCleanRoomError("terminal cash invariant")
    return RelativeEconomicResult(
        equity - 1.0, costs, tuple(intervals), tuple(decisions), tuple(fills), True
    )
