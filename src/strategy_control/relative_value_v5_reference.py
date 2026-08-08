"""Intentionally separate reference score/decision calculator for RV v5."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from .mean_reversion_v5_cleanroom import ASSETS, CleanSession

Target = Literal["CASH", "BTCUSDT", "ETHUSDT"]


@dataclass(frozen=True)
class ReferenceEconomicResult:
    net_return: float
    costs: float
    decisions: tuple[Target, ...]
    fills: int
    terminal_cash: bool


def reference_decide(sessions: tuple[CleanSession, ...], index: int) -> Target | None:
    if index < 120 or not sessions[index].complete:
        return None
    window = sessions[index - 120 : index + 1]
    if any(not item.complete or set(item.rows) != set(ASSETS) for item in window):
        return None
    scores: dict[str, tuple[float, tuple[float, ...]]] = {}
    for asset in ASSETS:
        close = [item.rows[asset].close for item in sessions[: index + 1]]
        raw = tuple(math.log(close[-1] / close[-1 - horizon]) for horizon in (20, 60, 120))
        one = [math.log(close[i] / close[i - 1]) for i in range(len(close) - 19, len(close))]
        average = sum(one) / len(one)
        variance = sum((value - average) ** 2 for value in one) / (len(one) - 1)
        vol = math.sqrt(variance)
        scores[asset] = (
            sum(
                value / (vol * math.sqrt(h))
                for value, h in zip(raw, (20, 60, 120), strict=True)
            )
            / 3,
            raw,
        )
    winner = ASSETS[0] if scores[ASSETS[0]][0] >= scores[ASSETS[1]][0] else ASSETS[1]
    loser = ASSETS[1] if winner == ASSETS[0] else ASSETS[0]
    if scores[winner][1][1] - scores[loser][1][1] >= 0.25 and sum(scores[winner][1]) / 3 > 0:
        return winner  # type: ignore[return-value]
    return "CASH"


def reference_evaluate(
    sessions: tuple[CleanSession, ...],
    *,
    cost_bps: float = 14.0,
    start_timestamp: datetime | None = None,
) -> ReferenceEconomicResult:
    """Independent cash/one-asset accounting reference for a development fold."""
    actual: Target = "CASH"
    pending: tuple[int, Target] | None = None
    equity = 1.0
    costs = 0.0
    decisions: list[Target] = []
    fills = 0
    previous: dict[str, float] | None = None
    started = False
    causal: list[CleanSession] = []
    for index, session in enumerate(sessions):
        if not session.complete:
            if actual != "CASH" or pending is not None:
                raise ValueError("reference exposure crosses incomplete session")
            causal = []
            previous = None
            continue
        causal.append(session)
        if start_timestamp is not None and session.timestamp < start_timestamp:
            continue
        if not started:
            started = True
            actual = "CASH"
            pending = None
            previous = None
        prices = {asset: session.execution_rows[asset].close for asset in ASSETS}
        if previous is not None and actual != "CASH":
            equity *= prices[actual] / previous[actual]
        if pending is not None and pending[0] == index:
            target = pending[1]
            fee = (
                equity
                * abs(float(target != "CASH") - float(actual != "CASH"))
                * cost_bps
                / 10_000
            )
            equity -= fee
            costs += fee
            fills += 1
            actual = target
            pending = None
        if index == len(sessions) - 1:
            if actual != "CASH":
                fee = equity * cost_bps / 10_000
                equity -= fee
                costs += fee
                fills += 1
                actual = "CASH"
            previous = prices
            continue
        next_target = reference_decide(tuple(causal), len(causal) - 1)
        if next_target is not None:
            decisions.append(next_target)
            pending = (index + 1, next_target)
        previous = prices
    if actual != "CASH" or pending is not None:
        raise ValueError("reference terminal cash invariant")
    return ReferenceEconomicResult(equity - 1.0, costs, tuple(decisions), fills, True)
