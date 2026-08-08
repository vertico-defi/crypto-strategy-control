"""Intentionally separate reference score/decision calculator for RV v5."""

from __future__ import annotations

import math
from typing import Literal

from .mean_reversion_v5_cleanroom import ASSETS, CleanSession

Target = Literal["CASH", "BTCUSDT", "ETHUSDT"]


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
