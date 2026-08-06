"""Intentionally simple independent reference calculator for v5 traces."""

from __future__ import annotations

import math
from dataclasses import dataclass

from .mean_reversion_v5_cleanroom import (
    ASSETS,
    ENTRY_Z,
    EXIT_Z,
    HORIZON,
    MAX_HOLDING,
    TARGET_WEIGHT,
    VOL_LOOKBACK,
    CleanSession,
)


@dataclass(frozen=True)
class ReferenceResult:
    terminal_equity: float
    net_return: float
    costs: float
    decisions: tuple[tuple[str, object, float | None, float, float | None], ...]
    fills: tuple[tuple[str, object, object, float, float], ...]
    interval_returns: tuple[float, ...]


def _signal(values: list[float]) -> float | None:
    if len(values) < max(HORIZON, VOL_LOOKBACK) + 1:
        return None
    raw = values[-1] / values[-1 - HORIZON] - 1.0
    changes = [
        values[index] / values[index - 1] - 1.0
        for index in range(len(values) - VOL_LOOKBACK, len(values))
    ]
    mean = sum(changes) / len(changes)
    variance = sum((item - mean) ** 2 for item in changes) / (len(changes) - 1)
    volatility = math.sqrt(variance)
    return None if volatility <= 0 else raw / (volatility * math.sqrt(HORIZON))


def calculate(sessions: tuple[CleanSession, ...], cost_bps: float = 14.0) -> ReferenceResult:
    """A separate loop-based calculation; no production state or accounting calls."""
    history: dict[str, list[float]] = {asset: [] for asset in ASSETS}
    weight = {asset: 0.0 for asset in ASSETS}
    entry: dict[str, int | None] = {asset: None for asset in ASSETS}
    pending: dict[str, tuple[int, float] | None] = {asset: None for asset in ASSETS}
    decisions: list[tuple[str, object, float | None, float, float | None]] = []
    fills: list[tuple[str, object, object, float, float]] = []
    intervals: list[float] = []
    equity = 1.0
    costs = 0.0
    previous: dict[str, float] | None = None
    for index, session in enumerate(sessions):
        if not session.complete:
            history = {asset: [] for asset in ASSETS}
            pending = {asset: None for asset in ASSETS}
            previous = None
            continue
        mark = {
            asset: session.execution_rows.get(asset, session.rows[asset]).close
            for asset in ASSETS
        }
        interval_start_equity = equity
        if previous is not None:
            before = equity
            for asset in ASSETS:
                before *= 1.0 + weight[asset] * (mark[asset] / previous[asset] - 1.0)
            equity = before
        execution_row = next(iter(session.execution_rows.values()), None)
        execution_time = execution_row.timestamp if execution_row is not None else session.timestamp
        for asset in ASSETS:
            order = pending[asset]
            if order is None:
                continue
            if order[0] != index:
                raise ValueError("reference skipped a pending exact execution")
            target = order[1]
            fee = equity * abs(target - weight[asset]) * cost_bps / 10_000
            equity -= fee
            costs += fee
            weight[asset] = target
            entry[asset] = index if target > 0 else None
            fills.append(
                (asset, sessions[order[0] - 1].timestamp, execution_time, mark[asset], target)
            )
            pending[asset] = None
        for asset in ASSETS:
            value = session.rows[asset].close
            history[asset].append(value)
            signal = _signal(history[asset])
            desired: float | None = None
            if weight[asset] == 0 and signal is not None and signal <= ENTRY_Z:
                desired = TARGET_WEIGHT
            elif weight[asset] > 0:
                held = entry[asset]
                if held is not None and (
                    index - held >= MAX_HOLDING
                    or (signal is not None and signal >= EXIT_Z)
                ):
                    desired = 0.0
            decisions.append((asset, session.timestamp, signal, weight[asset], desired))
            if desired is not None:
                if index == len(sessions) - 1 and desired == 0:
                    fee = equity * weight[asset] * cost_bps / 10_000
                    equity -= fee
                    costs += fee
                    fills.append(
                        (
                            asset,
                            session.timestamp,
                            session.rows[asset].timestamp,
                            session.rows[asset].close,
                            0.0,
                        )
                    )
                    weight[asset] = 0.0
                    entry[asset] = None
                else:
                    if index + 1 >= len(sessions) or not sessions[index + 1].complete:
                        raise ValueError("reference missing exact next execution")
                    pending[asset] = (index + 1, desired)
        if previous is not None:
            intervals.append(equity / interval_start_equity - 1.0)
        previous = mark
    if any(weight[asset] != 0 for asset in ASSETS):
        terminal = sessions[-1]
        terminal_start_equity = equity
        for asset in ASSETS:
            if weight[asset] == 0:
                continue
            fee = equity * weight[asset] * cost_bps / 10_000
            equity -= fee
            costs += fee
            fills.append(
                (
                    asset,
                    terminal.timestamp,
                    terminal.rows[asset].timestamp,
                    terminal.rows[asset].close,
                    0.0,
                )
            )
        if equity != terminal_start_equity:
            intervals.append(equity / terminal_start_equity - 1.0)
    return ReferenceResult(
        equity, equity - 1.0, costs, tuple(decisions), tuple(fills), tuple(intervals)
    )
