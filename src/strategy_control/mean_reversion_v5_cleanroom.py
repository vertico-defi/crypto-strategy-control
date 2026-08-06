"""Small clean-room evaluator for the frozen BTC/ETH mean-reversion contract.

This module intentionally does not import the prior strategy state machine or
accounting implementation.  It accepts already verified rows and is suitable
for deterministic fixtures before the production adapter is connected.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

ASSETS = ("BTCUSDT", "ETHUSDT")
ENTRY_Z = -1.5
EXIT_Z = -0.25
HORIZON = 3
VOL_LOOKBACK = 20
MAX_HOLDING = 5
TARGET_WEIGHT = 0.5
ONE_WAY_COST_BPS = 14.0


class CleanRoomInvariantError(ValueError):
    """A frozen-contract or trace invariant failed closed."""


@dataclass(frozen=True)
class CleanRow:
    asset: str
    timestamp: datetime
    close: float

    def __post_init__(self) -> None:
        if self.asset not in ASSETS or self.timestamp.tzinfo != UTC:
            raise CleanRoomInvariantError("invalid row identity")
        if not math.isfinite(self.close) or self.close <= 0:
            raise CleanRoomInvariantError("invalid close")


@dataclass(frozen=True)
class CleanSession:
    timestamp: datetime
    rows: Mapping[str, CleanRow]
    complete: bool
    quarantine: bool = False


@dataclass(frozen=True)
class CleanDecision:
    asset: str
    information_timestamp: datetime
    signal: float | None
    prior_weight: float
    target_weight: float | None


@dataclass(frozen=True)
class CleanFill:
    asset: str
    target_timestamp: datetime
    execution_timestamp: datetime
    price: float
    target_weight: float
    cost: float


@dataclass(frozen=True)
class CleanResult:
    terminal_equity: float
    net_return: float
    costs: float
    decisions: tuple[CleanDecision, ...]
    fills: tuple[CleanFill, ...]
    interval_returns: tuple[float, ...]
    terminal_cash: bool


def _utc(value: datetime) -> datetime:
    if value.tzinfo != UTC:
        raise CleanRoomInvariantError("timestamps must be UTC")
    return value


def build_sessions(
    rows: Iterable[CleanRow], timestamps: Iterable[datetime]
) -> tuple[CleanSession, ...]:
    """Build exact sessions; duplicates and nonmonotonic asset rows fail closed."""
    ordered = tuple(_utc(item) for item in timestamps)
    if tuple(sorted(set(ordered))) != ordered:
        raise CleanRoomInvariantError("session timestamps are not strictly increasing")
    by_asset: dict[str, dict[datetime, CleanRow]] = {asset: {} for asset in ASSETS}
    previous: dict[str, datetime | None] = {asset: None for asset in ASSETS}
    for row in rows:
        if row.timestamp in by_asset[row.asset]:
            raise CleanRoomInvariantError("duplicate asset/timestamp row")
        prior = previous[row.asset]
        if prior is not None and row.timestamp <= prior:
            raise CleanRoomInvariantError("nonmonotonic rows")
        by_asset[row.asset][row.timestamp] = row
        previous[row.asset] = row.timestamp
    output: list[CleanSession] = []
    for timestamp in ordered:
        selected = {
            asset: by_asset[asset][timestamp]
            for asset in ASSETS
            if timestamp in by_asset[asset]
        }
        output.append(
            CleanSession(
                timestamp,
                MappingProxyType(selected),
                len(selected) == len(ASSETS),
                len(selected) != len(ASSETS),
            )
        )
    return tuple(output)


def _signal(history: list[float]) -> float | None:
    if len(history) < max(HORIZON, VOL_LOOKBACK) + 1:
        return None
    raw = history[-1] / history[-1 - HORIZON] - 1.0
    returns = [
        history[i] / history[i - 1] - 1.0
        for i in range(len(history) - VOL_LOOKBACK, len(history))
    ]
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    volatility = math.sqrt(variance)
    return (
        None
        if volatility <= 0 or not math.isfinite(volatility)
        else raw / (volatility * math.sqrt(HORIZON))
    )


def evaluate(
    sessions: tuple[CleanSession, ...], *, cost_bps: float = ONE_WAY_COST_BPS
) -> CleanResult:
    """Run the frozen primary trial with explicit next-session exact execution."""
    if cost_bps not in (14.0, 28.0):
        raise CleanRoomInvariantError("undeclared cost rate")
    histories: dict[str, list[float]] = {asset: [] for asset in ASSETS}
    weights = {asset: 0.0 for asset in ASSETS}
    entry_index: dict[str, int | None] = {asset: None for asset in ASSETS}
    pending: dict[str, tuple[int, float] | None] = {asset: None for asset in ASSETS}
    equity = 1.0
    previous_prices: dict[str, float] | None = None
    decisions: list[CleanDecision] = []
    fills: list[CleanFill] = []
    intervals: list[float] = []
    total_cost = 0.0
    for index, session in enumerate(sessions):
        if not session.complete:
            if any(
                weight > 0 or (item is not None and item[1] == 0.0)
                for weight, item in zip(weights.values(), pending.values(), strict=True)
            ):
                raise CleanRoomInvariantError("risky position cannot cross quarantine")
            histories = {asset: [] for asset in ASSETS}
            pending = {asset: None for asset in ASSETS}
            previous_prices = None
            continue
        prices = {asset: session.rows[asset].close for asset in ASSETS}
        if previous_prices is not None:
            marked = equity
            for asset in ASSETS:
                marked *= 1.0 + weights[asset] * (prices[asset] / previous_prices[asset] - 1.0)
            intervals.append(marked / equity - 1.0)
            equity = marked
        for asset in ASSETS:
            scheduled = pending[asset]
            if scheduled is not None:
                scheduled_index, target_weight = scheduled
                if scheduled_index != index:
                    raise CleanRoomInvariantError("execution skipped exact target index")
                turnover = abs(target_weight - weights[asset])
                cost = equity * turnover * cost_bps / 10_000
                equity -= cost
                total_cost += cost
                weights[asset] = target_weight
                entry_index[asset] = index if target_weight > 0 else None
                fills.append(
                    CleanFill(
                        asset,
                        sessions[scheduled_index - 1].timestamp,
                        session.timestamp,
                        prices[asset],
                        target_weight,
                        cost,
                    )
                )
                pending[asset] = None
        for asset in ASSETS:
            histories[asset].append(prices[asset])
            signal = _signal(histories[asset])
            target: float | None = None
            if weights[asset] == 0.0 and signal is not None and signal <= ENTRY_Z:
                target = TARGET_WEIGHT
            elif weights[asset] > 0.0:
                held = entry_index[asset]
                if held is not None and (
                    index - held >= MAX_HOLDING
                    or (signal is not None and signal >= EXIT_Z)
                ):
                    target = 0.0
            decisions.append(
                CleanDecision(asset, session.timestamp, signal, weights[asset], target)
            )
            if target is not None:
                if index + 1 >= len(sessions) or not sessions[index + 1].complete:
                    raise CleanRoomInvariantError("exact next-session execution row missing")
                pending[asset] = (index + 1, target)
        previous_prices = prices
    if any(weight != 0.0 for weight in weights.values()) or any(
        item is not None for item in pending.values()
    ):
        raise CleanRoomInvariantError("terminal liquidation did not produce cash")
    return CleanResult(
        equity,
        equity - 1.0,
        total_cost,
        tuple(decisions),
        tuple(fills),
        tuple(intervals),
        True,
    )
