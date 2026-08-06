"""Small clean-room evaluator for the frozen BTC/ETH mean-reversion contract.

This module intentionally does not import the prior strategy state machine or
accounting implementation.  It accepts already verified rows and is suitable
for deterministic fixtures before the production adapter is connected.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
    source_path: str = "fixture"
    row_index: int = -1

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
    execution_rows: Mapping[str, CleanRow] = MappingProxyType({})


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


@dataclass(frozen=True)
class TrialSpec:
    name: str
    horizon: int
    volatility_lookback: int | None
    entry: float
    exit: float
    maximum_holding_intervals: int
    raw: bool = False


TRIALS = (
    TrialSpec("primary_standardized_shock", 3, 20, -1.5, -0.25, 5),
    TrialSpec("raw_three_session_drawdown_baseline", 3, None, -0.05, 0.0, 5, True),
    TrialSpec("shorter_two_session_shock", 2, 20, -1.5, -0.25, 4),
    TrialSpec("longer_five_session_shock", 5, 20, -1.5, -0.25, 7),
    TrialSpec("shallower_entry", 3, 20, -1.25, -0.25, 5),
    TrialSpec("deeper_entry", 3, 20, -1.75, -0.25, 5),
    TrialSpec("slower_volatility_estimator", 3, 40, -1.5, -0.25, 5),
)


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


def build_daily_sessions(rows: Iterable[CleanRow]) -> tuple[CleanSession, ...]:
    """Construct the frozen daily sessions from exact 1-minute event rows."""
    grouped: dict[str, dict[datetime, dict[datetime, CleanRow]]] = {
        asset: {} for asset in ASSETS
    }
    for row in rows:
        session_day = (row.timestamp - timedelta(minutes=1)).date()
        session = datetime(session_day.year, session_day.month, session_day.day, tzinfo=UTC)
        grouped[row.asset].setdefault(session, {})[row.timestamp] = row
    days = sorted({day for items in grouped.values() for day in items})
    output: list[CleanSession] = []
    for day in days:
        expected = tuple(day + timedelta(minutes=i) for i in range(1, 1441))
        selected = {asset: grouped[asset].get(day, {}) for asset in ASSETS}
        complete = all(tuple(selected[asset]) == expected for asset in ASSETS)
        if complete:
            rows_at_close = {
                asset: selected[asset][expected[-1]] for asset in ASSETS
            }
            first_expected = day + timedelta(minutes=1)
            execution = {
                asset: grouped[asset].get(day, {}).get(first_expected, rows_at_close[asset])
                for asset in ASSETS
            }
        else:
            rows_at_close = {}
            execution = {}
        output.append(
            CleanSession(
                day,
                MappingProxyType(rows_at_close),
                complete,
                not complete,
                MappingProxyType(execution),
            )
        )
    return tuple(output)


def _signal(
    history: list[float], *, horizon: int = HORIZON, volatility_lookback: int | None = VOL_LOOKBACK
) -> float | None:
    lookback = volatility_lookback
    if len(history) < horizon + 1 or (lookback is not None and len(history) < lookback + 1):
        return None
    raw = history[-1] / history[-1 - horizon] - 1.0
    if lookback is None:
        return raw
    returns = [
        history[i] / history[i - 1] - 1.0
        for i in range(len(history) - lookback, len(history))
    ]
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    volatility = math.sqrt(variance)
    return (
        None
        if volatility <= 0 or not math.isfinite(volatility)
        else raw / (volatility * math.sqrt(horizon))
    )


def evaluate(
    sessions: tuple[CleanSession, ...], *, cost_bps: float = ONE_WAY_COST_BPS,
    trial: TrialSpec = TRIALS[0], delay_sessions: int = 0,
    start_timestamp: datetime | None = None,
    active_assets: tuple[str, ...] = ASSETS,
) -> CleanResult:
    """Run the frozen primary trial with explicit next-session exact execution."""
    if cost_bps not in (0.0, 14.0, 28.0):
        raise CleanRoomInvariantError("undeclared cost rate")
    if delay_sessions not in (0, 1):
        raise CleanRoomInvariantError("only frozen base or delayed execution is allowed")
    if not active_assets or any(asset not in ASSETS for asset in active_assets):
        raise CleanRoomInvariantError("invalid active asset subset")
    histories: dict[str, list[float]] = {asset: [] for asset in active_assets}
    weights = {asset: 0.0 for asset in active_assets}
    entry_index: dict[str, int | None] = {asset: None for asset in active_assets}
    pending: dict[str, tuple[int, float, datetime] | None] = {
        asset: None for asset in active_assets
    }
    equity = 1.0
    previous_prices: dict[str, float] | None = None
    decisions: list[CleanDecision] = []
    fills: list[CleanFill] = []
    intervals: list[float] = []
    total_cost = 0.0
    recovering = False
    recovery_count = 0
    for index, session in enumerate(sessions):
        if not session.complete:
            if any(
                weight > 0 or (item is not None and item[1] == 0.0)
                for weight, item in zip(weights.values(), pending.values(), strict=True)
            ):
                raise CleanRoomInvariantError("risky position cannot cross quarantine")
            histories = {asset: [] for asset in active_assets}
            pending = {asset: None for asset in active_assets}
            previous_prices = None
            recovering = True
            recovery_count = 0
            continue
        if recovering:
            recovery_count += 1
            if recovery_count >= 150:
                recovering = False
        if start_timestamp is not None and session.timestamp < start_timestamp:
            for asset in active_assets:
                histories[asset].append(session.rows[asset].close)
            continue
        if start_timestamp is not None and previous_prices is None:
            weights = {asset: 0.0 for asset in active_assets}
            pending = {asset: None for asset in active_assets}
        prices = {
            asset: session.execution_rows.get(asset, session.rows[asset]).close
            for asset in active_assets
        }
        execution_timestamp = next(iter(session.execution_rows.values()), None)
        fill_timestamp = (
            execution_timestamp.timestamp
            if execution_timestamp is not None
            else session.timestamp
        )
        interval_start_equity = equity
        if previous_prices is not None:
            marked = equity
            for asset in active_assets:
                marked *= 1.0 + weights[asset] * (prices[asset] / previous_prices[asset] - 1.0)
            equity = marked
        for asset in active_assets:
            scheduled = pending[asset]
            if scheduled is not None:
                scheduled_index, target_weight, decision_timestamp = scheduled
                if scheduled_index < index:
                    raise CleanRoomInvariantError("execution skipped exact target index")
                if scheduled_index == index:
                    turnover = abs(target_weight - weights[asset])
                    cost = equity * turnover * cost_bps / 10_000
                    equity -= cost
                    total_cost += cost
                    weights[asset] = target_weight
                    entry_index[asset] = index if target_weight > 0 else None
                    fills.append(
                        CleanFill(
                            asset,
                            decision_timestamp,
                            fill_timestamp,
                            prices[asset],
                            target_weight,
                            cost,
                        )
                    )
                    pending[asset] = None
        for asset in active_assets:
            # Signals use the completed information-session close, never the
            # later execution-row price.
            histories[asset].append(session.rows[asset].close)
            signal = None if recovering else _signal(
                histories[asset],
                horizon=trial.horizon,
                volatility_lookback=trial.volatility_lookback,
            )
            target: float | None = None
            if pending[asset] is not None:
                target = None
            elif weights[asset] == 0.0 and signal is not None and signal <= trial.entry:
                target = TARGET_WEIGHT
            elif weights[asset] > 0.0:
                held = entry_index[asset]
                if held is not None and (
                    index - held >= trial.maximum_holding_intervals
                    or (signal is not None and signal >= trial.exit)
                ):
                    target = 0.0
            decisions.append(
                CleanDecision(asset, session.timestamp, signal, weights[asset], target)
            )
            if target is not None:
                if index == len(sessions) - 1 and target == 0.0:
                    turnover = abs(target - weights[asset])
                    cost = equity * turnover * cost_bps / 10_000
                    equity -= cost
                    total_cost += cost
                    fills.append(
                        CleanFill(
                            asset,
                            session.timestamp,
                            fill_timestamp,
                            prices[asset],
                            target,
                            cost,
                        )
                    )
                    weights[asset] = target
                    entry_index[asset] = None
                    continue
                execution_index = index + 1 + delay_sessions
                if execution_index >= len(sessions) or not sessions[execution_index].complete:
                    raise CleanRoomInvariantError("exact next-session execution row missing")
                pending[asset] = (execution_index, target, session.timestamp)
        if previous_prices is not None:
            intervals.append(equity / interval_start_equity - 1.0)
        previous_prices = prices
    if sessions and any(weight != 0.0 for weight in weights.values()):
        terminal = sessions[-1]
        terminal_start_equity = equity
        terminal_prices = {
            asset: terminal.rows[asset].close for asset in active_assets
        }
        terminal_time = terminal.rows[active_assets[0]].timestamp
        for asset in active_assets:
            if weights[asset] == 0.0:
                continue
            cost = equity * weights[asset] * cost_bps / 10_000
            equity -= cost
            total_cost += cost
            fills.append(
                CleanFill(
                    asset,
                    terminal.timestamp,
                    terminal_time,
                    terminal_prices[asset],
                    0.0,
                    cost,
                )
            )
            weights[asset] = 0.0
            entry_index[asset] = None
        if equity != terminal_start_equity:
            intervals.append(equity / terminal_start_equity - 1.0)
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
