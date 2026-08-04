"""Pure, fail-closed mechanics for frozen mean-reversion v2.

This module accepts only in-memory synthetic or independently verified records.
It has no filesystem, data-loader, network, exchange, wallet, or order-routing
interface. Production integration must verify and supply causal records later.
"""

from __future__ import annotations

import hashlib
import importlib
import itertools
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, datetime, timedelta
from statistics import NormalDist
from types import MappingProxyType
from typing import Any


class MeanReversionV2Error(ValueError):
    """A frozen v2 invariant was not met."""


ASSETS = ("BTCUSDT", "ETHUSDT")
BOOTSTRAP_SEED = 4480959964820476661
RECOVERY_SESSIONS = 150
ONE_WAY_COST_BPS = 14.0
DOUBLED_ONE_WAY_COST_BPS = 28.0


@dataclass(frozen=True)
class Trial:
    """One frozen trial; the primary may never be replaced by a neighbor."""

    name: str
    horizon: int
    volatility_lookback: int | None
    entry: float
    exit: float
    maximum_holding_intervals: int
    raw: bool = False


TRIALS = (
    Trial("primary_standardized_shock", 3, 20, -1.5, -0.25, 5),
    Trial("raw_three_session_drawdown_baseline", 3, None, -0.05, 0.0, 5, True),
    Trial("shorter_two_session_shock", 2, 20, -1.5, -0.25, 4),
    Trial("longer_five_session_shock", 5, 20, -1.5, -0.25, 7),
    Trial("shallower_entry", 3, 20, -1.25, -0.25, 5),
    Trial("deeper_entry", 3, 20, -1.75, -0.25, 5),
    Trial("slower_volatility_estimator", 3, 40, -1.5, -0.25, 5),
)
TRIAL_ORDER = tuple(item.name for item in TRIALS)
GATE_NAMES = (
    "aggregate_net_return_gt",
    "annualized_sharpe_gte",
    "positive_folds_minimum",
    "fold_count",
    "maximum_drawdown_lte",
    "doubled_cost_aggregate_net_return_gt",
    "additional_delay_aggregate_net_return_gt",
    "positive_parameter_neighbors_minimum",
    "parameter_neighbor_count",
    "asset_standalone_net_return_each_gt",
    "completed_entries_total_minimum",
    "completed_entries_each_asset_minimum",
    "bootstrap_mean_daily_net_return_lower_95_ci_gt",
    "deflated_sharpe_probability_gte",
    "probability_of_backtest_overfitting_lte",
    "regime_gate",
    "exceptional_trade_gate",
    "baseline_superiority",
    "no_material_leakage",
)
DEVELOPMENT_FOLDS = (
    (
        datetime(2025, 1, 1, tzinfo=UTC),
        datetime(2025, 4, 1, tzinfo=UTC),
    ),
    (
        datetime(2025, 4, 1, tzinfo=UTC),
        datetime(2025, 7, 1, tzinfo=UTC),
    ),
    (
        datetime(2025, 7, 1, tzinfo=UTC),
        datetime(2025, 10, 1, tzinfo=UTC),
    ),
    (
        datetime(2025, 10, 1, tzinfo=UTC),
        datetime(2026, 1, 1, tzinfo=UTC),
    ),
)
GATE_REQUIREMENTS: Mapping[str, object] = MappingProxyType(
    {
        "aggregate_net_return_gt": 0.0,
        "annualized_sharpe_gte": 0.75,
        "positive_folds_minimum": 3,
        "fold_count": 4,
        "maximum_drawdown_lte": 0.2,
        "doubled_cost_aggregate_net_return_gt": 0.0,
        "additional_delay_aggregate_net_return_gt": 0.0,
        "positive_parameter_neighbors_minimum": 3,
        "parameter_neighbor_count": 4,
        "asset_standalone_net_return_each_gt": 0.0,
        "completed_entries_total_minimum": 24,
        "completed_entries_each_asset_minimum": 10,
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": 0.0,
        "deflated_sharpe_probability_gte": 0.95,
        "probability_of_backtest_overfitting_lte": 0.2,
        "regime_gate": "pass",
        "exceptional_trade_gate": "pass",
        "baseline_superiority": (
            "primary Sharpe strictly above both equal-weight buy-and-hold and "
            "raw-drawdown-baseline Sharpes, and primary maximum drawdown strictly "
            "below both comparators' maximum drawdowns"
        ),
        "no_material_leakage": True,
    }
)


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise MeanReversionV2Error(f"{field} must be timezone-aware UTC")
    normalized = value.astimezone(UTC)
    if value.utcoffset() != UTC.utcoffset(normalized):
        raise MeanReversionV2Error(f"{field} must be UTC")
    return normalized


def canonical_hash(value: object) -> str:
    """Hash sorted-key compact UTF-8 JSON with finite values and UTC timestamps."""

    def normalize(item: object) -> object:
        if isinstance(item, datetime):
            return _utc(item, field="canonical timestamp").isoformat().replace("+00:00", "Z")
        if is_dataclass(item) and not isinstance(item, type):
            return normalize(asdict(item))
        if isinstance(item, Mapping):
            if any(not isinstance(key, str) for key in item):
                raise MeanReversionV2Error("canonical mapping keys must be strings")
            return {str(key): normalize(child) for key, child in item.items()}
        if isinstance(item, (tuple, list)):
            return [normalize(child) for child in item]
        if isinstance(item, float) and not math.isfinite(item):
            raise MeanReversionV2Error("nonfinite canonical value")
        return item

    payload = json.dumps(
        normalize(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def guard_development_relative_path(relative_path: str) -> str:
    """Reject traversal, absolute paths, and every holdout path before filesystem use."""

    parts = relative_path.split("/")
    if (
        not relative_path
        or relative_path.startswith("/")
        or "\\" in relative_path
        or any(part in {"", ".", ".."} for part in parts)
        or "year=2026" in parts
    ):
        raise MeanReversionV2Error("development path guard rejected path before access")
    return relative_path


def strict_prefix(
    rows: Sequence[tuple[datetime, object]], end: datetime
) -> list[tuple[datetime, object]]:
    """Validate row order and slice before any fold transformation."""

    boundary = _utc(end, field="fold end")
    output: list[tuple[datetime, object]] = []
    previous: datetime | None = None
    for timestamp, row in rows:
        normalized = _utc(timestamp, field="row timestamp")
        if normalized >= boundary:
            break
        if previous is not None and normalized <= previous:
            raise MeanReversionV2Error("duplicate or nonmonotonic row timestamp")
        previous = normalized
        output.append((normalized, row))
    return output


def causal_gap_segments(
    sessions: Sequence[tuple[datetime, bool]], *, recovery: int = RECOVERY_SESSIONS
) -> list[int | None]:
    """Return eligible segment ids only after 150 new contiguous complete sessions."""

    if recovery != RECOVERY_SESSIONS:
        raise MeanReversionV2Error("recovery must be exactly 150 sessions")
    result: list[int | None] = []
    complete_run = 0
    segment = 0
    previous: datetime | None = None
    for supplied_session, supplied_complete in sessions:
        session = _utc(supplied_session, field="session")
        if previous is not None and session <= previous:
            raise MeanReversionV2Error("duplicate or nonmonotonic session")
        contiguous = previous is None or session - previous == timedelta(days=1)
        previous = session
        complete = supplied_complete and contiguous
        if not complete:
            complete_run = 0
            segment += 1
            result.append(None)
            continue
        complete_run += 1
        result.append(segment if complete_run >= recovery else None)
    return result


@dataclass(frozen=True)
class Decision:
    asset: str
    session: datetime
    actual_before: float
    pending: bool
    signal: float | None
    target: float | None
    recovery_signal: float | None = None


@dataclass(frozen=True)
class Target:
    target_id: str
    asset: str
    decision_session: datetime
    fill_index: int
    fill_time: datetime
    desired_weight: float


def target_identity(target: Target) -> str:
    """Derive the frozen target id from all economic target fields except itself."""

    return canonical_hash(
        (
            target.asset,
            target.decision_session,
            target.fill_index,
            target.fill_time,
            target.desired_weight,
        )
    )


@dataclass(frozen=True)
class Fill:
    target_id: str
    asset: str
    fill_index: int
    timestamp: datetime
    price: float
    target_weight: float


@dataclass(frozen=True)
class Disposition:
    target_id: str
    kind: str
    timestamp: datetime


class Clock:
    """Per-asset state with exact fill-before-later-decision semantics."""

    def __init__(self, trial: Trial = TRIALS[0], delay: int = 0) -> None:
        if delay not in (0, 1):
            raise MeanReversionV2Error("only frozen base or one-eligible-fill delay")
        self.trial = trial
        self.delay = delay
        self.actual = 0.0
        self.pending: Target | None = None
        self.entry_fill_index: int | None = None
        self.asset: str | None = None

    def apply_fill(self, timestamp: datetime, price: float, fill_index: int) -> Fill | None:
        """Execute only the exact pending fill; never scan to a later vector."""

        supplied_time = _utc(timestamp, field="fill timestamp")
        if not math.isfinite(price) or price <= 0 or fill_index < 0:
            raise MeanReversionV2Error("invalid exact fill")
        if self.pending is None:
            return None
        target = self.pending
        if fill_index < target.fill_index:
            if supplied_time >= target.fill_time:
                raise MeanReversionV2Error("inconsistent eligible fill index and timestamp")
            return None
        if fill_index != target.fill_index or supplied_time != target.fill_time:
            raise MeanReversionV2Error("missing exact ordinary fill; forward scan prohibited")
        self.pending = None
        self.actual = target.desired_weight
        self.entry_fill_index = fill_index if self.actual > 0 else None
        return Fill(
            target.target_id,
            target.asset,
            fill_index,
            supplied_time,
            price,
            target.desired_weight,
        )

    def decide(
        self,
        asset: str,
        session: datetime,
        base_fill_time: datetime,
        base_fill_index: int,
        signal: float | None,
        *,
        delayed_fill_time: datetime | None = None,
        raw_daily_return: float | None = None,
    ) -> tuple[Decision, Target | None]:
        """Decide after all earlier exact fills have updated actual state."""

        if asset not in ASSETS or base_fill_index < 0:
            raise MeanReversionV2Error("invalid decision identity")
        if self.asset is None:
            self.asset = asset
        elif asset != self.asset:
            raise MeanReversionV2Error("per-asset clock identity changed")
        decision_session = _utc(session, field="decision session")
        base_time = _utc(base_fill_time, field="base fill time")
        if base_time <= decision_session:
            raise MeanReversionV2Error("base fill must strictly follow decision information")
        if self.pending is not None:
            return (
                Decision(
                    asset,
                    decision_session,
                    self.actual,
                    True,
                    signal,
                    None,
                    raw_daily_return,
                ),
                None,
            )
        valid = signal is not None and math.isfinite(signal)
        recovery_signal = raw_daily_return if self.trial.raw else signal
        valid_recovery = recovery_signal is not None and math.isfinite(recovery_signal)
        desired: float | None = None
        if self.actual == 0.0 and valid and signal is not None and signal <= self.trial.entry:
            desired = 0.5
        elif self.actual > 0.0:
            mandatory = (
                self.entry_fill_index is not None
                and base_fill_index >= self.entry_fill_index + self.trial.maximum_holding_intervals
            )
            recovered = bool(
                valid_recovery
                and recovery_signal is not None
                and (
                    recovery_signal > self.trial.exit
                    if self.trial.raw
                    else recovery_signal >= self.trial.exit
                )
            )
            if mandatory or recovered:
                desired = 0.0
        decision = Decision(
            asset,
            decision_session,
            self.actual,
            False,
            signal,
            desired,
            recovery_signal,
        )
        if desired is None:
            return decision, None
        execution_index = base_fill_index + self.delay
        if self.delay == 0:
            execution_time = base_time
        else:
            if delayed_fill_time is None:
                raise MeanReversionV2Error("delayed target requires next eligible fill identity")
            execution_time = _utc(delayed_fill_time, field="delayed fill time")
            if execution_time <= base_time:
                raise MeanReversionV2Error("delayed fill must follow base fill")
        draft_target = Target(
            "",
            asset,
            decision_session,
            execution_index,
            execution_time,
            desired,
        )
        target = Target(
            target_identity(draft_target),
            asset,
            decision_session,
            execution_index,
            execution_time,
            desired,
        )
        self.pending = target
        return decision, target

    def quarantine(self) -> None:
        """Cancel a cash pending entry; fail if economic exposure remains live."""

        if self.actual > 0.0 or (self.pending is not None and self.pending.desired_weight == 0.0):
            raise MeanReversionV2Error("DATA_INTEGRITY_FAILURE risky quarantine")
        self.pending = None
        self.entry_fill_index = None


def exact_signal(closes: Sequence[float], index: int, trial: Trial = TRIALS[0]) -> float | None:
    """Use only the exact causal window ending at index; ignore every future close."""

    if index < 0 or index >= len(closes) or index < trial.horizon:
        return None
    start = index - trial.horizon
    if trial.volatility_lookback is not None:
        if index < trial.volatility_lookback:
            return None
        start = min(start, index - trial.volatility_lookback)
    window = closes[start : index + 1]
    if any(isinstance(value, bool) or not math.isfinite(value) or value <= 0 for value in window):
        return None
    raw = closes[index] / closes[index - trial.horizon] - 1.0
    if trial.raw:
        return raw
    lookback = trial.volatility_lookback
    if lookback is None:
        raise MeanReversionV2Error("standardized trial requires volatility lookback")
    returns = [
        closes[position] / closes[position - 1] - 1.0
        for position in range(index - lookback + 1, index + 1)
    ]
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    volatility = math.sqrt(variance)
    if not math.isfinite(volatility) or volatility <= 0:
        return None
    return raw / (volatility * math.sqrt(trial.horizon))


@dataclass(frozen=True)
class PanelReturn:
    start: int
    end: int
    value: float
    endpoints_valid: bool
    same_segment: bool
    genuine_cash: bool


def _valid_panel_return(item: PanelReturn) -> bool:
    return bool(
        item.start >= 0
        and item.end == item.start + 1
        and math.isfinite(item.value)
        and item.endpoints_valid is True
        and item.same_segment is True
        and isinstance(item.genuine_cash, bool)
        and (item.value != 0.0 or item.genuine_cash is True)
    )


def _panel_by_endpoint(rows: Sequence[PanelReturn]) -> dict[int, PanelReturn]:
    output: dict[int, PanelReturn] = {}
    for item in rows:
        if not _valid_panel_return(item):
            continue
        if item.end in output:
            raise MeanReversionV2Error("duplicate valid panel endpoint")
        output[item.end] = item
    return output


def common_panel(
    intervals: Mapping[str, Sequence[PanelReturn]],
) -> dict[str, list[float]]:
    """Return the exact seven-current-trial consecutive endpoint intersection."""

    if tuple(intervals) != TRIAL_ORDER:
        raise MeanReversionV2Error("common panel requires declared seven trials in order")
    by_trial = {name: _panel_by_endpoint(rows) for name, rows in intervals.items()}
    shared = set.intersection(*(set(items) for items in by_trial.values()))
    return {name: [by_trial[name][end].value for end in sorted(shared)] for name in TRIAL_ORDER}


def comparator_panel(
    primary: Sequence[PanelReturn], comparator: Sequence[PanelReturn]
) -> tuple[list[float], list[float]]:
    """Pair a primary and comparator on identical valid consecutive endpoints."""

    left = _panel_by_endpoint(primary)
    right = _panel_by_endpoint(comparator)
    shared = sorted(set(left) & set(right))
    return (
        [left[end].value for end in shared],
        [right[end].value for end in shared],
    )


def annualized_sharpe(values: Sequence[float]) -> float:
    """Compute the frozen sample Sharpe on one already-aligned daily panel."""

    if len(values) < 2 or any(not math.isfinite(value) or value <= -1.0 for value in values):
        raise MeanReversionV2Error("invalid Sharpe panel")
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    if variance <= 0 or not math.isfinite(variance):
        raise MeanReversionV2Error("degenerate Sharpe panel")
    return mean / math.sqrt(variance) * math.sqrt(365.0)


def compounded_equity(values: Sequence[float]) -> tuple[float, ...]:
    """Return a unit-starting wealth path for one exact aligned return panel."""

    equity = 1.0
    output = [equity]
    for value in values:
        if not math.isfinite(value) or value <= -1.0:
            raise MeanReversionV2Error("invalid return for compounded equity")
        equity *= 1.0 + value
        if not math.isfinite(equity) or equity <= 0:
            raise MeanReversionV2Error("invalid compounded equity")
        output.append(equity)
    return tuple(output)


def maximum_drawdown(equity_path: Sequence[float]) -> float:
    """Compute drawdown on every supplied wealth point, without endpoint omission."""

    if not equity_path or any(not math.isfinite(value) or value <= 0 for value in equity_path):
        raise MeanReversionV2Error("invalid drawdown path")
    peak = equity_path[0]
    maximum = 0.0
    for equity in equity_path:
        peak = max(peak, equity)
        maximum = max(maximum, 1.0 - equity / peak)
    return maximum


@dataclass(frozen=True)
class AccountingFill:
    timestamp: datetime
    prices: Mapping[str, float]
    targets: Mapping[str, float]


@dataclass(frozen=True)
class AccountedInterval:
    start: datetime
    end: datetime
    prior_postcost_equity: float
    pretrade_equity: float
    postcost_equity: float
    turnover: float
    cost: float
    net_return: float


@dataclass(frozen=True)
class AccountingResult:
    intervals: tuple[AccountedInterval, ...]
    initial_cost: float
    terminal_equity: float
    terminal_cash: bool


def accounting_equity_path(result: AccountingResult) -> tuple[float, ...]:
    """Materialize starting, initial-postcost, and every later postcost wealth point."""

    initial_postcost = 1.0 - result.initial_cost
    path = (1.0, initial_postcost, *(item.postcost_equity for item in result.intervals))
    if (
        initial_postcost <= 0
        or not math.isfinite(initial_postcost)
        or not math.isclose(path[-1], result.terminal_equity, rel_tol=1e-10, abs_tol=1e-12)
    ):
        raise MeanReversionV2Error("accounting equity path does not reconcile")
    return path


def self_financing(
    fills: Sequence[AccountingFill], *, one_way_cost_bps: float = ONE_WAY_COST_BPS
) -> AccountingResult:
    """Apply drifted risky weights, cost-before-allocation, and derived terminal cash."""

    if one_way_cost_bps not in (ONE_WAY_COST_BPS, DOUBLED_ONE_WAY_COST_BPS):
        raise MeanReversionV2Error("undeclared cost rate")
    if not fills:
        raise MeanReversionV2Error("accounting requires at least one fill")
    units = {asset: 0.0 for asset in ASSETS}
    cash = 1.0
    previous_fill: AccountingFill | None = None
    previous_postcost: float | None = None
    initial_cost = 0.0
    output: list[AccountedInterval] = []
    for position, fill in enumerate(fills):
        timestamp = _utc(fill.timestamp, field="accounting fill timestamp")
        if previous_fill is not None and timestamp <= previous_fill.timestamp:
            raise MeanReversionV2Error("nonmonotonic accounting fill")
        if set(fill.prices) != set(ASSETS) or set(fill.targets) != set(ASSETS):
            raise MeanReversionV2Error("invalid synchronized fill vector")
        if any(
            isinstance(fill.prices[asset], bool)
            or not math.isfinite(fill.prices[asset])
            or fill.prices[asset] <= 0
            for asset in ASSETS
        ):
            raise MeanReversionV2Error("invalid synchronized fill price")
        if any(
            isinstance(fill.targets[asset], bool) or fill.targets[asset] not in (0.0, 0.5)
            for asset in ASSETS
        ):
            raise MeanReversionV2Error("invalid frozen target")
        if sum(fill.targets.values()) > 1.0:
            raise MeanReversionV2Error("frozen leverage boundary exceeded")
        pretrade = cash + sum(units[asset] * fill.prices[asset] for asset in ASSETS)
        if not math.isfinite(pretrade) or pretrade <= 0:
            raise MeanReversionV2Error("DATA_INTEGRITY_FAILURE nonpositive equity")
        drifted = {asset: units[asset] * fill.prices[asset] / pretrade for asset in ASSETS}
        turnover = sum(abs(fill.targets[asset] - drifted[asset]) for asset in ASSETS)
        cost = pretrade * (one_way_cost_bps / 10_000.0) * turnover
        postcost = pretrade - cost
        if not math.isfinite(postcost) or postcost <= 0:
            raise MeanReversionV2Error("DATA_INTEGRITY_FAILURE insufficient cash")
        units = {asset: postcost * fill.targets[asset] / fill.prices[asset] for asset in ASSETS}
        cash = postcost * (1.0 - sum(fill.targets.values()))
        if position == 0:
            initial_cost = cost
        else:
            if previous_fill is None or previous_postcost is None:
                raise MeanReversionV2Error("accounting state corruption")
            output.append(
                AccountedInterval(
                    previous_fill.timestamp,
                    timestamp,
                    previous_postcost,
                    pretrade,
                    postcost,
                    turnover,
                    cost,
                    postcost / previous_postcost - 1.0,
                )
            )
        previous_fill = AccountingFill(timestamp, fill.prices, fill.targets)
        previous_postcost = postcost
    terminal_cash = bool(
        previous_fill is not None
        and all(previous_fill.targets[asset] == 0.0 for asset in ASSETS)
        and all(abs(units[asset]) <= 1e-15 for asset in ASSETS)
        and math.isclose(cash, previous_postcost or math.nan, rel_tol=1e-12, abs_tol=1e-12)
    )
    if previous_postcost is None:
        raise MeanReversionV2Error("accounting produced no terminal equity")
    return AccountingResult(tuple(output), initial_cost, previous_postcost, terminal_cash)


def reconcile_accounting(
    fills: Sequence[AccountingFill],
    result: AccountingResult,
    *,
    one_way_cost_bps: float = ONE_WAY_COST_BPS,
) -> bool:
    """Independently rebuild units, costs, returns, wealth, and terminal cash."""

    if (
        not fills
        or one_way_cost_bps not in (ONE_WAY_COST_BPS, DOUBLED_ONE_WAY_COST_BPS)
        or len(result.intervals) != len(fills) - 1
    ):
        return False

    units = {asset: 0.0 for asset in ASSETS}
    cash = 1.0
    prior_time: datetime | None = None
    prior_postcost: float | None = None
    interval_index = 0
    initial_cost: float | None = None
    last_targets: Mapping[str, float] | None = None

    for position, fill in enumerate(fills):
        try:
            timestamp = _utc(fill.timestamp, field="reconciliation fill timestamp")
        except MeanReversionV2Error:
            return False
        if (
            (prior_time is not None and timestamp <= prior_time)
            or set(fill.prices) != set(ASSETS)
            or set(fill.targets) != set(ASSETS)
            or any(
                not isinstance(fill.prices[asset], (int, float))
                or isinstance(fill.prices[asset], bool)
                or not math.isfinite(float(fill.prices[asset]))
                or float(fill.prices[asset]) <= 0
                for asset in ASSETS
            )
            or any(
                isinstance(fill.targets[asset], bool) or fill.targets[asset] not in (0.0, 0.5)
                for asset in ASSETS
            )
            or sum(fill.targets.values()) > 1.0
        ):
            return False

        pretrade = cash + sum(units[asset] * fill.prices[asset] for asset in ASSETS)
        if not math.isfinite(pretrade) or pretrade <= 0:
            return False
        drifted = {asset: units[asset] * fill.prices[asset] / pretrade for asset in ASSETS}
        turnover = sum(abs(fill.targets[asset] - drifted[asset]) for asset in ASSETS)
        cost = pretrade * one_way_cost_bps / 10_000.0 * turnover
        postcost = pretrade - cost
        if not math.isfinite(postcost) or postcost <= 0:
            return False

        if position == 0:
            initial_cost = cost
        else:
            if prior_time is None or prior_postcost is None:
                return False
            reported = result.intervals[interval_index]
            expected_scalars = {
                "prior_postcost_equity": prior_postcost,
                "pretrade_equity": pretrade,
                "postcost_equity": postcost,
                "turnover": turnover,
                "cost": cost,
                "net_return": postcost / prior_postcost - 1.0,
            }
            if reported.start != prior_time or reported.end != timestamp:
                return False
            if any(
                not math.isclose(
                    getattr(reported, name),
                    expected,
                    rel_tol=1e-10,
                    abs_tol=1e-12,
                )
                for name, expected in expected_scalars.items()
            ):
                return False
            interval_index += 1

        units = {asset: postcost * fill.targets[asset] / fill.prices[asset] for asset in ASSETS}
        cash = postcost * (1.0 - sum(fill.targets.values()))
        prior_time = timestamp
        prior_postcost = postcost
        last_targets = fill.targets

    if initial_cost is None or prior_postcost is None or last_targets is None:
        return False
    terminal_cash = bool(
        all(last_targets[asset] == 0.0 for asset in ASSETS)
        and all(abs(units[asset]) <= 1e-15 for asset in ASSETS)
        and math.isclose(cash, prior_postcost, rel_tol=1e-12, abs_tol=1e-12)
    )
    return bool(
        result.terminal_cash == terminal_cash
        and math.isclose(result.initial_cost, initial_cost, rel_tol=1e-10, abs_tol=1e-12)
        and math.isclose(
            result.terminal_equity,
            prior_postcost,
            rel_tol=1e-10,
            abs_tol=1e-12,
        )
    )


def trace_hashes(
    *,
    inputs: Sequence[object],
    decisions: Sequence[Decision],
    targets: Sequence[Target],
    fills: Sequence[Fill],
    dispositions: Sequence[Disposition],
    costs: Sequence[float],
    returns: Sequence[float],
) -> dict[str, str]:
    """Hash each declared trace family separately."""

    return {
        "input": canonical_hash(inputs),
        "decision": canonical_hash(decisions),
        "target": canonical_hash(targets),
        "fill": canonical_hash(fills),
        "disposition": canonical_hash(dispositions),
        "cost": canonical_hash(costs),
        "return": canonical_hash(returns),
    }


def reconcile_target_outcomes(
    targets: Sequence[Target],
    fills: Sequence[Fill],
    dispositions: Sequence[Disposition],
) -> bool:
    """Require exactly one valid fill/cancel/terminal disposition per target."""

    target_by_id = {target.target_id: target for target in targets}
    if len(target_by_id) != len(targets):
        return False
    for target in targets:
        try:
            decision_session = _utc(target.decision_session, field="target decision session")
            fill_time = _utc(target.fill_time, field="target fill time")
        except MeanReversionV2Error:
            return False
        if (
            target.asset not in ASSETS
            or target.fill_index < 0
            or target.desired_weight not in (0.0, 0.5)
            or fill_time <= decision_session
            or target.target_id != target_identity(target)
        ):
            return False
    fills_by_id: dict[str, list[Fill]] = {target_id: [] for target_id in target_by_id}
    dispositions_by_id: dict[str, list[Disposition]] = {target_id: [] for target_id in target_by_id}
    for fill in fills:
        if fill.target_id not in target_by_id:
            return False
        try:
            _utc(fill.timestamp, field="outcome fill timestamp")
        except MeanReversionV2Error:
            return False
        if (
            fill.asset not in ASSETS
            or not math.isfinite(fill.price)
            or fill.price <= 0
            or fill.target_weight not in (0.0, 0.5)
        ):
            return False
        fills_by_id[fill.target_id].append(fill)
    for disposition in dispositions:
        if disposition.target_id not in target_by_id:
            return False
        try:
            disposition_time = _utc(disposition.timestamp, field="disposition timestamp")
        except MeanReversionV2Error:
            return False
        if (
            disposition.kind not in {"fill", "cancel", "terminal_cancel"}
            or disposition_time < target_by_id[disposition.target_id].decision_session
        ):
            return False
        dispositions_by_id[disposition.target_id].append(disposition)
    for target_id, target in target_by_id.items():
        matched_fills = fills_by_id[target_id]
        statuses = dispositions_by_id[target_id]
        if len(statuses) != 1 or statuses[0].kind not in {
            "fill",
            "cancel",
            "terminal_cancel",
        }:
            return False
        if statuses[0].kind == "fill":
            if len(matched_fills) != 1:
                return False
            fill = matched_fills[0]
            if (
                fill.asset != target.asset
                or fill.fill_index != target.fill_index
                or fill.timestamp != target.fill_time
                or fill.target_weight != target.desired_weight
                or statuses[0].timestamp != fill.timestamp
            ):
                return False
        elif matched_fills:
            return False
    return True


def reconcile_decision_targets(decisions: Sequence[Decision], targets: Sequence[Target]) -> bool:
    """Require every economic target to arise from exactly one valid decision."""

    expected: dict[tuple[str, datetime], float] = {}
    seen: set[tuple[str, datetime]] = set()
    for decision in decisions:
        try:
            session = _utc(decision.session, field="decision trace session")
        except MeanReversionV2Error:
            return False
        key = (decision.asset, session)
        if (
            key in seen
            or decision.asset not in ASSETS
            or decision.actual_before not in (0.0, 0.5)
            or (decision.signal is not None and not math.isfinite(decision.signal))
            or (
                decision.recovery_signal is not None and not math.isfinite(decision.recovery_signal)
            )
            or (decision.target is not None and decision.target not in (0.0, 0.5))
            or (decision.pending and decision.target is not None)
            or (decision.target == 0.5 and decision.actual_before != 0.0)
            or (decision.target == 0.0 and decision.actual_before != 0.5)
        ):
            return False
        seen.add(key)
        if decision.target is not None:
            expected[key] = decision.target
    if len(expected) != len(targets):
        return False
    observed: set[tuple[str, datetime]] = set()
    for target in targets:
        key = (target.asset, target.decision_session)
        if key in observed or expected.get(key) != target.desired_weight:
            return False
        observed.add(key)
    return observed == set(expected)


def reconcile_trace(
    *,
    inputs: Sequence[object],
    decisions: Sequence[Decision],
    targets: Sequence[Target],
    fills: Sequence[Fill],
    dispositions: Sequence[Disposition],
    costs: Sequence[float],
    returns: Sequence[float],
    expected_hashes: Mapping[str, str],
) -> bool:
    """Validate hashes, terminal target outcomes, and finite cost/return evidence."""

    if any(not math.isfinite(value) or value < 0 for value in costs):
        return False
    if any(not math.isfinite(value) for value in returns):
        return False
    return bool(
        trace_hashes(
            inputs=inputs,
            decisions=decisions,
            targets=targets,
            fills=fills,
            dispositions=dispositions,
            costs=costs,
            returns=returns,
        )
        == dict(expected_hashes)
        and reconcile_decision_targets(decisions, targets)
        and reconcile_target_outcomes(targets, fills, dispositions)
    )


def require_predeclared_terminal_fill(
    eligible_fill_times: Sequence[datetime], declared: datetime, end: datetime
) -> datetime:
    """Require the predeclared last eligible fill strictly inside a half-open boundary."""

    boundary = _utc(end, field="terminal boundary")
    terminal = _utc(declared, field="declared terminal fill")
    normalized = [_utc(value, field="eligible fill") for value in eligible_fill_times]
    if (
        not normalized
        or any(right <= left for left, right in itertools.pairwise(normalized))
        or terminal >= boundary
        or normalized[-1] != terminal
    ):
        raise MeanReversionV2Error("missing exact predeclared terminal fill")
    return terminal


def regime_labels(
    closes: Sequence[float | None], segments: Sequence[int | None]
) -> list[str | None]:
    """Retain finalized labels while resetting only future history at a gap."""

    if len(closes) != len(segments):
        raise MeanReversionV2Error("regime inputs misaligned")
    labels: list[str | None] = []
    current_segment: int | None = None
    history: list[float] = []
    volatility_history: list[float] = []
    for close, segment in zip(closes, segments, strict=True):
        if segment != current_segment:
            current_segment = segment
            history = []
            volatility_history = []
        if segment is None or close is None or not math.isfinite(close) or close <= 0:
            labels.append(None)
            continue
        history.append(close)
        if len(history) < 61:
            labels.append(None)
            continue
        returns = [
            history[position] / history[position - 1] - 1.0
            for position in range(len(history) - 60, len(history))
        ]
        mean = sum(returns) / 60
        volatility = math.sqrt(sum((value - mean) ** 2 for value in returns) / 59)
        label: str | None = None
        if len(history) >= 121 and len(volatility_history) >= 120:
            trend = "up" if history[-1] / history[-121] - 1.0 > 0 else "down"
            ordered = sorted(volatility_history)
            count = len(ordered)
            median = (ordered[(count - 1) // 2] + ordered[count // 2]) / 2.0
            level = "high" if volatility > median else "low"
            label = f"{trend}_{level}"
        labels.append(label)
        volatility_history.append(volatility)
    return labels


def stationary_bootstrap(
    values: Sequence[float], *, resamples: int = 2000, block_length: int = 20
) -> tuple[float, float]:
    """Politis--Romano circular bootstrap with frozen PCG64 and linear percentiles."""

    if (
        len(values) < 2
        or resamples < 1
        or block_length != 20
        or any(not math.isfinite(value) for value in values)
    ):
        raise MeanReversionV2Error("invalid frozen bootstrap input")
    numpy: Any = importlib.import_module("numpy")
    rng = numpy.random.Generator(numpy.random.PCG64(BOOTSTRAP_SEED))
    restart = 1.0 / block_length
    count = len(values)
    samples: list[float] = []
    for _ in range(resamples):
        index = int(rng.integers(count))
        sample: list[float] = []
        for _ in range(count):
            sample.append(values[index])
            index = int(rng.integers(count)) if rng.random() < restart else (index + 1) % count
        samples.append(sum(sample) / count)
    lower, upper = numpy.percentile(numpy.asarray(samples), [2.5, 97.5], method="linear")
    if not math.isfinite(float(lower)) or not math.isfinite(float(upper)):
        raise MeanReversionV2Error("nonfinite bootstrap")
    return float(lower), float(upper)


def multiplicity_counts(completed_v2_runs: int = 1, *, holdout: bool = False) -> dict[str, int]:
    """Return the frozen append-only first/repaired/holdout registry counts."""

    if completed_v2_runs not in (1, 2, 3):
        raise MeanReversionV2Error("only first run and two authorized repairs")
    holdout_slots = 7 if holdout else 0
    return {
        "N": 42 + 7 * completed_v2_runs + holdout_slots,
        "observed": 28 + 7 * completed_v2_runs + holdout_slots,
        "unimputed": 14,
    }


def _nonannualized_sharpe(values: Sequence[float]) -> float:
    if len(values) < 2 or any(not math.isfinite(value) for value in values):
        return math.nan
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    if variance == 0:
        return math.inf if mean > 0 else -math.inf
    return mean / math.sqrt(variance)


def dsr_probability(primary: Sequence[float], registry: Sequence[float], *, N: int = 49) -> float:
    """Compute the frozen DSR and return zero for every declared degeneracy."""

    if (
        N not in (49, 56, 63, 70)
        or len(registry) != N - 14
        or len(primary) < 30
        or any(not math.isfinite(value) for value in [*primary, *registry])
    ):
        return 0.0
    mean = sum(primary) / len(primary)
    denominator = sum((value - mean) ** 2 for value in primary)
    if denominator <= 0:
        return 0.0
    autocorrelations = [
        sum(
            (primary[position] - mean) * (primary[position - lag] - mean)
            for position in range(lag, len(primary))
        )
        / denominator
        for lag in range(1, 29)
    ]
    vif = max(
        1.0,
        1.0 + 2.0 * sum((1.0 - lag / 29.0) * rho for lag, rho in enumerate(autocorrelations, 1)),
    )
    effective = len(primary) / vif
    registry_mean = sum(registry) / len(registry)
    dispersion = math.sqrt(
        sum((value - registry_mean) ** 2 for value in registry) / (len(registry) - 1)
    )
    observed = _nonannualized_sharpe(primary)
    if not math.isfinite(vif) or effective < 30 or dispersion <= 0 or not math.isfinite(observed):
        return 0.0
    gamma = 0.5772156649015329
    sr0 = dispersion * (
        (1.0 - gamma) * NormalDist().inv_cdf(1.0 - 1.0 / N)
        + gamma * NormalDist().inv_cdf(1.0 - 1.0 / (N * math.e))
    )
    count = len(primary)
    sample_std = math.sqrt(denominator / (count - 1))
    centered_standard = [(value - mean) / sample_std for value in primary]
    skew = count / ((count - 1) * (count - 2)) * sum(value**3 for value in centered_standard)
    excess_kurtosis = count * (count + 1) / ((count - 1) * (count - 2) * (count - 3)) * sum(
        value**4 for value in centered_standard
    ) - 3 * (count - 1) ** 2 / ((count - 2) * (count - 3))
    nonexcess_kurtosis = excess_kurtosis + 3.0
    radicand = 1.0 - skew * observed + ((nonexcess_kurtosis - 1.0) / 4.0) * observed**2
    if radicand <= 0 or not math.isfinite(radicand):
        return 0.0
    z_score = (observed - sr0) * math.sqrt(effective - 1.0) / math.sqrt(radicand)
    return NormalDist().cdf(z_score) if math.isfinite(z_score) else 0.0


def _array_split(values: Sequence[int], parts: int) -> list[Sequence[int]]:
    base, remainder = divmod(len(values), parts)
    output: list[Sequence[int]] = []
    start = 0
    for index in range(parts):
        end = start + base + int(index < remainder)
        output.append(values[start:end])
        start = end
    return output


def pbo(matrix: Sequence[Sequence[float]]) -> float:
    """Run exact seven-trial, eight-block, 70-split CSCV with rankable infinities."""

    if (
        len(matrix) != 7
        or len({len(row) for row in matrix}) != 1
        or len(matrix[0]) < 8
        or any(not math.isfinite(value) for row in matrix for value in row)
    ):
        return 1.0
    blocks = [list(block) for block in _array_split(list(range(len(matrix[0]))), 8)]
    if any(not block for block in blocks):
        return 1.0
    events = 0
    split_count = 0
    for training_blocks in itertools.combinations(range(8), 4):
        test_blocks = [index for index in range(8) if index not in training_blocks]
        training_scores = [
            _nonannualized_sharpe(
                [row[position] for block in training_blocks for position in blocks[block]]
            )
            for row in matrix
        ]
        if any(math.isnan(score) for score in training_scores):
            return 1.0
        chosen = max(range(7), key=lambda index: training_scores[index])
        test_scores = [
            _nonannualized_sharpe(
                [row[position] for block in test_blocks for position in blocks[block]]
            )
            for row in matrix
        ]
        if any(math.isnan(score) for score in test_scores):
            return 1.0
        selected = test_scores[chosen]
        less = sum(score < selected for score in test_scores)
        less_or_equal = sum(score <= selected for score in test_scores)
        rank = (1.0 + less + less_or_equal) / 2.0
        relative_rank = rank / 8.0
        logit = math.log(relative_rank / (1.0 - relative_rank))
        if not math.isfinite(logit):
            return 1.0
        events += int(logit <= 0)
        split_count += 1
    return events / 70.0 if split_count == 70 else 1.0


def aggregate_gates(metrics: Mapping[str, object]) -> dict[str, bool]:
    """Evaluate the exact v1 19-gate map; missing or extra evidence fails all gates."""

    if set(metrics) != set(GATE_NAMES):
        return {name: False for name in GATE_NAMES}
    numeric_rules: Mapping[str, tuple[str, float]] = {
        "aggregate_net_return_gt": (">", 0.0),
        "annualized_sharpe_gte": (">=", 0.75),
        "positive_folds_minimum": (">=", 3.0),
        "fold_count": (">=", 4.0),
        "maximum_drawdown_lte": ("<=", 0.2),
        "doubled_cost_aggregate_net_return_gt": (">", 0.0),
        "additional_delay_aggregate_net_return_gt": (">", 0.0),
        "positive_parameter_neighbors_minimum": (">=", 3.0),
        "parameter_neighbor_count": (">=", 4.0),
        "asset_standalone_net_return_each_gt": (">", 0.0),
        "completed_entries_total_minimum": (">=", 24.0),
        "completed_entries_each_asset_minimum": (">=", 10.0),
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": (">", 0.0),
        "deflated_sharpe_probability_gte": (">=", 0.95),
        "probability_of_backtest_overfitting_lte": ("<=", 0.2),
    }
    exact_counts = {"fold_count": 4, "parameter_neighbor_count": 4}
    minimum_counts = {
        "positive_folds_minimum": 3,
        "positive_parameter_neighbors_minimum": 3,
        "completed_entries_total_minimum": 24,
        "completed_entries_each_asset_minimum": 10,
    }
    output: dict[str, bool] = {}
    for name in GATE_NAMES:
        value = metrics[name]
        if name in {"regime_gate", "exceptional_trade_gate"}:
            output[name] = value is True or value == "pass"
            continue
        if name in {"baseline_superiority", "no_material_leakage"}:
            output[name] = value is True
            continue
        if name in exact_counts:
            output[name] = type(value) is int and value == exact_counts[name]
            continue
        if name in minimum_counts:
            output[name] = type(value) is int and value >= minimum_counts[name]
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            output[name] = False
            continue
        operator, threshold = numeric_rules[name]
        number = float(value)
        output[name] = {
            ">": number > threshold,
            ">=": number >= threshold,
            "<=": number <= threshold,
        }[operator]
    return output


def derive_integrity_evidence(
    *,
    input_identity_pass: bool,
    trace_reconciliation_pass: bool,
    terminal_cash: bool,
    strict_prefix_pass: bool,
    holdout_closed: bool,
    gate_names: Sequence[str],
) -> bool:
    """Derive rather than hard-code leakage, trace, terminal, and holdout integrity."""

    return bool(
        input_identity_pass
        and trace_reconciliation_pass
        and terminal_cash
        and strict_prefix_pass
        and holdout_closed
        and tuple(gate_names) == GATE_NAMES
    )
