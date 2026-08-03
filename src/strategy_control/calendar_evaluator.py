"""Executable frozen evaluator for BTC/ETH intraday calendar seasonality.

The module is development-only.  It resolves the reused allowlist before importing
Parquet support, and every caller-supplied record is sliced before the evaluation
boundary.  It has no exchange, credential, order, network, GPU, or holdout path.
"""

from __future__ import annotations

import hashlib
import importlib
import itertools
import json
import math
import statistics
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from strategy_control.calendar_pipeline import (
    DEVELOPMENT_END,
    DEVELOPMENT_START,
    FOLDS,
    OBSERVATION_START,
    MinuteRecord,
    development_partitions,
)
from strategy_control.calendar_seasonality import (
    ASSETS,
    BASE_COST,
    TRIALS,
    CalendarIntegrityError,
    CellEstimate,
    Counters,
    H,
    JointVector,
    Observation,
    Portfolio,
    TrialSpec,
    annualized_sharpe,
    bucket_at,
    cscv_pbo,
    deflated_sharpe_probability,
    estimate_cell,
    holm_active,
    joint_targets,
    maximum_drawdown,
    monday_refresh,
    rebalance,
    schedule_for_interval,
    stationary_bootstrap,
)

EXPERIMENT_ID = "btc-eth-intraday-calendar-seasonality-v1"
EXPECTED_COLUMNS = (
    "event_timestamp",
    "available_timestamp",
    "open",
    "high",
    "low",
    "close",
)
DATASET_ROOT_RELATIVE = Path("data/real/historical-v2-pathc-20260723T175155Z")
TRIAL_ORDER = tuple(trial.name for trial in TRIALS)
PRIMARY_NAME = TRIAL_ORDER[0]
SIMPLE_NAME = TRIAL_ORDER[1]
NEIGHBORS = TRIAL_ORDER[2:]
PRIOR_RESULT_HASHES = {
    "btc-eth-vol-targeted-trend-v1": (
        "d21faab707543a5123aba12fd2849c204eaaf93513bc528574661ce72c79e703"
    ),
    "btc-eth-long-only-mean-reversion-v1": (
        "0a5a783d886dbabb4807619557429d4b3abcc00ee6c9a968f6f75501d6de3e23"
    ),
    "btc-eth-relative-value-rotation-v1": (
        "e03d5fc44598a4c3e4b0f34b87b3f0a37427af44248b45ee78dd4154e45a3d4e"
    ),
}
PRIOR_TRIAL_ORDERS = {
    "btc-eth-vol-targeted-trend-v1": (
        "primary_combined",
        "donchian_only",
        "time_series_momentum_only",
        "shorter_horizons",
        "longer_horizons",
        "lower_volatility_target",
        "higher_volatility_target",
    ),
    "btc-eth-long-only-mean-reversion-v1": (
        "primary_standardized_shock",
        "raw_three_session_drawdown_baseline",
        "shorter_two_session_shock",
        "longer_five_session_shock",
        "shallower_entry",
        "deeper_entry",
        "slower_volatility_estimator",
    ),
    "btc-eth-relative-value-rotation-v1": (
        "primary_risk_adjusted_20_60_120",
        "raw_60_session_relative_strength_rotation",
        "short_10_30_60_horizons",
        "long_60_120_180_horizons",
        "raw_unadjusted_20_60_120",
        "wide_0_50_rotation_gap",
        "always_in_higher_score_no_cash_filter",
    ),
}


class CalendarEvaluationError(RuntimeError):
    """Raised when the frozen evaluator cannot preserve a required invariant."""


def _utc(value: Any) -> datetime:
    converted = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if not isinstance(converted, datetime):
        raise CalendarEvaluationError("source timestamp is not datetime")
    if converted.tzinfo is None:
        converted = converted.replace(tzinfo=UTC)
    return converted.astimezone(UTC)


@dataclass(frozen=True)
class CalendarMarket:
    records: Mapping[str, Mapping[datetime, MinuteRecord]]
    joint: Mapping[datetime, JointVector]
    joint_available: Mapping[datetime, datetime]
    source_partition_count: int
    holdout_values_read: bool = False
    max_open_timestamp: datetime | None = None
    max_available_timestamp: datetime | None = None

    def is_strict_prefix(self, end: datetime) -> bool:
        end = _utc(end)
        return (self.max_open_timestamp is None or self.max_open_timestamp < end) and (
            self.max_available_timestamp is None or self.max_available_timestamp < end
        )

    def prefix(self, end: datetime) -> CalendarMarket:
        end = _utc(end)
        records = {
            symbol: {
                stamp: row for stamp, row in rows.items() if stamp < end and row.available_at < end
            }
            for symbol, rows in self.records.items()
        }
        return build_market(
            tuple(row for rows in records.values() for row in rows.values()),
            source_partition_count=self.source_partition_count,
        )


def build_market(
    records: Sequence[MinuteRecord], *, source_partition_count: int = 36
) -> CalendarMarket:
    """Validate decoded records once and create fail-closed causal indexes."""

    by_asset: dict[str, dict[datetime, MinuteRecord]] = {asset: {} for asset in ASSETS}
    for row in records:
        if not row.valid():
            raise CalendarEvaluationError("invalid source minute record")
        if not OBSERVATION_START <= row.open_timestamp < DEVELOPMENT_END:
            raise CalendarEvaluationError("record outside frozen development observation window")
        target = by_asset[row.symbol]
        if row.open_timestamp in target:
            raise CalendarEvaluationError("duplicate source minute record")
        target[row.open_timestamp] = row
    joint: dict[datetime, JointVector] = {}
    available: dict[datetime, datetime] = {}
    for stamp in sorted(set(by_asset[ASSETS[0]]) & set(by_asset[ASSETS[1]])):
        btc, eth = (by_asset[asset][stamp] for asset in ASSETS)
        joint[stamp] = JointVector(stamp, btc.open, eth.open)
        available[stamp] = max(btc.available_at, eth.available_at)
    max_open = max(
        (stamp for rows in by_asset.values() for stamp in rows),
        default=None,
    )
    max_available = max(
        (row.available_at for rows in by_asset.values() for row in rows.values()),
        default=None,
    )
    return CalendarMarket(
        by_asset,
        joint,
        available,
        source_partition_count,
        False,
        max_open,
        max_available,
    )


def load_development_market(
    source_repository: Path, data_contract: Mapping[str, Any]
) -> CalendarMarket:
    """Read exactly 36 allowlisted 2024--25 partitions, never a 2026 footer."""

    partitions = development_partitions(data_contract)
    parquet = importlib.import_module("pyarrow.parquet")
    records: list[MinuteRecord] = []
    dataset_root = source_repository / DATASET_ROOT_RELATIVE
    for partition in partitions:
        if "year=2026" in partition.relative_path:
            raise CalendarEvaluationError("holdout path reached development reader")
        path = dataset_root / partition.relative_path
        table = parquet.ParquetFile(path).read(columns=EXPECTED_COLUMNS)
        if tuple(str(name) for name in table.column_names) != EXPECTED_COLUMNS:
            raise CalendarEvaluationError("development column order mismatch")
        for raw in table.to_pylist():
            event = _utc(raw["event_timestamp"])
            records.append(
                MinuteRecord(
                    symbol=partition.symbol,
                    open_timestamp=event - timedelta(minutes=1),
                    event_timestamp=event,
                    available_at=_utc(raw["available_timestamp"]),
                    open=float(raw["open"]),
                    high=float(raw["high"]),
                    low=float(raw["low"]),
                    close=float(raw["close"]),
                )
            )
    return build_market(records, source_partition_count=len(partitions))


@dataclass(frozen=True)
class EstimatorInterval:
    start: datetime
    endpoint: datetime
    available_at: datetime
    returns: tuple[float, float]


@dataclass(frozen=True)
class FrozenSchedule:
    refresh: datetime
    trial: str
    active: Mapping[str, tuple[bool, ...]]

    def target(self, hour: datetime) -> tuple[float, float, float]:
        split = len(self.active[ASSETS[0]]) == 48
        cell = bucket_at(hour, split=split)
        return joint_targets(self.active[ASSETS[0]][cell], self.active[ASSETS[1]][cell])


def estimator_intervals(market: CalendarMarket) -> tuple[EstimatorInterval, ...]:
    rows: list[EstimatorInterval] = []
    for start, vector in market.joint.items():
        if start.minute or start.second or start.microsecond:
            continue
        endpoint = start + timedelta(hours=1)
        later = market.joint.get(endpoint)
        if later is None:
            continue
        values = (later.btc / vector.btc - 1.0, later.eth / vector.eth - 1.0)
        if any(not math.isfinite(value) for value in values):
            continue
        rows.append(
            EstimatorInterval(
                start,
                endpoint,
                max(market.joint_available[start], market.joint_available[endpoint]),
                values,
            )
        )
    return tuple(sorted(rows, key=lambda row: row.start))


def _minimum(trial: TrialSpec, cell: int) -> int:
    minimum = trial.observation_minimum
    if isinstance(minimum, int):
        return minimum
    return minimum[1] if cell % 2 else minimum[0]


def fit_schedules(
    market: CalendarMarket,
    trial: TrialSpec,
    start: datetime,
    end: datetime,
    *,
    student_t_cdf: Callable[[float, int], float] | None = None,
) -> dict[datetime, FrozenSchedule]:
    """Fit every causal Monday schedule needed by one half-open evaluation."""

    start, end = _utc(start), _utc(end)
    if not OBSERVATION_START <= start < end <= DEVELOPMENT_END:
        raise CalendarEvaluationError("schedule boundary violation")
    if student_t_cdf is None:
        scipy_stats = importlib.import_module("scipy.stats")

        def scipy_student_t_cdf(value: float, degrees: int) -> float:
            return float(scipy_stats.t.cdf(value, degrees))

        student_t_cdf = scipy_student_t_cdf
    intervals = estimator_intervals(market)
    refreshes: set[datetime] = set()
    cursor = start.replace(minute=0, second=0, microsecond=0)
    while cursor < end:
        refreshes.add(schedule_for_interval(cursor))
        cursor += timedelta(hours=1)
    schedules: dict[datetime, FrozenSchedule] = {}
    for refresh in sorted(refreshes):
        lower = refresh - timedelta(weeks=trial.lookback_weeks)
        eligible = [
            row for row in intervals if lower <= row.start < refresh and row.available_at < refresh
        ]
        active_by_asset: dict[str, tuple[bool, ...]] = {}
        if trial.holm_alpha is None:
            for asset_index, asset in enumerate(ASSETS):
                flags: list[bool] = []
                for cell in range(24):
                    cell_rows = [
                        row for row in eligible if bucket_at(row.start, split=False) == cell
                    ]
                    values = [row.returns[asset_index] for row in cell_rows]
                    weeks = {monday_refresh(row.start) for row in cell_rows}
                    flags.append(
                        len(values) >= _minimum(trial, cell)
                        and len(weeks) >= trial.week_minimum
                        and all(math.isfinite(value) for value in values)
                        and statistics.mean(values) > H
                    )
                active_by_asset[asset] = tuple(flags)
        else:
            estimates: list[CellEstimate] = []
            for asset_index, _asset in enumerate(ASSETS):
                for cell in range(48):
                    observations = tuple(
                        Observation(
                            row.returns[asset_index], row.endpoint, monday_refresh(row.start)
                        )
                        for row in eligible
                        if bucket_at(row.start) == cell
                    )
                    estimates.append(
                        estimate_cell(
                            observations,
                            trim_fraction=trial.trim_fraction,
                            minimum=_minimum(trial, cell),
                            minimum_weeks=trial.week_minimum,
                            cdf=student_t_cdf,
                        )
                    )
            holm_flags = holm_active(estimates, trial.holm_alpha)
            active_by_asset = {
                asset: tuple(holm_flags[index * 48 : (index + 1) * 48])
                for index, asset in enumerate(ASSETS)
            }
        schedules[refresh] = FrozenSchedule(refresh, trial.name, active_by_asset)
    return schedules


def regime_labels(market: CalendarMarket, end: datetime) -> dict[datetime, str | None]:
    """Build the frozen prior-completed-day BTC regime with hard gap resets."""

    end = _utc(end)
    labels: dict[datetime, str | None] = {}
    closes: list[float] = []
    returns: list[float] = []
    prior_volatility: list[float] = []
    day = OBSERVATION_START
    minute = timedelta(minutes=1)
    while day < end:
        stamps = tuple(day + index * minute for index in range(1440))
        complete = all(stamp in market.joint for stamp in stamps)
        next_day = day + timedelta(days=1)
        if not complete:
            closes.clear()
            returns.clear()
            prior_volatility.clear()
            labels[next_day] = None
            day = next_day
            continue
        close = market.records[ASSETS[0]][stamps[-1]].close
        if closes:
            returns.append(close / closes[-1] - 1.0)
        closes.append(close)
        volatility = statistics.stdev(returns[-60:]) if len(returns) >= 60 else None
        label: str | None = None
        if len(closes) >= 121 and volatility is not None and len(prior_volatility) >= 120:
            direction = "up" if math.log(close / closes[-121]) > 0.0 else "down"
            median = statistics.median(prior_volatility)
            label = f"{direction}_{'high' if volatility > median else 'low'}"
        labels[next_day] = label
        if volatility is not None:
            prior_volatility.append(volatility)
        day = next_day
    return labels


@dataclass(frozen=True)
class PnlEvent:
    timestamp: datetime
    pnl: float
    cell: int
    regime: str | None


@dataclass(frozen=True)
class RunResult:
    name: str
    net_return: float
    annualized_sharpe: float | None
    maximum_drawdown: float
    daily_returns: Mapping[datetime, float]
    pnl_events: tuple[PnlEvent, ...]
    asset_net: tuple[float, float]
    cell_net: tuple[float, ...]
    counters: Counters
    eligible_hours: int
    exposed_hours: tuple[int, int]
    asset_refresh_weeks: tuple[frozenset[datetime], frozenset[datetime]]
    regime_pnl: Mapping[str, float]
    regime_exposed_hours: Mapping[str, int]
    regime_entries: Mapping[str, int]
    regime_refresh_weeks: Mapping[str, frozenset[datetime]]
    quarantine_liquidations: int
    final_cash: bool

    def summary(self) -> dict[str, Any]:
        return {
            "net_return": self.net_return,
            "annualized_sharpe": self.annualized_sharpe,
            "maximum_drawdown": self.maximum_drawdown,
            "daily_observations": len(self.daily_returns),
            "eligible_hours": self.eligible_hours,
            "exposed_hours": dict(zip(ASSETS, self.exposed_hours, strict=True)),
            "entries": self.counters.entries,
            "entries_by_asset": dict(zip(ASSETS, self.counters.asset_entries, strict=True)),
            "completed_episodes": self.counters.episodes,
            "target_changes": self.counters.target_changes,
            "refresh_weeks": len(self.counters.refresh_weeks),
            "asset_net": dict(zip(ASSETS, self.asset_net, strict=True)),
            "quarantine_liquidations": self.quarantine_liquidations,
            "final_cash": self.final_cash,
        }


def _target_for(
    schedules: Mapping[datetime, FrozenSchedule],
    hour: datetime,
    *,
    terminal_hour: datetime,
    asset_only: str | None,
    omitted_bucket: int | None,
) -> tuple[float, float, float]:
    if hour >= terminal_hour or (omitted_bucket is not None and bucket_at(hour) == omitted_bucket):
        return (0.0, 0.0, 1.0)
    schedule = schedules.get(schedule_for_interval(hour))
    if schedule is None:
        return (0.0, 0.0, 1.0)
    target = schedule.target(hour)
    if asset_only is None:
        return target
    if asset_only not in ASSETS:
        raise CalendarEvaluationError("invalid asset-only sensitivity")
    values = list(target)
    prohibited = 1 if asset_only == ASSETS[0] else 0
    values[2] += values[prohibited]
    values[prohibited] = 0.0
    return values[0], values[1], values[2]


def _resume_hour(market: CalendarMarket, anchor: datetime, end: datetime) -> datetime | None:
    consecutive = 0
    cursor = anchor + timedelta(minutes=1)
    sixtieth: datetime | None = None
    while cursor < end:
        if cursor in market.joint:
            consecutive += 1
            if consecutive == 60:
                sixtieth = cursor
                break
        else:
            consecutive = 0
        cursor += timedelta(minutes=1)
    if sixtieth is None:
        return None
    boundary = sixtieth.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return boundary if boundary < end else None


def _first_joint_after(
    market: CalendarMarket, moment: datetime, end: datetime
) -> JointVector | None:
    cursor = moment + timedelta(minutes=1)
    while cursor < end:
        vector = market.joint.get(cursor)
        if vector is not None:
            return vector
        cursor += timedelta(minutes=1)
    return None


def _execution_vector(
    market: CalendarMarket, hour: datetime, delay_minutes: int
) -> tuple[JointVector | None, datetime | None]:
    """Return a valid fill or the exact first causal trigger timestamp."""

    if delay_minutes == 0:
        if hour not in market.joint:
            return None, hour
        if hour > DEVELOPMENT_START and hour - timedelta(minutes=1) not in market.joint:
            return None, hour - timedelta(minutes=1)
        return market.joint[hour], None
    if delay_minutes != 5:
        raise CalendarEvaluationError("only frozen fifth-minute delay is allowed")
    for offset in range(1, 6):
        stamp = hour + timedelta(minutes=offset)
        if stamp not in market.joint:
            return None, stamp
    return market.joint[hour + timedelta(minutes=5)], None


def _mark_wealth(portfolio: Portfolio, vector: JointVector) -> float:
    if portfolio.mark is None or portfolio.weights[0] == portfolio.weights[1] == 0.0:
        return portfolio.wealth
    btc_return = vector.btc / portfolio.mark.btc
    eth_return = vector.eth / portfolio.mark.eth
    factor = (
        portfolio.weights[2] + portfolio.weights[0] * btc_return + portfolio.weights[1] * eth_return
    )
    wealth = portfolio.wealth * factor
    if not math.isfinite(wealth) or wealth <= 0.0:
        raise CalendarEvaluationError("invalid marked wealth")
    return wealth


def _daily_returns(
    market: CalendarMarket,
    snapshots: Sequence[tuple[datetime, Portfolio]],
    start: datetime,
    end: datetime,
) -> dict[datetime, float]:
    equities: dict[datetime, float] = {start: 1.0}
    cursor = start + timedelta(days=1)
    index = 0
    state = snapshots[0][1]
    while cursor <= end:
        while index + 1 < len(snapshots) and snapshots[index + 1][0] <= cursor:
            index += 1
            state = snapshots[index][1]
        if state.weights[0] > 0.0 or state.weights[1] > 0.0:
            vector = market.joint.get(cursor)
            if vector is None:
                raise CalendarEvaluationError("unpriced exposed daily boundary")
            equities[cursor] = _mark_wealth(state, vector)
        else:
            equities[cursor] = state.wealth
        cursor += timedelta(days=1)
    result: dict[datetime, float] = {}
    ordered = sorted(equities)
    for prior, current in itertools.pairwise(ordered):
        value = equities[current] / equities[prior] - 1.0
        if not math.isfinite(value) or value <= -1.0:
            raise CalendarEvaluationError("invalid daily return")
        result[current] = value
    return result


def execute_trial(
    market: CalendarMarket,
    trial: TrialSpec,
    start: datetime,
    end: datetime,
    *,
    schedules: Mapping[datetime, FrozenSchedule] | None = None,
    cost_rate: float = BASE_COST,
    delay_minutes: int = 0,
    asset_only: str | None = None,
    omitted_bucket: int | None = None,
    labels: Mapping[datetime, str | None] | None = None,
) -> RunResult:
    """Execute one immutable trial path from cash over one half-open period."""

    start, end = _utc(start), _utc(end)
    if market.holdout_values_read or not DEVELOPMENT_START <= start < end <= DEVELOPMENT_END:
        raise CalendarEvaluationError("development execution boundary violation")
    if not market.is_strict_prefix(end):
        market = market.prefix(end)
    if schedules is None:
        schedules = fit_schedules(market, trial, start, end)
    if labels is None:
        labels = regime_labels(market, end)
    terminal_hour = end - timedelta(hours=1)
    portfolio = Portfolio()
    snapshots: list[tuple[datetime, Portfolio]] = [(start, portfolio)]
    events: list[PnlEvent] = []
    counters = Counters()
    exposed_hours = [0, 0]
    asset_weeks: list[set[datetime]] = [set(), set()]
    regime_pnl: dict[str, float] = {}
    regime_hours: dict[str, int] = {}
    regime_entries: dict[str, int] = {}
    regime_weeks: dict[str, set[datetime]] = {}
    open_entry_regime: str | None = None
    eligible_hours = 0
    liquidation_count = 0
    held_cell: int | None = None
    held_regime: str | None = None
    quarantined = False
    resume_at: datetime | None = None
    hour = start
    while hour < end:
        if quarantined:
            if resume_at is None or hour < resume_at:
                hour += timedelta(hours=1)
                continue
            quarantined = False
            resume_at = None
        target = _target_for(
            schedules,
            hour,
            terminal_hour=terminal_hour,
            asset_only=asset_only,
            omitted_bucket=omitted_bucket,
        )
        fill, trigger = _execution_vector(market, hour, delay_minutes)
        if fill is None:
            if trigger is None:
                raise CalendarEvaluationError("missing execution trigger")
            exposed = portfolio.weights[0] > 0.0 or portfolio.weights[1] > 0.0
            anchor = trigger
            if exposed:
                liquidation = _first_joint_after(market, trigger, end)
                if liquidation is None:
                    raise CalendarEvaluationError("exposed liquidation unavailable before end")
                old = portfolio
                cell = held_cell if held_cell is not None else bucket_at(hour)
                portfolio = rebalance(
                    portfolio, (0.0, 0.0, 1.0), liquidation, cost_rate=cost_rate, cell=cell
                )
                pnl = portfolio.wealth - old.wealth
                events.append(PnlEvent(liquidation.timestamp, pnl, cell, held_regime))
                if held_regime is not None:
                    regime_pnl[held_regime] = regime_pnl.get(held_regime, 0.0) + pnl
                counters = counters.completed_fill(
                    old.weights, portfolio.weights, liquidation.timestamp
                )
                if open_entry_regime is not None:
                    regime_entries[open_entry_regime] = regime_entries.get(open_entry_regime, 0) + 1
                    open_entry_regime = None
                snapshots.append((liquidation.timestamp, portfolio))
                liquidation_count += 1
                anchor = liquidation.timestamp
                held_cell = None
                held_regime = None
            quarantined = True
            resume_at = _resume_hour(market, anchor, end)
            hour += timedelta(hours=1)
            continue
        old = portfolio
        current_cell = bucket_at(hour)
        current_regime = labels.get(hour.replace(hour=0, minute=0, second=0, microsecond=0))
        attribution_cell = held_cell if held_cell is not None else current_cell
        attribution_regime = held_regime if held_cell is not None else current_regime
        portfolio = rebalance(
            portfolio,
            target,
            fill,
            cost_rate=cost_rate,
            cell=attribution_cell,
        )
        pnl = portfolio.wealth - old.wealth
        events.append(PnlEvent(fill.timestamp, pnl, attribution_cell, attribution_regime))
        if attribution_regime is not None:
            regime_pnl[attribution_regime] = regime_pnl.get(attribution_regime, 0.0) + pnl
        counters = counters.completed_fill(old.weights, target, fill.timestamp)
        entering = old.weights[0] == old.weights[1] == 0.0 and (target[0] > 0.0 or target[1] > 0.0)
        exiting = (old.weights[0] > 0.0 or old.weights[1] > 0.0) and (target[0] == target[1] == 0.0)
        if entering:
            open_entry_regime = current_regime
        if exiting and open_entry_regime is not None:
            regime_entries[open_entry_regime] = regime_entries.get(open_entry_regime, 0) + 1
            open_entry_regime = None
        eligible_hours += 1
        refresh = schedule_for_interval(hour)
        for index in range(2):
            if target[index] > 0.0:
                exposed_hours[index] += 1
                asset_weeks[index].add(refresh)
        if target[0] > 0.0 or target[1] > 0.0:
            if current_regime is not None:
                regime_hours[current_regime] = regime_hours.get(current_regime, 0) + 1
                regime_weeks.setdefault(current_regime, set()).add(refresh)
            held_cell = current_cell
            held_regime = current_regime
        else:
            held_cell = None
            held_regime = None
        snapshots.append((fill.timestamp, portfolio))
        hour += timedelta(hours=1)
    if portfolio.weights[0] > 0.0 or portfolio.weights[1] > 0.0:
        raise CalendarEvaluationError("terminal risky exposure")
    daily = _daily_returns(market, snapshots, start, end)
    values = tuple(daily.values())
    try:
        sharpe: float | None = annualized_sharpe(values)
    except CalendarIntegrityError:
        sharpe = None
    return RunResult(
        name=trial.name,
        net_return=portfolio.wealth - 1.0,
        annualized_sharpe=sharpe,
        maximum_drawdown=maximum_drawdown(values),
        daily_returns=daily,
        pnl_events=tuple(events),
        asset_net=portfolio.asset_net,
        cell_net=portfolio.cell_net,
        counters=counters,
        eligible_hours=eligible_hours,
        exposed_hours=(exposed_hours[0], exposed_hours[1]),
        asset_refresh_weeks=(frozenset(asset_weeks[0]), frozenset(asset_weeks[1])),
        regime_pnl=regime_pnl,
        regime_exposed_hours=regime_hours,
        regime_entries=regime_entries,
        regime_refresh_weeks={key: frozenset(value) for key, value in regime_weeks.items()},
        quarantine_liquidations=liquidation_count,
        final_cash=True,
    )


def prior_daily_sharpes(experiments_root: Path) -> tuple[float, ...]:
    """Load the 21 hash-bound frequency-compatible prior trial records."""

    result: list[float] = []
    for experiment, expected_hash in PRIOR_RESULT_HASHES.items():
        path = experiments_root / experiment / "DEVELOPMENT_RESULT.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CalendarEvaluationError("missing immutable DSR prior result") from exc
        observed = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if observed != expected_hash:
            raise CalendarEvaluationError("immutable DSR prior result hash mismatch")
        variants = payload.get("variants")
        if not isinstance(variants, Mapping):
            raise CalendarEvaluationError("DSR prior variants missing")
        for name in PRIOR_TRIAL_ORDERS[experiment]:
            value = variants.get(name)
            if not isinstance(value, Mapping):
                raise CalendarEvaluationError("DSR prior trial missing")
            annualized = value.get("annualized_sharpe")
            if not isinstance(annualized, (int, float)) or not math.isfinite(annualized):
                raise CalendarEvaluationError("DSR prior Sharpe incompatible")
            result.append(float(annualized) / math.sqrt(365.0))
    if len(result) != 21:
        raise CalendarEvaluationError("expected 21 prior DSR records")
    return tuple(result)


def _common_panel(runs: Mapping[str, RunResult]) -> dict[str, tuple[float, ...]]:
    if tuple(runs) != TRIAL_ORDER:
        raise CalendarEvaluationError("trial order changed")
    common = set.intersection(*(set(run.daily_returns) for run in runs.values()))
    ordered = sorted(common)
    if len(ordered) < 320:
        raise CalendarEvaluationError("fewer than 320 common development days")
    panel = {name: tuple(runs[name].daily_returns[day] for day in ordered) for name in TRIAL_ORDER}
    if any(any(not math.isfinite(value) for value in column) for column in panel.values()):
        raise CalendarEvaluationError("nonfinite common daily panel")
    return panel


def _concentration(run: RunResult) -> dict[str, Any]:
    positives = sorted((event.pnl for event in run.pnl_events if event.pnl > 0.0), reverse=True)
    denominator = sum(positives)
    positive_cells = sorted((value for value in run.cell_net if value > 0.0), reverse=True)
    cell_denominator = sum(positive_cells)
    if denominator <= 0.0 or cell_denominator <= 0.0:
        return {
            "pass": False,
            "reason": "nonpositive positive-profit denominator",
        }
    interval_largest = positives[0] / denominator
    interval_top_five = sum(positives[:5]) / denominator
    cell_largest = positive_cells[0] / cell_denominator
    cell_top_five = sum(positive_cells[:5]) / cell_denominator
    return {
        "positive_interval_profit": denominator,
        "largest_positive_interval_fraction": interval_largest,
        "top_five_positive_intervals_fraction": interval_top_five,
        "positive_cell_profit": cell_denominator,
        "largest_positive_cell_fraction": cell_largest,
        "top_five_positive_cells_fraction": cell_top_five,
        "pass": (
            interval_largest <= 0.25
            and interval_top_five <= 0.50
            and cell_largest <= 0.25
            and cell_top_five <= 0.50
        ),
    }


def _regime_report(run: RunResult) -> tuple[dict[str, dict[str, Any]], bool]:
    names = ("up_high", "up_low", "down_high", "down_low")
    report = {
        name: {
            "exposed_hours": run.regime_exposed_hours.get(name, 0),
            "completed_entries": run.regime_entries.get(name, 0),
            "refresh_weeks": len(run.regime_refresh_weeks.get(name, frozenset())),
            "net_currency_pnl": run.regime_pnl.get(name, 0.0),
        }
        for name in names
    }
    passed = all(
        value["exposed_hours"] >= 120
        and value["completed_entries"] >= 8
        and value["refresh_weeks"] >= 8
        and value["net_currency_pnl"] > 0.0
        for value in report.values()
    )
    return report, passed


def _simple_buy_hold(
    market: CalendarMarket,
    start: datetime,
    end: datetime,
    target: tuple[float, float, float],
) -> dict[str, Any]:
    """Costed no-rebalance comparator on the applicable frozen source grid."""

    if target == (1.0, 0.0, 0.0):
        stamps = market.records[ASSETS[0]]
        vectors = {stamp: JointVector(stamp, row.open, 1.0) for stamp, row in stamps.items()}
    elif target == (0.0, 1.0, 0.0):
        stamps = market.records[ASSETS[1]]
        vectors = {stamp: JointVector(stamp, 1.0, row.open) for stamp, row in stamps.items()}
    else:
        vectors = dict(market.joint)
    entry = vectors.get(start)
    exit_vector = vectors.get(end - timedelta(hours=1))
    if entry is None or exit_vector is None:
        raise CalendarEvaluationError("benchmark boundary unavailable")
    portfolio = rebalance(Portfolio(), target, entry, cell=bucket_at(start))
    entry_state = portfolio
    portfolio = rebalance(
        portfolio,
        (0.0, 0.0, 1.0),
        exit_vector,
        cell=bucket_at(end - timedelta(hours=2)),
    )
    snapshots = ((start, entry_state), (end - timedelta(hours=1), portfolio))
    if target == (1.0, 0.0, 0.0):
        synthetic_market = CalendarMarket(
            market.records,
            vectors,
            {stamp: market.records[ASSETS[0]][stamp].available_at for stamp in vectors},
            market.source_partition_count,
        )
    elif target == (0.0, 1.0, 0.0):
        synthetic_market = CalendarMarket(
            market.records,
            vectors,
            {stamp: market.records[ASSETS[1]][stamp].available_at for stamp in vectors},
            market.source_partition_count,
        )
    else:
        synthetic_market = market
    daily = _daily_returns(synthetic_market, snapshots, start, end)
    values = tuple(daily.values())
    try:
        sharpe: float | None = annualized_sharpe(values)
    except CalendarIntegrityError:
        sharpe = None
    return {
        "net_return": portfolio.wealth - 1.0,
        "annualized_sharpe": sharpe,
        "maximum_drawdown": maximum_drawdown(values),
        "daily_observations": len(values),
    }


def evaluate_calendar_development(
    market: CalendarMarket,
    preregistration: Mapping[str, Any],
    experiments_root: Path,
) -> dict[str, Any]:
    """Execute every frozen 2025 gate without touching January--June 2026."""

    if market.holdout_values_read:
        raise CalendarEvaluationError("holdout values were read")
    if preregistration.get("status") != "DRAFT_REVISED_REVIEWED":
        raise CalendarEvaluationError("effective frozen contract status changed")
    frozen_gates = preregistration.get("historical_gates_all_required")
    if not isinstance(frozen_gates, Mapping) or len(frozen_gates) != 17:
        raise CalendarEvaluationError("frozen historical gates missing")
    if not market.is_strict_prefix(DEVELOPMENT_END):
        market = market.prefix(DEVELOPMENT_END)
    labels = regime_labels(market, DEVELOPMENT_END)
    schedules = {
        trial.name: fit_schedules(market, trial, DEVELOPMENT_START, DEVELOPMENT_END)
        for trial in TRIALS
    }
    runs: dict[str, RunResult] = {}
    for trial in TRIALS:
        runs[trial.name] = execute_trial(
            market,
            trial,
            DEVELOPMENT_START,
            DEVELOPMENT_END,
            schedules=schedules[trial.name],
            labels=labels,
        )
    primary = runs[PRIMARY_NAME]
    primary_spec = TRIALS[0]
    doubled = execute_trial(
        market,
        primary_spec,
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        schedules=schedules[PRIMARY_NAME],
        cost_rate=BASE_COST * 2.0,
        labels=labels,
    )
    delayed = execute_trial(
        market,
        primary_spec,
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        schedules=schedules[PRIMARY_NAME],
        delay_minutes=5,
        labels=labels,
    )
    fold_runs: list[RunResult] = []
    for fold_start, fold_end in FOLDS:
        fold_market = market.prefix(fold_end)
        fold_schedules = fit_schedules(fold_market, primary_spec, fold_start, fold_end)
        fold_runs.append(
            execute_trial(
                fold_market,
                primary_spec,
                fold_start,
                fold_end,
                schedules=fold_schedules,
                labels=labels,
            )
        )
    standalone = {
        asset: execute_trial(
            market,
            primary_spec,
            DEVELOPMENT_START,
            DEVELOPMENT_END,
            schedules=schedules[PRIMARY_NAME],
            asset_only=asset,
            labels=labels,
        )
        for asset in ASSETS
    }
    leave_one_out = tuple(
        execute_trial(
            market,
            primary_spec,
            DEVELOPMENT_START,
            DEVELOPMENT_END,
            schedules=schedules[PRIMARY_NAME],
            omitted_bucket=cell,
            labels=labels,
        ).net_return
        for cell in range(48)
    )
    panel = _common_panel(runs)
    current_sharpes = tuple(
        statistics.mean(panel[name]) / statistics.stdev(panel[name]) for name in TRIAL_ORDER
    )
    all_sharpes = prior_daily_sharpes(experiments_root) + current_sharpes
    primary_panel = panel[PRIMARY_NAME]
    bootstrap = {str(block): stationary_bootstrap(primary_panel, block) for block in (7, 28, 91)}
    dsr = deflated_sharpe_probability(primary_panel, all_sharpes)
    pbo = cscv_pbo(tuple(panel[name] for name in TRIAL_ORDER))
    concentration = _concentration(primary)
    regimes, regime_pass = _regime_report(primary)
    fold_eligible = all(run.eligible_hours >= 500 for run in fold_runs)
    positive_folds = sum(run.net_return > 0.0 for run in fold_runs if run.eligible_hours >= 500)
    refresh_union = primary.asset_refresh_weeks[0] | primary.asset_refresh_weeks[1]
    sufficiency = bool(
        fold_eligible
        and primary.counters.entries >= 48
        and min(primary.counters.asset_entries) >= 16
        and all(run.counters.entries >= 8 for run in fold_runs)
        and min(primary.exposed_hours) >= 120
        and len(refresh_union) >= 26
        and min(len(value) for value in primary.asset_refresh_weeks) >= 16
        and primary.counters.target_changes >= 24
        and min(primary.asset_net) > 0.0
    )
    simple = runs[SIMPLE_NAME]
    baseline = bool(
        primary.annualized_sharpe is not None
        and simple.annualized_sharpe is not None
        and primary.net_return > simple.net_return
        and primary.annualized_sharpe > simple.annualized_sharpe
    )
    checks = {
        "aggregate_net_return_gt": primary.net_return > 0.0,
        "positive_development_folds_minimum": positive_folds >= 3 and fold_eligible,
        "annualized_daily_net_sharpe_gte": (
            primary.annualized_sharpe is not None and primary.annualized_sharpe >= 0.75
        ),
        "maximum_drawdown_lte": primary.maximum_drawdown <= 0.20,
        "doubled_cost_net_return_gt": doubled.net_return > 0.0,
        "fifth_minute_delay_net_return_gt": delayed.net_return > 0.0,
        "positive_parameter_neighbors_minimum": (
            sum(runs[name].net_return > 0.0 for name in NEIGHBORS) >= 4
        ),
        "bootstrap_all_three_lower_bounds_gt": all(
            float(report["lower_95"]) > 0.0 for report in bootstrap.values()
        ),
        "DSR_probability_gte": dsr >= 0.95,
        "within_family_PBO_lte": pbo <= 0.20,
        "baseline_superiority": baseline,
        "asset_sensitivity": all(run.net_return > 0.0 for run in standalone.values()),
        "regime_gate": regime_pass,
        "leave_one_bucket_out": all(value > 0.0 for value in leave_one_out),
        "sufficiency": sufficiency,
        "concentration": concentration.get("pass") is True,
        "data_integrity": all(run.final_cash for run in runs.values())
        and not market.holdout_values_read,
    }
    if set(checks) != set(frozen_gates):
        raise CalendarEvaluationError("implemented gate registry differs from freeze")
    all_pass = all(checks.values())
    benchmarks = {
        "cash": {
            "net_return": 0.0,
            "annualized_sharpe": None,
            "maximum_drawdown": 0.0,
        },
        "BTC_buy_and_hold": _simple_buy_hold(
            market, DEVELOPMENT_START, DEVELOPMENT_END, (1.0, 0.0, 0.0)
        ),
        "ETH_buy_and_hold": _simple_buy_hold(
            market, DEVELOPMENT_START, DEVELOPMENT_END, (0.0, 1.0, 0.0)
        ),
        "equal_weight_buy_and_hold": _simple_buy_hold(
            market, DEVELOPMENT_START, DEVELOPMENT_END, (0.5, 0.5, 0.0)
        ),
        "simple_calendar": simple.summary(),
    }
    common_days = sorted(set.intersection(*(set(run.daily_returns) for run in runs.values())))
    return {
        "schema_version": "1.0",
        "experiment_id": EXPERIMENT_ID,
        "stage": "DEVELOPMENT",
        "classification": "DEVELOPMENT_GO" if all_pass else "HISTORICAL_NO_GO",
        "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "all_development_gates_pass": all_pass,
        "gate_checks": checks,
        "failed_gates": sorted(name for name, passed in checks.items() if not passed),
        "primary": primary.summary(),
        "variants": {name: runs[name].summary() for name in TRIAL_ORDER},
        "doubled_cost": doubled.summary(),
        "fifth_minute_delay": delayed.summary(),
        "folds": [run.summary() for run in fold_runs],
        "standalone_assets": {asset: run.summary() for asset, run in standalone.items()},
        "leave_one_bucket_out_net_returns": list(leave_one_out),
        "benchmarks": benchmarks,
        "bootstrap": bootstrap,
        "deflated_sharpe_probability": dsr,
        "probability_of_backtest_overfitting": pbo,
        "prior_dsr_record_count": 21,
        "current_dsr_record_count": 7,
        "common_daily_count": len(common_days),
        "common_daily_endpoints": [day.isoformat() for day in common_days],
        "common_daily_panel": {name: list(panel[name]) for name in TRIAL_ORDER},
        "regimes": regimes,
        "concentration": concentration,
        "source_partition_count": market.source_partition_count,
        "holdout_values_read": False,
        "holdout_opened": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
        "returns_calculated": True,
        "performance_claim_made": False,
    }
