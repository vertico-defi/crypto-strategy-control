"""Pure deterministic primitives for the frozen intraday calendar study.

This deliberately accepts only already validated values.  It has no loader,
clock, filesystem, scheduling, or order-routing dependency.
"""

from __future__ import annotations

import hashlib
import importlib
import itertools
import math
import statistics
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from statistics import NormalDist
from typing import Any

ASSETS = ("BTCUSDT", "ETHUSDT")
H = (1.0 - 0.0014) ** -2 - 1.0
BASE_COST = 0.0014


class CalendarIntegrityError(ValueError):
    """Raised when an input violates a frozen invariant."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise CalendarIntegrityError("timestamp must be UTC")
    return value


def bucket_at(moment: datetime, *, split: bool = True) -> int:
    """Return the registered bucket index for an exact UTC-labelled hour."""
    moment = _utc(moment)
    if moment.minute or moment.second or moment.microsecond:
        raise CalendarIntegrityError("bucket requires exact hour")
    return moment.hour * 2 + (1 if moment.weekday() >= 5 else 0) if split else moment.hour


def split_buckets() -> tuple[tuple[int, bool], ...]:
    return tuple((hour, weekend) for hour in range(24) for weekend in (False, True))


def hypothesis_order() -> tuple[tuple[str, int], ...]:
    return tuple((asset, cell) for asset in ASSETS for cell in range(48))


def monday_refresh(moment: datetime) -> datetime:
    moment = _utc(moment)
    midnight = moment.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight - timedelta(days=midnight.weekday())


def schedule_for_interval(interval: datetime) -> datetime:
    """The prior Monday controls Monday 00; R controls R+1 and later."""
    interval = _utc(interval)
    if interval.minute or interval.second or interval.microsecond:
        raise CalendarIntegrityError("interval must be an exact hour")
    r = monday_refresh(interval)
    return r - timedelta(days=7) if interval == r else r


def deadline_for(interval: datetime) -> datetime:
    return _utc(interval) - timedelta(minutes=1)


def decision_is_timely(interval: datetime, materialized: datetime) -> bool:
    return _utc(materialized) <= deadline_for(interval)


@dataclass(frozen=True)
class TrialSpec:
    name: str
    lookback_weeks: int
    buckets: int
    trim_fraction: float
    observation_minimum: int | tuple[int, int]
    week_minimum: int
    holm_alpha: float | None


TRIALS = (
    TrialSpec("primary_26_week_split_trimmed_holm_0_05", 26, 48, 0.05, (80, 32), 20, 0.05),
    TrialSpec("simple_26_week_hour_only_untrimmed_mean_above_H", 26, 24, 0.0, 112, 20, None),
    TrialSpec("short_13_week_split_trimmed_holm_0_05", 13, 48, 0.05, (40, 16), 10, 0.05),
    TrialSpec("long_39_week_split_trimmed_holm_0_05", 39, 48, 0.05, (120, 48), 30, 0.05),
    TrialSpec("liberal_26_week_split_trimmed_holm_0_10", 26, 48, 0.05, (80, 32), 20, 0.10),
    TrialSpec("strict_26_week_split_trimmed_holm_0_025", 26, 48, 0.05, (80, 32), 20, 0.025),
    TrialSpec("untrimmed_26_week_split_holm_0_05", 26, 48, 0.0, (80, 32), 20, 0.05),
)
TRIAL_BY_NAME = {trial.name: trial for trial in TRIALS}


@dataclass(frozen=True)
class Observation:
    value: float
    endpoint: datetime
    week: datetime


@dataclass(frozen=True)
class CellEstimate:
    mean: float | None
    standard_error: float | None
    p_value: float
    retained: tuple[Observation, ...]
    eligible: bool


def trim_observations(
    observations: Sequence[Observation], fraction: float
) -> tuple[Observation, ...]:
    if not 0 <= fraction < 0.5:
        raise CalendarIntegrityError("invalid trim fraction")
    if len({item.endpoint for item in observations}) != len(observations):
        raise CalendarIntegrityError("duplicate endpoint")
    if any(not math.isfinite(item.value) for item in observations):
        raise CalendarIntegrityError("nonfinite observation")
    ordered = sorted(observations, key=lambda item: (item.value, _utc(item.endpoint)))
    g = math.floor(fraction * len(ordered))
    return tuple(ordered[g : len(ordered) - g] if g else ordered)


def estimate_cell(
    observations: Sequence[Observation],
    *,
    trim_fraction: float,
    minimum: int,
    minimum_weeks: int,
    cdf: Callable[[float, int], float] | None,
    hurdle: float = H,
) -> CellEstimate:
    """Estimate a cell; every invalidity becomes the mandated inactive p=1."""
    try:
        retained = trim_observations(observations, trim_fraction)
        weeks = {item.week for item in retained}
        if len(retained) < minimum or len(weeks) < minimum_weeks:
            raise CalendarIntegrityError("sample minimum")
        mean = sum(item.value for item in retained) / len(retained)
        residuals = [
            sum(item.value - mean for item in retained if item.week == week) for week in weeks
        ]
        g = len(weeks)
        se = math.sqrt(g / (g - 1) * sum(x * x for x in residuals)) / len(retained)
        if not math.isfinite(mean) or not math.isfinite(se) or se <= 0 or cdf is None:
            raise CalendarIntegrityError("undefined inference")
        p = 1.0 - cdf((mean - hurdle) / se, g - 1)
        if not math.isfinite(p) or not 0 <= p <= 1:
            raise CalendarIntegrityError("invalid cdf")
        return CellEstimate(mean, se, p, retained, True)
    except (CalendarIntegrityError, ArithmeticError, ValueError):
        return CellEstimate(None, None, 1.0, (), False)


def holm_active(
    estimates: Sequence[CellEstimate], alpha: float, *, hurdle: float = H
) -> tuple[bool, ...]:
    if len(estimates) != 96 or not 0 < alpha < 1:
        raise CalendarIntegrityError("Holm requires 96 estimates and valid alpha")
    indexed = sorted(enumerate(estimates), key=lambda pair: (pair[1].p_value, pair[0]))
    active = [False] * 96
    for rank, (index, estimate) in enumerate(indexed, 1):
        if estimate.p_value <= alpha / (97 - rank):
            active[index] = bool(
                estimate.eligible and estimate.mean is not None and estimate.mean > hurdle
            )
        else:
            break
    return tuple(active)


def joint_targets(active_btc: bool, active_eth: bool) -> tuple[float, float, float]:
    if active_btc and active_eth:
        return (0.5, 0.5, 0.0)
    if active_btc:
        return (1.0, 0.0, 0.0)
    if active_eth:
        return (0.0, 1.0, 0.0)
    return (0.0, 0.0, 1.0)


@dataclass(frozen=True)
class JointVector:
    timestamp: datetime
    btc: float
    eth: float

    def __post_init__(self) -> None:
        _utc(self.timestamp)
        if not all(math.isfinite(v) and v > 0 for v in (self.btc, self.eth)):
            raise CalendarIntegrityError("joint prices must be finite positive")


def exact_joint_vector(rows: Sequence[JointVector], expected: datetime) -> JointVector:
    expected = _utc(expected)
    matches = [row for row in rows if row.timestamp == expected]
    if len(matches) != 1:
        raise CalendarIntegrityError("missing or duplicate exact synchronized vector")
    return matches[0]


@dataclass(frozen=True)
class Portfolio:
    wealth: float = 1.0
    weights: tuple[float, float, float] = (0.0, 0.0, 1.0)
    mark: JointVector | None = None
    asset_net: tuple[float, float] = (0.0, 0.0)
    cell_net: tuple[float, ...] = (0.0,) * 48

    def __post_init__(self) -> None:
        if not math.isfinite(self.wealth) or self.wealth <= 0:
            raise CalendarIntegrityError("wealth must be positive")
        if len(self.weights) != 3 or any(not math.isfinite(x) or x < 0 for x in self.weights):
            raise CalendarIntegrityError("invalid weights")
        if abs(sum(self.weights) - 1) > 1e-12:
            raise CalendarIntegrityError("weights must sum to one")
        if len(self.cell_net) != 48:
            raise CalendarIntegrityError("need all 48 cell attributions")


def rebalance(
    portfolio: Portfolio,
    target: tuple[float, float, float],
    fill: JointVector,
    *,
    cost_rate: float = BASE_COST,
    cell: int | None = None,
) -> Portfolio:
    """Mark then atomically rebalance.  This handles initial/terminal costs too."""
    if not math.isfinite(cost_rate) or cost_rate < 0:
        raise CalendarIntegrityError("invalid cost rate")
    if (
        len(target) != 3
        or any(not math.isfinite(x) or x < 0 for x in target)
        or abs(sum(target) - 1) > 1e-12
    ):
        raise CalendarIntegrityError("invalid target")
    w_b, w_e, w_c = portfolio.weights
    gross_b = gross_e = 0.0
    if portfolio.mark is None:
        pre, drift = portfolio.wealth, portfolio.weights
    else:
        rb, re = fill.btc / portfolio.mark.btc, fill.eth / portfolio.mark.eth
        gross_b, gross_e = portfolio.wealth * w_b * (rb - 1), portfolio.wealth * w_e * (re - 1)
        factor = w_c + w_b * rb + w_e * re
        if not math.isfinite(factor) or factor <= 0:
            raise CalendarIntegrityError("invalid gross factor")
        pre, drift = portfolio.wealth * factor, (w_b * rb / factor, w_e * re / factor, w_c / factor)
    turnover = 0.5 * sum(abs(a - b) for a, b in zip(target, drift, strict=True))
    if cost_rate * turnover >= 1:
        raise CalendarIntegrityError("cost consumes wealth")
    post = pre * (1 - cost_rate * turnover)
    trades = [abs(target[i] * post - drift[i] * pre) for i in range(2)]
    cost = pre - post
    if sum(trades) == 0 and cost != 0:
        raise CalendarIntegrityError("unattributable cost")
    costs = [cost * trade / sum(trades) if sum(trades) else 0.0 for trade in trades]
    asset = (
        portfolio.asset_net[0] + gross_b - costs[0],
        portfolio.asset_net[1] + gross_e - costs[1],
    )
    cells = list(portfolio.cell_net)
    if cell is not None:
        if not 0 <= cell < 48:
            raise CalendarIntegrityError("invalid cell")
        cells[cell] += gross_b + gross_e - cost
    return Portfolio(post, target, fill, asset, tuple(cells))


@dataclass(frozen=True)
class Quarantine:
    exposed: bool
    consecutive_valid: int = 0
    pending: tuple[float, float, float] | None = None

    def trigger(self) -> Quarantine:
        return Quarantine(self.exposed, 0, None)

    def valid_minute(self) -> Quarantine:
        return Quarantine(self.exposed, self.consecutive_valid + 1, self.pending)

    def may_resume(self, boundary: datetime, timely: bool) -> bool:
        return (
            not self.exposed
            and self.consecutive_valid >= 60
            and timely
            and _utc(boundary).minute == 0
        )


@dataclass(frozen=True)
class PendingTarget:
    target: tuple[float, float, float]
    hour: datetime
    valid_events: int = 0

    def event(self, vector: JointVector) -> PendingTarget | None:
        if not self.hour < vector.timestamp < self.hour + timedelta(hours=1):
            return self
        if self.valid_events + 1 == 5:
            return None
        return PendingTarget(self.target, self.hour, self.valid_events + 1)

    def timed_out(self, now: datetime) -> bool:
        return _utc(now) >= self.hour + timedelta(hours=1) and self.valid_events < 5


def terminal_target_time(end: datetime) -> datetime:
    end = _utc(end)
    return end - timedelta(hours=1)


def fold_prefix(rows: Sequence[datetime], end: datetime) -> tuple[datetime, ...]:
    end = _utc(end)
    return tuple(row for row in rows if _utc(row) < end)


def bootstrap_seed(block_length: int) -> int:
    if block_length not in (7, 28, 91):
        raise CalendarIntegrityError("undeclared bootstrap block")
    text = f"btc-eth-intraday-calendar-seasonality-v1|stationary-bootstrap|{block_length}"
    return int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big", signed=False)


def nonannualized_sharpe(values: Sequence[float]) -> float:
    if len(values) < 2 or any(not math.isfinite(v) for v in values):
        raise CalendarIntegrityError("invalid Sharpe series")
    deviation = statistics.stdev(values)
    if deviation == 0:
        raise CalendarIntegrityError("zero volatility")
    return statistics.mean(values) / deviation


def annualized_sharpe(values: Sequence[float]) -> float:
    """Frozen daily Sharpe annualized by ``sqrt(365)``."""

    return nonannualized_sharpe(values) * math.sqrt(365.0)


def aggregate_return(values: Sequence[float]) -> float:
    wealth = 1.0
    for value in values:
        if not math.isfinite(value) or value <= -1.0:
            raise CalendarIntegrityError("invalid return")
        wealth *= 1.0 + value
    return wealth - 1.0


def maximum_drawdown(values: Sequence[float]) -> float:
    wealth = peak = 1.0
    result = 0.0
    for value in values:
        if not math.isfinite(value) or value <= -1.0:
            raise CalendarIntegrityError("invalid return")
        wealth *= 1.0 + value
        peak = max(peak, wealth)
        result = max(result, 1.0 - wealth / peak)
    return result


def stationary_bootstrap(
    values: Sequence[float],
    block_length: int,
    *,
    resamples: int = 2000,
    rng: Any | None = None,
) -> dict[str, float | int]:
    """Frozen stationary circular bootstrap using NumPy PCG64 by default."""

    if (
        len(values) < 1
        or any(not math.isfinite(value) for value in values)
        or resamples != 2000
        or block_length not in (7, 28, 91)
    ):
        raise CalendarIntegrityError("invalid frozen bootstrap input")
    seed = bootstrap_seed(block_length)
    if rng is None:
        numpy = importlib.import_module("numpy")
        rng = numpy.random.Generator(numpy.random.PCG64(seed))
    count = len(values)
    restart = 1.0 / block_length
    means: list[float] = []
    for _ in range(resamples):
        index = int(rng.integers(count))
        sample: list[float] = []
        for _ in range(count):
            sample.append(values[index])
            index = (
                int(rng.integers(count)) if float(rng.random()) < restart else (index + 1) % count
            )
        means.append(statistics.mean(sample))
    means.sort()

    def percentile(q: float) -> float:
        position = (len(means) - 1) * q
        lower, upper = math.floor(position), math.ceil(position)
        if lower == upper:
            return means[lower]
        return means[lower] + (means[upper] - means[lower]) * (position - lower)

    return {
        "seed": seed,
        "block_length": block_length,
        "resamples": resamples,
        "mean": statistics.mean(values),
        "lower_95": percentile(0.025),
        "upper_95": percentile(0.975),
    }


def deflated_sharpe_probability(values: Sequence[float], sharpe_records: Sequence[float]) -> float:
    """Compute the frozen 28-trial dependence-adjusted DSR probability."""

    if len(sharpe_records) != 28 or len(values) < 29:
        return 0.0
    try:
        if any(not math.isfinite(value) for value in sharpe_records):
            return 0.0
        observed = nonannualized_sharpe(values)
        sigma = statistics.stdev(sharpe_records)
        if sigma <= 0.0:
            return 0.0
        mean = statistics.mean(values)
        denominator_ac = sum((value - mean) ** 2 for value in values)
        if denominator_ac <= 0.0:
            return 0.0
        vif = 1.0 + 2.0 * sum(
            (1.0 - lag / 29.0)
            * (
                sum(
                    (values[index] - mean) * (values[index - lag] - mean)
                    for index in range(lag, len(values))
                )
                / denominator_ac
            )
            for lag in range(1, 29)
        )
        vif = max(1.0, vif)
        effective = len(values) / vif
        if not math.isfinite(effective) or effective < 30.0:
            return 0.0
        deviation = statistics.stdev(values)
        n = len(values)
        standardized = [(value - mean) / deviation for value in values]
        skew = n / ((n - 1) * (n - 2)) * sum(value**3 for value in standardized)
        excess_kurtosis = n * (n + 1) / ((n - 1) * (n - 2) * (n - 3)) * sum(
            value**4 for value in standardized
        ) - 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))
        nonexcess_kurtosis = excess_kurtosis + 3.0
        normal = NormalDist()
        gamma = 0.5772156649015329
        sr0 = sigma * (
            (1.0 - gamma) * normal.inv_cdf(1.0 - 1.0 / 28.0)
            + gamma * normal.inv_cdf(1.0 - 1.0 / (28.0 * math.e))
        )
        denominator = 1.0 - skew * observed + (nonexcess_kurtosis - 1.0) / 4.0 * observed * observed
        if denominator <= 0.0 or not all(
            math.isfinite(value) for value in (observed, skew, nonexcess_kurtosis, sr0, denominator)
        ):
            return 0.0
        probability = normal.cdf(
            (observed - sr0) * math.sqrt(effective - 1.0) / math.sqrt(denominator)
        )
        return probability if math.isfinite(probability) else 0.0
    except (ArithmeticError, CalendarIntegrityError, statistics.StatisticsError, ValueError):
        return 0.0


def _pbo_sharpe(values: Sequence[float]) -> float:
    if not values or any(not math.isfinite(value) for value in values):
        raise CalendarIntegrityError("invalid PBO series")
    mean = statistics.mean(values)
    if len(values) < 2:
        raise CalendarIntegrityError("insufficient PBO series")
    deviation = statistics.stdev(values)
    if deviation == 0.0:
        return math.inf if mean > 0.0 else -math.inf
    return mean / deviation


def cscv_pbo(values: Sequence[Sequence[float]]) -> float:
    """Exact frozen 8-block/70-split within-family PBO."""

    if len(values) != 7 or any(
        len(column) < 8
        or len(column) != len(values[0])
        or any(not math.isfinite(x) for x in column)
        for column in values
    ):
        return 1.0
    length = len(values[0])
    quotient, remainder = divmod(length, 8)
    cuts: list[tuple[int, int]] = []
    cursor = 0
    for index in range(8):
        size = quotient + int(index < remainder)
        if size == 0:
            return 1.0
        cuts.append((cursor, cursor + size))
        cursor += size
    events = 0
    try:
        for train_blocks in itertools.combinations(range(8), 4):
            train = set(train_blocks)
            test = set(range(8)) - train
            training = [
                [item for block in train for item in column[slice(*cuts[block])]]
                for column in values
            ]
            selected = max(range(7), key=lambda index: (_pbo_sharpe(training[index]), -index))
            testing = [
                _pbo_sharpe([item for block in test for item in column[slice(*cuts[block])]])
                for column in values
            ]
            score = testing[selected]
            rank = (
                sum(value < score for value in testing)
                + sum(value <= score for value in testing)
                + 1
            ) / 2.0
            relative = rank / 8.0
            if not 0.0 < relative < 1.0:
                return 1.0
            if math.log(relative / (1.0 - relative)) <= 0.0:
                events += 1
    except (ArithmeticError, CalendarIntegrityError, ValueError):
        return 1.0
    return events / 70.0


def dsr_degenerate_probability(values: Sequence[float], sharpe_records: Sequence[float]) -> float:
    """Compatibility alias for the now-complete frozen DSR implementation."""

    return deflated_sharpe_probability(values, sharpe_records)


def pbo_degenerate(values: Sequence[Sequence[float]]) -> float:
    """Compatibility alias for the now-complete frozen PBO implementation."""

    return cscv_pbo(values)


@dataclass(frozen=True)
class Counters:
    entries: int = 0
    asset_entries: tuple[int, int] = (0, 0)
    episodes: int = 0
    target_changes: int = 0
    refresh_weeks: frozenset[datetime] = field(default_factory=frozenset)

    def completed_fill(
        self, old: tuple[float, float, float], new: tuple[float, float, float], when: datetime
    ) -> Counters:
        entering = old[0] == old[1] == 0 and (new[0] > 0 or new[1] > 0)
        exiting = (old[0] > 0 or old[1] > 0) and new[0] == new[1] == 0
        asset_entries = (
            self.asset_entries[0] + int(entering and new[0] > 0),
            self.asset_entries[1] + int(entering and new[1] > 0),
        )
        weeks = self.refresh_weeks | ({monday_refresh(when)} if new[0] or new[1] else set())
        return Counters(
            self.entries + int(entering),
            asset_entries,
            self.episodes + int(exiting),
            self.target_changes + int(old != new),
            weeks,
        )


def deduplicate_prospective(
    decisions: Iterable[tuple[datetime, tuple[float, float, float]]],
) -> tuple[tuple[datetime, tuple[float, float, float]], ...]:
    seen: set[datetime] = set()
    result: list[tuple[datetime, tuple[float, float, float]]] = []
    for hour, target in decisions:
        hour = _utc(hour)
        if hour.minute or hour.second or hour.microsecond:
            raise CalendarIntegrityError("prospective decision must name exact hour")
        if hour not in seen:
            seen.add(hour)
            result.append((hour, target))
    return tuple(result)
