"""No-data, fail-closed primitives for the frozen relative-value v2 contract.

This deliberately has no loader, filesystem, network, or market-data dependency.
It operates on supplied immutable synthetic/authorised records only.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import combinations
from statistics import NormalDist, mean, stdev

SYMBOLS = ("BTCUSDT", "ETHUSDT")
CASH = "CASH"
TRIAL_ORDER = (
    "primary_risk_adjusted_20_60_120",
    "raw_60_session_relative_strength_rotation",
    "short_10_30_60_horizons",
    "long_60_120_180_horizons",
    "raw_unadjusted_20_60_120",
    "wide_0_50_rotation_gap",
    "always_in_higher_score_no_cash_filter",
)
V2_BOOTSTRAP_SEED = 4689472421920140622


class RelativeValueV2Error(ValueError):
    """The frozen contract cannot be satisfied."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RelativeValueV2Error("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _finite(value: float, label: str) -> float:
    if not math.isfinite(value):
        raise RelativeValueV2Error(f"{label} must be finite")
    return value


@dataclass(frozen=True)
class Observation:
    """One required lookback item, with its event and causal-availability times."""

    asset: str
    event_at: datetime
    available_at: datetime
    value: float

    def __post_init__(self) -> None:
        if self.asset not in SYMBOLS:
            raise RelativeValueV2Error("unknown asset")
        _utc(self.event_at)
        _utc(self.available_at)
        _finite(self.value, "observation")


@dataclass(frozen=True)
class MinuteRow:
    asset: str
    timestamp: datetime
    price: float
    row_id: str

    def __post_init__(self) -> None:
        if self.asset not in SYMBOLS or not self.row_id:
            raise RelativeValueV2Error("invalid minute-row identity")
        _utc(self.timestamp)
        if _finite(self.price, "price") <= 0:
            raise RelativeValueV2Error("price must be positive")


@dataclass(frozen=True)
class CanonicalVector:
    timestamp: datetime
    rows: tuple[MinuteRow, MinuteRow]

    def __post_init__(self) -> None:
        stamp = _utc(self.timestamp)
        if len(self.rows) != 2 or {row.asset for row in self.rows} != set(SYMBOLS):
            raise RelativeValueV2Error("canonical vector requires exactly both assets")
        if any(_utc(row.timestamp) != stamp for row in self.rows):
            raise RelativeValueV2Error("canonical vector must be atomic")

    @property
    def prices(self) -> Mapping[str, float]:
        return {row.asset: row.price for row in self.rows}


@dataclass(frozen=True)
class DecisionTrace:
    session_id: str
    cutoff: datetime
    due_timestamp: datetime
    desired: str
    actual_before: str
    actual_after: str
    pending_before: str | None
    pending_after: str | None
    disposition: str


def information_cutoff(observations: Sequence[Observation]) -> datetime:
    """Maximum timestamp over every required observation for both assets."""
    if not observations or {item.asset for item in observations} != set(SYMBOLS):
        raise RelativeValueV2Error("full synchronized information set is required")
    return max(max(_utc(item.event_at), _utc(item.available_at)) for item in observations)


def canonical_vector_after(
    cutoff: datetime, rows: Sequence[MinuteRow], *, end: datetime | None = None
) -> CanonicalVector:
    """Return the earliest exact two-row vector strictly after the joint cutoff."""
    cutoff = _utc(cutoff)
    boundary = _utc(end) if end is not None else None
    grouped: dict[datetime, dict[str, MinuteRow]] = {}
    previous: dict[str, datetime] = {}
    for row in rows:
        stamp = _utc(row.timestamp)
        if boundary is not None and stamp >= boundary:
            continue
        if row.asset in previous and stamp <= previous[row.asset]:
            raise RelativeValueV2Error("duplicate or nonmonotonic minute rows")
        previous[row.asset] = stamp
        at_stamp = grouped.setdefault(stamp, {})
        if row.asset in at_stamp:
            raise RelativeValueV2Error("duplicate minute row")
        at_stamp[row.asset] = row
    eligible = [
        stamp for stamp, vector in grouped.items() if stamp > cutoff and set(vector) == set(SYMBOLS)
    ]
    if not eligible:
        raise RelativeValueV2Error("no exact synchronized vector after information cutoff")
    stamp = min(eligible)
    vector = grouped[stamp]
    return CanonicalVector(stamp, (vector[SYMBOLS[0]], vector[SYMBOLS[1]]))


def terminal_vector(vectors: Sequence[CanonicalVector], end: datetime) -> CanonicalVector:
    boundary = _utc(end)
    inside = [item for item in vectors if _utc(item.timestamp) < boundary]
    if not inside:
        raise RelativeValueV2Error("no terminal vector inside half-open boundary")
    if any(
        _utc(inside[i].timestamp) <= _utc(inside[i - 1].timestamp) for i in range(1, len(inside))
    ):
        raise RelativeValueV2Error("vectors must be strictly chronological")
    return inside[-1]


def target_weights(target: str) -> Mapping[str, float]:
    if target == CASH:
        return {asset: 0.0 for asset in SYMBOLS}
    if target not in SYMBOLS:
        raise RelativeValueV2Error("invalid target")
    return {asset: float(asset == target) for asset in SYMBOLS}


def run_clock(
    decisions: Sequence[tuple[str, str]], vectors: Sequence[CanonicalVector], *, delayed: bool
) -> tuple[DecisionTrace, ...]:
    """Independent cash-initialized C_s/C_(s+1) clock with terminal override."""
    if len(decisions) != len(vectors) or not decisions:
        raise RelativeValueV2Error("decisions and vectors must align")
    actual, pending, traces = CASH, None, []
    for index, ((session_id, desired), vector) in enumerate(zip(decisions, vectors, strict=True)):
        if desired not in {CASH, *SYMBOLS}:
            raise RelativeValueV2Error("invalid desired target")
        before: str = actual
        pending_before: str | None = pending
        is_terminal = index == len(vectors) - 1
        if delayed and pending is not None:
            actual, pending = pending, None
        if is_terminal:
            actual, pending = CASH, None
            disposition = "terminal_cash"
        elif delayed:
            pending = desired
            disposition = "queued_for_next_vector"
        else:
            actual = desired
            disposition = "executed_at_current_vector"
        traces.append(
            DecisionTrace(
                session_id,
                vector.timestamp,
                vector.timestamp,
                desired,
                before,
                actual,
                pending_before,
                pending,
                disposition,
            )
        )
    if actual != CASH or pending is not None:
        raise RelativeValueV2Error("terminal state is not exact cash")
    return tuple(traces)


def common_endpoint_panel(series: Mapping[str, Mapping[datetime, float]]) -> tuple[datetime, ...]:
    if tuple(series) != TRIAL_ORDER:
        raise RelativeValueV2Error("trial identities/order are frozen")
    endpoints = set.intersection(*(set(values) for values in series.values()))
    ordered = tuple(sorted(endpoints))
    if any(not math.isfinite(series[name][stamp]) for name in TRIAL_ORDER for stamp in ordered):
        raise RelativeValueV2Error("nonfinite common return")
    return ordered


def primitive_dsr_valid(primary: Sequence[float]) -> bool:
    """The inherited primitive accepts T=3, but not fewer observations."""
    return len(primary) >= 3 and all(math.isfinite(item) for item in primary) and stdev(primary) > 0


def phase2_dsr_degenerate(
    primary: Sequence[float], registry_sharpes: Sequence[float | None], *, slots: int
) -> bool:
    return (
        slots != 56
        or len(registry_sharpes) != 56
        or sum(item is not None for item in registry_sharpes) != 35
        or len(primary) < 30
        or not primitive_dsr_valid(primary)
        or any(item is not None and not math.isfinite(item) for item in registry_sharpes)
        or stdev([item for item in registry_sharpes if item is not None]) == 0
    )


def phase2_dsr_probability(
    primary: Sequence[float], registry_sharpes: Sequence[float | None], *, slots: int = 56
) -> float:
    """Phase-2 DSR with the frozen 28-lag Bartlett dependence penalty.

    This is a pure statistic over a supplied common panel; callers must not use it
    for an unauthorised economic evaluation.
    """
    if phase2_dsr_degenerate(primary, registry_sharpes, slots=slots):
        return 0.0
    values = [float(item) for item in primary]
    center = mean(values)
    centered = [item - center for item in values]
    denominator = sum(item * item for item in centered)
    if denominator <= 0:
        return 0.0
    correlations = [
        sum(centered[index] * centered[index - lag] for index in range(lag, len(values)))
        / denominator
        for lag in range(1, 29)
    ]
    vif = max(
        1.0, 1.0 + 2.0 * sum((1.0 - lag / 29.0) * rho for lag, rho in enumerate(correlations, 1))
    )
    effective_n = len(values) / vif
    observed = center / stdev(values)
    observed_slots = [item for item in registry_sharpes if item is not None]
    sigma = stdev(observed_slots)
    normal = NormalDist()
    euler_gamma = 0.5772156649015329
    sr0 = sigma * (
        (1 - euler_gamma) * normal.inv_cdf(1 - 1 / slots)
        + euler_gamma * normal.inv_cdf(1 - 1 / (slots * math.e))
    )
    skew = sum(item**3 for item in centered) / len(values) / (stdev(values) ** 3)
    nonexcess_kurtosis = sum(item**4 for item in centered) / len(values) / (stdev(values) ** 4)
    probability_denominator = 1 - skew * observed + ((nonexcess_kurtosis - 1) / 4) * observed**2
    if (
        effective_n < 30
        or probability_denominator <= 0
        or not math.isfinite(probability_denominator)
    ):
        return 0.0
    value = normal.cdf(
        (observed - sr0) * math.sqrt(effective_n - 1) / math.sqrt(probability_denominator)
    )
    return value if math.isfinite(value) else 0.0


def finite_equal(left: float, right: float) -> bool:
    return (
        math.isfinite(left)
        and math.isfinite(right)
        and math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)
    )


def pbo_rankable_sharpe(values: Sequence[float]) -> float:
    if not values or any(not math.isfinite(item) for item in values):
        raise RelativeValueV2Error("PBO requires finite raw returns")
    deviation = stdev(values) if len(values) > 1 else 0.0
    if deviation == 0:
        return math.inf if mean(values) > 0 else -math.inf
    return mean(values) / deviation


def pbo_cscv(panel: Sequence[Sequence[float]]) -> float:
    """Exact 8-block/70-split current-trial CSCV, including finite zero-vol ranks."""
    if len(panel) != 7 or not panel or len(panel[0]) < 8:
        return 1.0
    if any(
        len(column) != len(panel[0]) or any(not math.isfinite(value) for value in column)
        for column in panel
    ):
        return 1.0
    length = len(panel[0])
    blocks = [list(range(round(i * length / 8), round((i + 1) * length / 8))) for i in range(8)]
    if any(not block for block in blocks):
        return 1.0
    overfit = 0
    for train_blocks in combinations(range(8), 4):
        train = [index for block in train_blocks for index in blocks[block]]
        test = [index for block in range(8) if block not in train_blocks for index in blocks[block]]
        train_scores = [pbo_rankable_sharpe([column[index] for index in train]) for column in panel]
        winner = max(range(7), key=lambda index: (train_scores[index], -index))
        test_scores = [pbo_rankable_sharpe([column[index] for index in test]) for column in panel]
        ordered = sorted(test_scores)
        rank = (
            ordered.index(test_scores[winner])
            + 1
            + len(ordered)
            - ordered[::-1].index(test_scores[winner])
        ) / 2
        relative_rank = rank / 8
        if math.log(relative_rank / (1 - relative_rank)) <= 0:
            overfit += 1
    return overfit / 70
