"""Pure, no-I/O implementation primitives for frozen relative-value rotation v2.

This module accepts only caller supplied records.  It deliberately has no loader,
path, network, or market-data dependency; production validation supplies verified
buffers only after its separately authorised stage.
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
ONE_WAY_COST = 0.0014
DOUBLE_ONE_WAY_COST = 0.0028
RECOVERY_SESSIONS = 150


class RelativeValueV2Error(ValueError):
    """A frozen no-data contract invariant is not satisfied."""


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise RelativeValueV2Error("timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _finite(value: float, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise RelativeValueV2Error(f"{label} must be finite")
    return float(value)


@dataclass(frozen=True)
class TrialSpec:
    horizons: tuple[int, ...]
    risk_adjusted: bool
    cash_filter: bool
    gap: float
    tie_from_cash_btc: bool = False


TRIAL_SPECS: Mapping[str, TrialSpec] = {
    TRIAL_ORDER[0]: TrialSpec((20, 60, 120), True, True, 0.25),
    TRIAL_ORDER[1]: TrialSpec((60,), False, True, 0.0),
    TRIAL_ORDER[2]: TrialSpec((10, 30, 60), True, True, 0.25),
    TRIAL_ORDER[3]: TrialSpec((60, 120, 180), True, True, 0.25),
    TRIAL_ORDER[4]: TrialSpec((20, 60, 120), False, True, 0.25),
    TRIAL_ORDER[5]: TrialSpec((20, 60, 120), True, True, 0.5),
    TRIAL_ORDER[6]: TrialSpec((20, 60, 120), True, False, 0.0, True),
}


@dataclass(frozen=True)
class Observation:
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
        if self.asset not in SYMBOLS or not isinstance(self.row_id, str) or not self.row_id:
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
        if len(self.rows) != 2 or tuple(sorted(row.asset for row in self.rows)) != SYMBOLS:
            raise RelativeValueV2Error("canonical vector requires exactly both assets")
        if any(_utc(row.timestamp) != stamp for row in self.rows):
            raise RelativeValueV2Error("canonical vector must be atomic")

    @property
    def prices(self) -> Mapping[str, float]:
        return {row.asset: row.price for row in self.rows}

    @property
    def row_ids(self) -> tuple[str, str]:
        return tuple(
            next(row.row_id for row in self.rows if row.asset == asset) for asset in SYMBOLS
        )  # type: ignore[return-value]


class BoundaryIndex:
    """Immutable exact lookup index for one strict half-open boundary."""

    def __init__(self, rows: Sequence[MinuteRow], end: datetime) -> None:
        boundary = _utc(end)
        previous: dict[str, datetime] = {}
        seen: set[tuple[str, datetime]] = set()
        # Validate stream order before filtering so disorder can never be sorted away.
        for row in rows:
            stamp = _utc(row.timestamp)
            if row.asset in previous and stamp <= previous[row.asset]:
                raise RelativeValueV2Error("duplicate or nonmonotonic minute rows")
            key = (row.asset, stamp)
            if key in seen:
                raise RelativeValueV2Error("duplicate minute row")
            previous[row.asset] = stamp
            seen.add(key)
        retained = tuple(row for row in rows if _utc(row.timestamp) < boundary)
        grouped: dict[datetime, dict[str, MinuteRow]] = {}
        for row in retained:
            grouped.setdefault(_utc(row.timestamp), {})[row.asset] = row
        self.end = boundary
        self.rows = retained
        self._by_stamp = grouped

    def exact_vector(self, timestamp: datetime) -> CanonicalVector:
        stamp = _utc(timestamp)
        if stamp >= self.end:
            raise RelativeValueV2Error("boundary-mismatched vector lookup")
        found = self._by_stamp.get(stamp, {})
        if set(found) != set(SYMBOLS):
            raise RelativeValueV2Error("exact synchronized vector unavailable")
        return CanonicalVector(stamp, (found[SYMBOLS[0]], found[SYMBOLS[1]]))

    def earliest_after(self, cutoff: datetime) -> CanonicalVector:
        cutoff = _utc(cutoff)
        candidates = [
            stamp
            for stamp, rows in self._by_stamp.items()
            if stamp > cutoff and set(rows) == set(SYMBOLS)
        ]
        if not candidates:
            raise RelativeValueV2Error("no exact synchronized vector after information cutoff")
        return self.exact_vector(min(candidates))

    def vectors(self) -> tuple[CanonicalVector, ...]:
        return tuple(
            self.exact_vector(stamp)
            for stamp in sorted(self._by_stamp)
            if set(self._by_stamp[stamp]) == set(SYMBOLS)
        )


def information_cutoff(observations: Sequence[Observation]) -> datetime:
    """Maximum availability/event time over the full named BTC/ETH lookback."""
    if not observations or {item.asset for item in observations} != set(SYMBOLS):
        raise RelativeValueV2Error("full synchronized information set is required")
    return max(max(_utc(item.event_at), _utc(item.available_at)) for item in observations)


def canonical_vector_after(
    cutoff: datetime, rows: Sequence[MinuteRow], *, end: datetime | None = None
) -> CanonicalVector:
    # A no-end synthetic call has an artificial far boundary, never a forward scan.
    boundary = _utc(end) if end is not None else datetime.max.replace(tzinfo=UTC)
    return BoundaryIndex(rows, boundary).earliest_after(cutoff)


def terminal_vector(vectors: Sequence[CanonicalVector], end: datetime) -> CanonicalVector:
    boundary = _utc(end)
    retained = [vector for vector in vectors if _utc(vector.timestamp) < boundary]
    if not retained:
        raise RelativeValueV2Error("no terminal vector inside half-open boundary")
    if any(
        _utc(retained[i].timestamp) <= _utc(retained[i - 1].timestamp)
        for i in range(1, len(retained))
    ):
        raise RelativeValueV2Error("vectors must be strictly chronological")
    return retained[-1]


def target_weights(target: str) -> Mapping[str, float]:
    if target == CASH:
        return {asset: 0.0 for asset in SYMBOLS}
    if target not in SYMBOLS:
        raise RelativeValueV2Error("invalid target")
    return {asset: float(asset == target) for asset in SYMBOLS}


def decision_for_scores(
    trial: str,
    scores: Mapping[str, float | None],
    raw_returns: Mapping[str, Sequence[float]],
    actual: str,
) -> str:
    spec = TRIAL_SPECS.get(trial)
    if spec is None or actual not in {CASH, *SYMBOLS} or set(scores) != set(SYMBOLS):
        raise RelativeValueV2Error("invalid frozen decision inputs")
    eligible = {
        asset: value
        for asset, value in scores.items()
        if value is not None and math.isfinite(value)
    }
    if not eligible:
        return CASH
    for asset, values in raw_returns.items():
        if (
            asset not in SYMBOLS
            or len(values) != len(spec.horizons)
            or any(not math.isfinite(x) for x in values)
        ):
            raise RelativeValueV2Error("invalid raw-return lookback")
    if actual in SYMBOLS and actual not in eligible:
        return CASH
    if len(eligible) != 2:
        return actual if actual in eligible else CASH
    btc, eth = eligible[SYMBOLS[0]], eligible[SYMBOLS[1]]
    if btc == eth:
        return SYMBOLS[0] if actual == CASH and spec.tie_from_cash_btc else actual
    winner = SYMBOLS[0] if btc > eth else SYMBOLS[1]
    loser = SYMBOLS[1] if winner == SYMBOLS[0] else SYMBOLS[0]
    passes_cash = not spec.cash_filter or sorted(raw_returns[winner])[len(spec.horizons) // 2] > 0
    if actual == CASH:
        return winner if eligible[winner] - eligible[loser] >= spec.gap and passes_cash else CASH
    if actual == winner:
        return winner if passes_cash else CASH
    return winner if eligible[winner] - eligible[actual] >= spec.gap and passes_cash else actual


@dataclass(frozen=True)
class DecisionTrace:
    session_id: str
    decision_session_id: str
    due_session_id: str | None
    cutoff: datetime
    due_timestamp: datetime
    row_ids: tuple[str, str]
    desired: str
    actual_before: str
    actual_after: str
    pending_before: str | None
    pending_after: str | None
    disposition: str
    turnover: float = 0.0
    cost: float = 0.0
    interval_return: float = 0.0
    wealth: float = 1.0


def run_clock(
    decisions: Sequence[tuple[str, str]], vectors: Sequence[CanonicalVector], *, delayed: bool
) -> tuple[DecisionTrace, ...]:
    """One explicit C-clock; final vector replaces (never transiently fills) risk."""
    if len(decisions) != len(vectors) or not decisions:
        raise RelativeValueV2Error("decisions and vectors must align")
    actual, pending, traces = CASH, None, []
    for i, ((session_id, desired), vector) in enumerate(zip(decisions, vectors, strict=True)):
        if desired not in {CASH, *SYMBOLS}:
            raise RelativeValueV2Error("invalid desired target")
        before: str = actual
        pending_before: str | None = pending
        terminal = i == len(vectors) - 1
        due_id: str | None = decisions[i - 1][0] if delayed and i else None
        if terminal:
            # Do not execute the due risky pending target; terminal cash replaces it.
            actual, pending, disposition = (
                CASH,
                None,
                "terminal_cash_replaces_due" if delayed else "terminal_cash",
            )
        elif delayed:
            if pending is not None:
                actual, pending = pending, None
            pending, disposition = desired, "queued_for_next_vector"
        else:
            actual, disposition = desired, "executed_at_current_vector"
        turnover = 0.0 if before == actual else 1.0
        traces.append(
            DecisionTrace(
                session_id,
                session_id,
                due_id,
                vector.timestamp,
                vector.timestamp,
                vector.row_ids,
                desired,
                before,
                actual,
                pending_before,
                pending,
                disposition,
                turnover,
                turnover * ONE_WAY_COST,
            )
        )
    if actual != CASH or pending is not None:
        raise RelativeValueV2Error("terminal state is not exact cash")
    return tuple(traces)


def common_endpoint_panel(series: Mapping[str, Mapping[datetime, float]]) -> tuple[datetime, ...]:
    if tuple(series) != TRIAL_ORDER:
        raise RelativeValueV2Error("trial identities/order are frozen")
    endpoints = tuple(sorted(set.intersection(*(set(values) for values in series.values()))))
    if any(not math.isfinite(series[name][stamp]) for name in TRIAL_ORDER for stamp in endpoints):
        raise RelativeValueV2Error("nonfinite common return")
    return endpoints


def primitive_dsr_valid(primary: Sequence[float]) -> bool:
    return len(primary) >= 3 and all(math.isfinite(item) for item in primary) and stdev(primary) > 0


def phase2_dsr_degenerate(
    primary: Sequence[float], registry_sharpes: Sequence[float | None], *, slots: int
) -> bool:
    return (
        slots != 56
        or len(registry_sharpes) != 56
        or sum(x is not None for x in registry_sharpes) != 35
        or len(primary) < 30
        or not primitive_dsr_valid(primary)
        or any(x is not None and not math.isfinite(x) for x in registry_sharpes)
        or stdev([x for x in registry_sharpes if x is not None]) == 0
    )


def phase2_dsr_probability(
    primary: Sequence[float], registry_sharpes: Sequence[float | None], *, slots: int = 56
) -> float:
    if phase2_dsr_degenerate(primary, registry_sharpes, slots=slots):
        return 0.0
    values = [float(x) for x in primary]
    avg = mean(values)
    centered = [x - avg for x in values]
    denom = sum(x * x for x in centered)
    if denom <= 0:
        return 0.0
    rhos = [
        sum(centered[i] * centered[i - lag] for i in range(lag, len(values))) / denom
        for lag in range(1, 29)
    ]
    vif = max(1.0, 1.0 + 2 * sum((1 - lag / 29) * rho for lag, rho in enumerate(rhos, 1)))
    effective_n = len(values) / vif
    observed = avg / stdev(values)
    observed_slots = [x for x in registry_sharpes if x is not None]
    sigma = stdev(observed_slots)
    normal = NormalDist()
    gamma = 0.5772156649015329
    sr0 = sigma * (
        (1 - gamma) * normal.inv_cdf(1 - 1 / slots)
        + gamma * normal.inv_cdf(1 - 1 / (slots * math.e))
    )
    skew = sum(x**3 for x in centered) / len(values) / stdev(values) ** 3
    kurt = sum(x**4 for x in centered) / len(values) / stdev(values) ** 4
    pdenom = 1 - skew * observed + ((kurt - 1) / 4) * observed**2
    if (
        not all(math.isfinite(x) for x in (vif, effective_n, observed, sr0, skew, kurt, pdenom))
        or effective_n < 30
        or pdenom <= 0
    ):
        return 0.0
    result = normal.cdf((observed - sr0) * math.sqrt(effective_n - 1) / math.sqrt(pdenom))
    return result if math.isfinite(result) else 0.0


def finite_equal(left: float, right: float) -> bool:
    return (
        math.isfinite(left)
        and math.isfinite(right)
        and math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)
    )


def pbo_rankable_sharpe(values: Sequence[float]) -> float:
    if not values or any(not math.isfinite(x) for x in values):
        raise RelativeValueV2Error("PBO requires finite raw returns")
    deviation = stdev(values) if len(values) > 1 else 0.0
    return (
        (math.inf if mean(values) > 0 else -math.inf)
        if deviation == 0
        else mean(values) / deviation
    )


def pbo_cscv(panel: Sequence[Sequence[float]]) -> float:
    if len(panel) != 7 or not panel or len(panel[0]) < 8:
        return 1.0
    size = len(panel[0])
    if any(len(row) != size or any(not math.isfinite(x) for x in row) for row in panel):
        return 1.0
    # numpy.array_split equivalent: first size % 8 blocks receive one extra row.
    q, r = divmod(size, 8)
    blocks = []
    start = 0
    for i in range(8):
        stop = start + q + (i < r)
        blocks.append(tuple(range(start, stop)))
        start = stop
    if any(not block for block in blocks):
        return 1.0
    overfit = 0
    for selected in combinations(range(8), 4):
        train = [i for b in selected for i in blocks[b]]
        test = [i for b in range(8) if b not in selected for i in blocks[b]]
        train_scores = [pbo_rankable_sharpe([row[i] for i in train]) for row in panel]
        winner = max(range(7), key=lambda i: (train_scores[i], -i))
        scores = [pbo_rankable_sharpe([row[i] for i in test]) for row in panel]
        lo = sum(x < scores[winner] for x in scores)
        equal = sum(x == scores[winner] for x in scores)
        rank = lo + (equal + 1) / 2
        relative = rank / 8
        logit = math.log(relative / (1 - relative))
        if not math.isfinite(logit):
            return 1.0
        overfit += logit <= 0
    return overfit / 70
