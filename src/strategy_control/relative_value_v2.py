"""Pure, no-I/O implementation primitives for frozen relative-value rotation v2.

This module accepts only caller supplied records.  It deliberately has no loader,
path, network, or market-data dependency; production validation supplies verified
buffers only after its separately authorised stage.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import combinations
from statistics import NormalDist, mean, median, stdev

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
DEVELOPMENT_FOLDS = (
    (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
)


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
    identity: str = ""

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
        # The boundary is a security boundary: never inspect an unretained suffix.
        # In particular, a corrupt future row cannot invalidate a finished fold.
        retained = tuple(row for row in rows if _utc(row.timestamp) < boundary)
        for row in retained:
            stamp = _utc(row.timestamp)
            if row.asset in previous and stamp <= previous[row.asset]:
                raise RelativeValueV2Error("duplicate or nonmonotonic minute rows")
            key = (row.asset, stamp)
            if key in seen:
                raise RelativeValueV2Error("duplicate minute row")
            previous[row.asset] = stamp
            seen.add(key)
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
    # Missing/nonfinite is ineligible, but malformed supplied raw input is never
    # silently ignored.  This is a copied-formula implementation of v1 decide().
    eligible = {
        asset: value
        for asset, value in scores.items()
        if value is not None and math.isfinite(value)
    }
    if not eligible:
        return CASH
    if set(raw_returns) != set(SYMBOLS):
        raise RelativeValueV2Error("invalid raw-return lookback")
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
    # This precedes winner selection in the preserved evaluator.  It is material:
    # held BTC with a nonpositive median exits even if ETH has the higher score.
    if actual in SYMBOLS and spec.cash_filter and median(raw_returns[actual]) <= 0:
        return CASH
    if btc == eth:
        return SYMBOLS[0] if actual == CASH and spec.tie_from_cash_btc else actual
    winner = SYMBOLS[0] if btc > eth else SYMBOLS[1]
    loser = SYMBOLS[1] if winner == SYMBOLS[0] else SYMBOLS[0]
    passes_cash = not spec.cash_filter or median(raw_returns[winner]) > 0
    if actual == CASH:
        return winner if eligible[winner] - eligible[loser] >= spec.gap and passes_cash else CASH
    if actual == winner:
        return winner if passes_cash else CASH
    return winner if eligible[winner] - eligible[actual] >= spec.gap and passes_cash else actual


@dataclass(frozen=True)
class ScoreRecord:
    """Auditable score evidence; identities are retained rather than summarized away."""

    raw_returns: tuple[float, ...]
    volatility: float | None
    score: float
    cutoff: datetime
    identities: tuple[str, ...]
    observation_count: int


def score_at(
    trial: str, observations: Mapping[str, Sequence[Observation]], index: int
) -> Mapping[str, ScoreRecord] | None:
    """Compute both scores from complete named close observations only.

    Each asset series is chronological completed-session closes.  The 20 simple
    returns and every horizon close are included in the retained identity set.
    """
    spec = TRIAL_SPECS.get(trial)
    if spec is None or set(observations) != set(SYMBOLS) or index < max(max(spec.horizons), 20):
        return None
    # A lookback is a joint daily panel, not two unrelated price series.  The
    # equality check is deliberately made before any arithmetic so an
    # asynchronous/missing session cannot become a synthetic signal.
    required = tuple(range(index - max(max(spec.horizons), 20), index + 1))
    for position in required:
        btc, eth = observations[SYMBOLS[0]][position], observations[SYMBOLS[1]][position]
        if (
            _utc(btc.event_at) != _utc(eth.event_at)
            or not btc.identity
            or not eth.identity
            or btc.identity == eth.identity
            or _utc(btc.available_at) < _utc(btc.event_at)
            or _utc(eth.available_at) < _utc(eth.event_at)
        ):
            return None
        if position > required[0] and _utc(btc.event_at) != _utc(
            observations[SYMBOLS[0]][position - 1].event_at
        ) + timedelta(days=1):
            return None
    output: dict[str, ScoreRecord] = {}
    for asset in SYMBOLS:
        values = observations[asset]
        if index >= len(values):
            return None
        window = [values[i] for i in required]
        if any(
            item.asset != asset or not math.isfinite(item.value) or not item.identity
            for item in window
        ):
            return None
        stamps = [_utc(item.event_at) for item in window]
        if any(stamps[i] <= stamps[i - 1] for i in range(1, len(stamps))):
            return None
        close = [item.value for item in values]
        raw = tuple(math.log(close[index] / close[index - h]) for h in spec.horizons)
        simple = [close[i] / close[i - 1] - 1.0 for i in range(index - 19, index + 1)]
        avg = sum(simple) / 20
        volatility = math.sqrt(sum((x - avg) ** 2 for x in simple) / 19)
        if any(not math.isfinite(x) for x in (*raw, volatility)) or (
            spec.risk_adjusted and volatility <= 0
        ):
            return None
        components = (
            tuple(x / (volatility * math.sqrt(h)) for x, h in zip(raw, spec.horizons, strict=True))
            if spec.risk_adjusted
            else raw
        )
        value = sum(components) / len(components)
        if not math.isfinite(value):
            return None
        cutoff = max(max(_utc(item.event_at), _utc(item.available_at)) for item in window)
        output[asset] = ScoreRecord(
            raw,
            volatility if spec.risk_adjusted else None,
            value,
            cutoff,
            tuple(x.identity for x in window),
            len(window),
        )
    return output


def decision_at(
    trial: str, observations: Mapping[str, Sequence[Observation]], index: int, actual: str
) -> tuple[str, Mapping[str, ScoreRecord]]:
    records = score_at(trial, observations, index)
    if records is None:
        return CASH, {}
    cutoff = max(record.cutoff for record in records.values())
    if any(record.cutoff > cutoff for record in records.values()):  # defensive invariant
        raise RelativeValueV2Error("inconsistent information cutoff")
    return decision_for_scores(
        trial,
        {a: records[a].score for a in SYMBOLS},
        {a: records[a].raw_returns for a in SYMBOLS},
        actual,
    ), records


@dataclass(frozen=True)
class QuarantineAction:
    target: str
    cancel_pending: bool
    requires_priced_liquidation: bool


def quarantine_action(actual: str, pending: str | None) -> QuarantineAction:
    if actual not in {CASH, *SYMBOLS} or pending not in {None, CASH, *SYMBOLS}:
        raise RelativeValueV2Error("invalid quarantine state")
    return QuarantineAction(CASH, pending is not None, actual != CASH)


@dataclass(frozen=True)
class DecisionTrace:
    """Complete non-default execution evidence for one canonical vector."""

    trial: str
    period: int
    decision_session_id: str
    due_session_id: str | None
    cutoff: datetime
    fill_timestamp: datetime
    row_ids: tuple[str, str]
    score_inputs: tuple[tuple[str, tuple[str, ...]], ...]
    raw_returns: tuple[tuple[str, tuple[float, ...]], ...]
    volatility: tuple[tuple[str, float | None], ...]
    scores: tuple[tuple[str, float | None], ...]
    desired: str
    actual_before: str
    actual_after: str
    pending_before: str | None
    pending_after: str | None
    disposition: str
    held_weights: tuple[tuple[str, float], ...]
    target_weights: tuple[tuple[str, float], ...]
    price_relatives: tuple[tuple[str, float], ...]
    gross_attribution: tuple[tuple[str, float], ...]
    turnover: float
    one_way_cost: float
    interval_return: float
    wealth: float
    quarantine_segment: int
    recovery_count: int
    regime: str
    terminal_cash_evidence: bool


def simulate_period(
    trial: str,
    observations: Mapping[str, Sequence[Observation]],
    vectors: Sequence[CanonicalVector],
    *,
    delayed: bool = False,
    cost_rate: float = ONE_WAY_COST,
) -> tuple[DecisionTrace, ...]:
    """Internally driven post-fill simulator; no caller can provide targets."""
    if (
        trial not in TRIAL_SPECS
        or not vectors
        or cost_rate not in (ONE_WAY_COST, DOUBLE_ONE_WAY_COST)
    ):
        raise RelativeValueV2Error("invalid simulator inputs")
    if any(vectors[i].timestamp <= vectors[i - 1].timestamp for i in range(1, len(vectors))):
        raise RelativeValueV2Error("vectors must be chronological")
    actual, pending, wealth, segment, recovery = CASH, None, 1.0, 0, RECOVERY_SESSIONS
    traces: list[DecisionTrace] = []
    for i, vector in enumerate(vectors):
        before, pending_before = actual, pending
        relatives = {
            a: 1.0 if i == 0 else vector.prices[a] / vectors[i - 1].prices[a] for a in SYMBOLS
        }
        if any(not math.isfinite(x) or x <= 0 for x in relatives.values()):
            raise RelativeValueV2Error("DATA_INTEGRITY unpriceable vector")
        # Accrue exact previous exposure first.  A gap has no bridge: an exposed
        # state is liquidated only on this first synchronized priced vector.
        gross = sum(float(before == a) * relatives[a] for a in SYMBOLS) + float(before == CASH)
        wealth *= gross
        due = str(i - 1) if delayed and i else None
        terminal = i == len(vectors) - 1
        records = score_at(trial, observations, i)
        complete = records is not None
        if not complete:
            segment += 1
            recovery = 0
            pending = None
            desired, actual, disposition = (
                CASH,
                CASH,
                "quarantine_priced_liquidation" if before != CASH else "quarantine_cash",
            )
        else:
            recovery = min(RECOVERY_SESSIONS, recovery + 1)
            if terminal:
                desired, actual, pending, disposition = (
                    CASH,
                    CASH,
                    None,
                    "terminal_cash_replaces_due" if delayed else "terminal_cash",
                )
            elif delayed:
                if pending is not None:
                    actual, pending = pending, None
                desired, _ = decision_at(trial, observations, i, actual)
                pending, disposition = desired, "queued_for_next_vector"
            else:
                desired, _ = decision_at(trial, observations, i, actual)
                actual, disposition = desired, "executed_at_current_vector"
        turnover = float(before != actual)
        cost = wealth * turnover * cost_rate
        wealth -= cost
        if wealth <= 0 or not math.isfinite(wealth):
            raise RelativeValueV2Error("DATA_INTEGRITY invalid wealth")
        evidence = records or {}
        cutoff = max((r.cutoff for r in evidence.values()), default=vector.timestamp)
        raw = tuple((a, evidence[a].raw_returns if evidence else ()) for a in SYMBOLS)
        trace = DecisionTrace(
            trial,
            i,
            str(i),
            due,
            cutoff,
            vector.timestamp,
            vector.row_ids,
            tuple((a, evidence[a].identities if evidence else ()) for a in SYMBOLS),
            raw,
            tuple((a, evidence[a].volatility if evidence else None) for a in SYMBOLS),
            tuple((a, evidence[a].score if evidence else None) for a in SYMBOLS),
            desired,
            before,
            actual,
            pending_before,
            pending,
            disposition,
            tuple(target_weights(before).items()),
            tuple(target_weights(actual).items()),
            tuple(relatives.items()),
            tuple((a, wealth / gross * float(before == a) * (relatives[a] - 1)) for a in SYMBOLS),
            turnover,
            cost,
            gross - 1 - turnover * cost_rate,
            wealth,
            segment,
            recovery,
            "eligible" if complete else "quarantine",
            terminal and actual == CASH and pending is None,
        )
        traces.append(trace)
    if actual != CASH or pending is not None or not traces[-1].terminal_cash_evidence:
        raise RelativeValueV2Error("terminal cash invariant")
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


def stationary_bootstrap(
    values: Sequence[float], *, resamples: int = 2000, block_length: int = 20
) -> Mapping[str, float | int]:
    """Frozen Politis--Romano circular stationary bootstrap (PCG64 seed)."""
    if (
        len(values) < 2
        or resamples < 1
        or block_length < 1
        or any(not math.isfinite(x) for x in values)
    ):
        raise RelativeValueV2Error("invalid frozen bootstrap input")
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover - validated installed dependency
        raise RelativeValueV2Error("numpy required for exact bootstrap") from exc
    rng = np.random.Generator(np.random.PCG64(V2_BOOTSTRAP_SEED))
    n, means, restart = len(values), [], 1.0 / block_length
    for _ in range(resamples):
        cursor, sample = int(rng.integers(n)), []
        for _ in range(n):
            sample.append(values[cursor])
            cursor = int(rng.integers(n)) if float(rng.random()) < restart else (cursor + 1) % n
        means.append(sum(sample) / n)
    means.sort()

    def percentile(q: float) -> float:
        position = (len(means) - 1) * q
        lo, hi = math.floor(position), math.ceil(position)
        return means[lo] if lo == hi else means[lo] + (means[hi] - means[lo]) * (position - lo)

    return {
        "seed": V2_BOOTSTRAP_SEED,
        "mean": sum(values) / n,
        "lower_95": percentile(0.025),
        "upper_95": percentile(0.975),
        "resamples": resamples,
    }


def _json_pointer(document: object, pointer: str) -> object:
    if not pointer.startswith("/"):
        raise RelativeValueV2Error("invalid JSON pointer")
    value = document
    for token in pointer[1:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        if isinstance(value, Mapping):
            value = value[token]
        elif isinstance(value, list):
            value = value[int(token)]
        else:
            raise RelativeValueV2Error("JSON pointer does not resolve")
    return value


@dataclass(frozen=True)
class MultiplicitySlot:
    name: str
    artifact_sha256: str | None
    pointer: str | None
    observed: bool


class MultiplicityRegistry:
    """Hash-bound, ordered 49-slot registry; never reads an artifact itself."""

    def __init__(self, slots: Sequence[MultiplicitySlot]) -> None:
        if len(slots) != 49 or len({slot.name for slot in slots}) != 49:
            raise RelativeValueV2Error("exact 49-slot multiplicity registry required")
        if sum(slot.observed for slot in slots) != 28:
            raise RelativeValueV2Error("registry must contain 28 observed and 21 absent slots")
        if any(
            slot.observed != (slot.artifact_sha256 is not None and slot.pointer is not None)
            for slot in slots
        ):
            raise RelativeValueV2Error("malformed multiplicity slot")
        self.slots = tuple(slots)

    def extract(
        self, artifacts: Mapping[str, bytes | str | Mapping[str, object]]
    ) -> tuple[float | None, ...]:
        result: list[float | None] = []
        for slot in self.slots:
            if not slot.observed:
                result.append(None)
                continue
            assert slot.artifact_sha256 is not None and slot.pointer is not None
            artifact = artifacts.get(slot.name)
            if artifact is None:
                raise RelativeValueV2Error("missing named multiplicity artifact")
            raw = (
                artifact.encode()
                if isinstance(artifact, str)
                else artifact
                if isinstance(artifact, bytes)
                else json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode()
            )
            if hashlib.sha256(raw).hexdigest() != slot.artifact_sha256:
                raise RelativeValueV2Error("multiplicity artifact hash mismatch")
            doc = json.loads(raw) if isinstance(artifact, (str, bytes)) else artifact
            value = _json_pointer(doc, slot.pointer)
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
            ):
                raise RelativeValueV2Error("nonfinite multiplicity value")
            result.append(float(value) / math.sqrt(365))
        return tuple(result)
