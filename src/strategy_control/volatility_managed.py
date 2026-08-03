"""Pure fail-closed primitives for the frozen equal-sleeve volatility study.

This module intentionally knows nothing about paths, parquet, exchanges, or market
data.  A future evaluator must supply already validated in-memory records.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from statistics import NormalDist, median
from typing import Any

EXPERIMENT_ID = "btc-eth-volatility-managed-equal-weight-v1"
SYMBOLS = ("BTCUSDT", "ETHUSDT")
CASH = "CASH"
BASE_COST = 0.0014
DOUBLED_COST = 0.0028
FIXED_LATCH_PATH = (
    "experiments/btc-eth-volatility-managed-equal-weight-v1/HOLDOUT_ACCESS_LATCH.json"
)
TARGET_KEYS = (
    "schema_version",
    "experiment_id",
    "path_kind",
    "trial_or_benchmark",
    "boundary_start",
    "boundary_end",
    "decision_session_end",
    "I_s",
    "B_s",
    "lookback",
    "target_volatility",
    "sigma_hat",
    "cap_state",
    "risky_scalar",
    "weights_BTC_ETH_cash",
    "ordered_source_record_ids",
    "ordered_source_record_hashes",
    "session_input_manifest_sha256",
    "status",
)
FILL_KEYS = (
    "schema_version",
    "experiment_id",
    "path_kind",
    "trial_or_benchmark",
    "boundary_start",
    "boundary_end",
    "decision_session_end",
    "B_s",
    "execution_event_timestamp",
    "execution_available_timestamp",
    "BTC_row_sha256",
    "ETH_row_sha256",
    "execution_vector_sha256",
    "parent_target_sha256",
    "pretrade_weights_BTC_ETH_cash",
    "target_weights_BTC_ETH_cash",
    "cost_rate",
    "currency_cost",
    "turnover",
    "gross_risky_trade",
    "status",
    "cancellation_reason",
)


class VolatilityManagedError(ValueError):
    """A frozen invariant failed; callers must not substitute a discretionary value."""


@dataclass(frozen=True)
class Trial:
    name: str
    lookback: int
    target: float
    biweekly: bool = False


TRIALS = (
    Trial("primary_target40_lookback60_weekly", 60, 0.4),
    Trial("neighbor_target30_lookback60_weekly", 60, 0.3),
    Trial("neighbor_target50_lookback60_weekly", 60, 0.5),
    Trial("neighbor_target40_lookback30_weekly", 30, 0.4),
    Trial("neighbor_target40_lookback90_weekly", 90, 0.4),
    Trial("target60_lookback60_weekly", 60, 0.6),
    Trial("target40_lookback60_biweekly", 60, 0.4, True),
)


@dataclass(frozen=True)
class Target:
    record: Mapping[str, Any]
    sha256: str


@dataclass(frozen=True)
class Fill:
    record: Mapping[str, Any]
    sha256: str


@dataclass(frozen=True)
class GapEvent:
    expected_event: datetime
    trigger: str
    detected_at: datetime


@dataclass(frozen=True)
class JointGridStatus:
    complete: bool
    triggers: tuple[GapEvent, ...]


@dataclass(frozen=True)
class PathState:
    wealth: float = 1.0
    weights: tuple[float, float, float] = (0.0, 0.0, 1.0)
    pending_target_hash: str | None = None
    quarantined: bool = False
    pnl: tuple[float, float] = (0.0, 0.0)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise VolatilityManagedError("timestamp must be explicit UTC")
    return value.astimezone(UTC)


def _finite(value: Any, label: str) -> float:
    if not isinstance(value, int | float) or not math.isfinite(value):
        raise VolatilityManagedError(f"{label} must be finite")
    return float(value)


def canonical_json(value: Any) -> bytes:
    """Frozen JSON encoding; recursively reject NaN, infinity, and non-string keys."""

    def check(item: Any) -> None:
        if isinstance(item, float) and not math.isfinite(item):
            raise VolatilityManagedError("canonical JSON forbids nonfinite float")
        if isinstance(item, Mapping):
            if any(not isinstance(key, str) for key in item):
                raise VolatilityManagedError("canonical JSON requires string keys")
            for child in item.values():
                check(child)
        elif isinstance(item, list | tuple):
            for child in item:
                check(child)

    check(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def validate_allowlist(
    source_commit: str,
    expected_commit: str,
    entries: Sequence[Mapping[str, Any]],
    expected_hash: str,
) -> None:
    """Validate identity before any caller is allowed to resolve an input path."""
    if source_commit != expected_commit or len(entries) != 36:
        raise VolatilityManagedError("source commit or exact 36-entry allowlist mismatch")
    if canonical_hash(list(entries)) != expected_hash:
        raise VolatilityManagedError("allowlist hash mismatch")
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        if set(entry) != {"bytes", "month", "relative_path", "sha256", "symbol"}:
            raise VolatilityManagedError("invalid allowlist entry schema")
        identity = (str(entry["symbol"]), str(entry["month"]))
        if identity in seen or entry["symbol"] not in SYMBOLS or int(entry["bytes"]) <= 0:
            raise VolatilityManagedError("duplicate or invalid allowlist identity")
        if len(str(entry["sha256"])) != 64 or not str(entry["relative_path"]).startswith(
            "canonical/"
        ):
            raise VolatilityManagedError("invalid allowlist content identity")
        seen.add(identity)


def verify_opened_bytes(raw: bytes, expected_size: int, expected_sha256: str) -> None:
    if len(raw) != expected_size or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise VolatilityManagedError("opened byte identity mismatch before parse")


def session_manifest(rows: Sequence[Mapping[str, Any]]) -> str:
    required = {
        "relative_path",
        "file_sha256",
        "row_identifier",
        "event_timestamp",
        "available_timestamp",
        "row_hash",
    }
    if any(set(row) != required for row in rows):
        raise VolatilityManagedError("invalid session input manifest schema")
    return canonical_hash(list(rows))


def expected_grid(start: datetime) -> tuple[datetime, ...]:
    start = _utc(start)
    if start.time().isoformat() != "00:00:00":
        raise VolatilityManagedError("session start must be midnight")
    return tuple(start + timedelta(minutes=index) for index in range(1, 1441))


def causal_gap_event(
    expected: datetime, later_available: Sequence[datetime], boundary: datetime
) -> GapEvent:
    expected, boundary = _utc(expected), _utc(boundary)
    later = [_utc(value) for value in later_available if _utc(value) >= expected]
    detected = min(later) if later else boundary
    return GapEvent(expected, "missing_expected_row", max(expected, detected))


def joint_grid_status(
    start: datetime,
    btc_events: Sequence[datetime],
    eth_events: Sequence[datetime],
    boundary: datetime,
    *,
    btc_available: Sequence[datetime] | None = None,
    eth_available: Sequence[datetime] | None = None,
    btc_valid: Sequence[bool] | None = None,
    eth_valid: Sequence[bool] | None = None,
) -> JointGridStatus:
    """Compare both ordered streams with the prebuilt grid without hiding absences."""

    expected = expected_grid(start)
    boundary = _utc(boundary)
    normalized = [tuple(_utc(value) for value in values) for values in (btc_events, eth_events)]
    available: list[tuple[datetime, ...]] = []
    valid: list[tuple[bool, ...]] = []
    for events, supplied_available, supplied_valid in zip(
        normalized, (btc_available, eth_available), (btc_valid, eth_valid), strict=True
    ):
        event_available = (
            events
            if supplied_available is None
            else tuple(_utc(value) for value in supplied_available)
        )
        event_valid = (
            tuple(True for _ in events)
            if supplied_valid is None
            else tuple(bool(value) for value in supplied_valid)
        )
        if len(event_available) != len(events) or len(event_valid) != len(events):
            raise VolatilityManagedError("joint row evidence length mismatch")
        if any(seen < event for event, seen in zip(events, event_available, strict=True)):
            raise VolatilityManagedError("availability precedes event")
        available.append(event_available)
        valid.append(event_valid)

    triggers: list[GapEvent] = []
    for symbol, events, availability, validity in zip(
        SYMBOLS, normalized, available, valid, strict=True
    ):
        if len(events) != len(set(events)):
            duplicate_index = next(
                index for index, value in enumerate(events) if value in events[:index]
            )
            triggers.append(
                GapEvent(
                    events[duplicate_index],
                    f"{symbol}_duplicate",
                    availability[duplicate_index],
                )
            )
        if any(right <= left for left, right in itertools.pairwise(events)):
            offending_index = next(
                index
                for index, (left, right) in enumerate(itertools.pairwise(events), start=1)
                if right <= left
            )
            triggers.append(
                GapEvent(
                    events[offending_index],
                    f"{symbol}_nonmonotonic",
                    availability[offending_index],
                )
            )
        for event, seen, is_valid in zip(events, availability, validity, strict=True):
            if not is_valid:
                triggers.append(GapEvent(event, f"{symbol}_invalid", seen))
        observed = {event for event, is_valid in zip(events, validity, strict=True) if is_valid}
        for missing in (value for value in expected if value not in observed):
            later_available = [
                seen
                for other_events, other_available in zip(normalized, available, strict=True)
                for event, seen in zip(other_events, other_available, strict=True)
                if event >= missing and seen >= missing
            ]
            triggers.append(
                GapEvent(
                    missing,
                    f"{symbol}_missing_or_invalid",
                    max(missing, min(later_available)) if later_available else boundary,
                )
            )
    for index, (btc, eth) in enumerate(zip(normalized[0], normalized[1], strict=False)):
        if btc != eth:
            detected = max(available[0][index], available[1][index])
            triggers.append(GapEvent(min(btc, eth), "asynchronous_joint_row", detected))
            break
    ordered = tuple(sorted(set(triggers), key=lambda event: (event.detected_at, event.trigger)))
    return JointGridStatus(not ordered, ordered)


def exact_vector(
    expected_open: datetime, btc: Mapping[datetime, float], eth: Mapping[datetime, float]
) -> tuple[float, float]:
    expected_open = _utc(expected_open)
    if expected_open not in btc or expected_open not in eth:
        raise VolatilityManagedError("missing exact B_s; ordinary fills never scan later")
    return (_positive(btc[expected_open], "BTC price"), _positive(eth[expected_open], "ETH price"))


def _positive(value: Any, label: str) -> float:
    result = _finite(value, label)
    if result <= 0:
        raise VolatilityManagedError(f"{label} must be positive")
    return result


def safety_vector(
    trigger: datetime, vectors: Sequence[tuple[datetime, datetime, float, float]]
) -> tuple[datetime, datetime, float, float]:
    trigger = _utc(trigger)
    choices = [
        (_utc(event), _utc(available), _positive(btc, "BTC price"), _positive(eth, "ETH price"))
        for event, available, btc, eth in vectors
        if _utc(event) > trigger and _utc(available) > trigger
    ]
    if not choices:
        raise VolatilityManagedError("unpriceable exposed path before terminal")
    return min(choices, key=lambda row: (row[0], row[1]))


def sleeve_scalar(
    returns: Sequence[tuple[float, float]], trial: Trial = TRIALS[0]
) -> tuple[float, tuple[float, float, float]]:
    if len(returns) != trial.lookback:
        raise VolatilityManagedError("exact contiguous lookback required")
    sleeve = [
        _finite(
            (0.5 * _finite(a, "BTC return")) + (0.5 * _finite(b, "ETH return")), "sleeve return"
        )
        for a, b in returns
    ]
    mean = sum(sleeve) / len(sleeve)
    sigma = math.sqrt(sum((value - mean) ** 2 for value in sleeve) / (len(sleeve) - 1)) * math.sqrt(
        365
    )
    if not math.isfinite(sigma) or sigma <= 0:
        raise VolatilityManagedError("scheduled volatility numerical degeneracy")
    scalar = trial.target / sigma
    if not math.isfinite(scalar):
        raise VolatilityManagedError("scheduled scalar numerical degeneracy")
    scalar = min(1.0, max(0.0, scalar))
    weights = (0.5 * scalar, 0.5 * scalar, 1.0 - scalar)
    if (
        not all(math.isfinite(x) and x >= 0 for x in weights)
        or abs(sum(weights) - 1) > 1e-12
        or weights[0] != weights[1]
    ):
        raise VolatilityManagedError("invalid equal-sleeve target")
    return sigma, weights


def first_open_after(information_time: datetime) -> datetime:
    value = _utc(information_time)
    return value.replace(second=0, microsecond=0) + timedelta(minutes=1)


def make_target(record: Mapping[str, Any]) -> Target:
    if set(record) != set(TARGET_KEYS) or len(record) != len(TARGET_KEYS):
        raise VolatilityManagedError("target schema mismatch")
    if record.get("experiment_id") != EXPERIMENT_ID:
        raise VolatilityManagedError("target experiment mismatch")
    weights = record.get("weights_BTC_ETH_cash")
    source_ids = record.get("ordered_source_record_ids")
    source_hashes = record.get("ordered_source_record_hashes")
    if (
        not isinstance(weights, Sequence)
        or isinstance(weights, str | bytes)
        or len(weights) != 3
        or any(_finite(value, "target weight") < 0 for value in weights)
        or abs(sum(float(value) for value in weights) - 1) > 1e-12
        or float(weights[0]) != float(weights[1])
        or not isinstance(source_ids, Sequence)
        or isinstance(source_ids, str | bytes)
        or not isinstance(source_hashes, Sequence)
        or isinstance(source_hashes, str | bytes)
        or len(source_ids) != len(source_hashes)
        or len(str(record.get("session_input_manifest_sha256"))) != 64
    ):
        raise VolatilityManagedError("invalid target evidence trace")
    return Target(dict(record), canonical_hash(record))


def make_fill(record: Mapping[str, Any], target: Target) -> Fill:
    if (
        set(record) != set(FILL_KEYS)
        or len(record) != len(FILL_KEYS)
        or record.get("parent_target_sha256") != target.sha256
    ):
        raise VolatilityManagedError("fill parent target hash mismatch")
    for key in ("BTC_row_sha256", "ETH_row_sha256", "execution_vector_sha256"):
        if len(str(record.get(key))) != 64:
            raise VolatilityManagedError("invalid fill evidence trace")
    return Fill(dict(record), canonical_hash(record))


def cancel_pending(
    state: PathState, *, quarantine: bool = False, terminal: bool = False
) -> PathState:
    return PathState(state.wealth, state.weights, None, quarantine or state.quarantined, state.pnl)


def fresh_path() -> PathState:
    return PathState()


def fold_state(start: datetime, prestart_warmup: Sequence[datetime]) -> PathState:
    start = _utc(start)
    if any(_utc(row) >= start for row in prestart_warmup):
        raise VolatilityManagedError("warmup is not strictly pre-fold")
    return fresh_path()


def strict_prefix(
    rows: Sequence[tuple[datetime, Any]], end: datetime
) -> tuple[tuple[datetime, Any], ...]:
    end = _utc(end)
    return tuple(((_utc(t)), value) for t, value in rows if _utc(t) < end)


def common_consecutive_endpoints(
    panels: Sequence[Mapping[datetime, float]],
) -> tuple[datetime, ...]:
    if len(panels) != 7:
        raise VolatilityManagedError("exact seven trial panels required")
    common = set(panels[0])
    for panel in panels[1:]:
        common &= set(panel)
    ordered = tuple(sorted(common))
    if any(
        not math.isfinite(_finite(panel[t], "endpoint wealth")) for panel in panels for t in ordered
    ):
        raise VolatilityManagedError("nonfinite endpoint")
    return ordered


def consecutive_returns(
    panel: Mapping[datetime, float], endpoints: Sequence[datetime]
) -> tuple[float, ...]:
    output: list[float] = []
    for left, right in itertools.pairwise(endpoints):
        if _utc(right) - _utc(left) == timedelta(days=1):
            output.append(
                _positive(panel[right], "endpoint wealth")
                / _positive(panel[left], "endpoint wealth")
                - 1
            )
    return tuple(output)


def baseline_all_six(
    primary: Mapping[str, float], weekly: Mapping[str, float], buy_hold: Mapping[str, float]
) -> bool:
    keys = ("net", "sharpe", "drawdown")
    try:
        if any(
            not math.isfinite(_finite(row[key], key))
            for row in (primary, weekly, buy_hold)
            for key in keys
        ):
            return False
    except (KeyError, VolatilityManagedError):
        return False
    return (
        primary["net"] > 0
        and primary["net"] > weekly["net"]
        and primary["sharpe"] > weekly["sharpe"]
        and primary["sharpe"] > buy_hold["sharpe"]
        and primary["drawdown"] < weekly["drawdown"]
        and primary["drawdown"] < buy_hold["drawdown"]
    )


def event_drawdown(wealth: Sequence[float]) -> float:
    if not wealth:
        raise VolatilityManagedError("empty event path")
    peak = 0.0
    result = 0.0
    for value in wealth:
        current = _positive(value, "event wealth")
        peak = max(peak, current)
        result = max(result, 1 - current / peak)
    return result


def trade_metrics(
    prior: Sequence[float], target: Sequence[float], wealth: float, cost_rate: float
) -> Mapping[str, float]:
    if len(prior) != 3 or len(target) != 3:
        raise VolatilityManagedError("three weights required")
    prior = tuple(_finite(value, "prior weight") for value in prior)
    target = tuple(_finite(value, "target weight") for value in target)
    if (
        any(value < 0 for value in (*prior, *target))
        or abs(sum(prior) - 1) > 1e-12
        or abs(sum(target) - 1) > 1e-12
    ):
        raise VolatilityManagedError("weights do not sum to one")
    gross = abs(target[0] - prior[0]) + abs(target[1] - prior[1])
    turnover = 0.5 * sum(abs(a - b) for a, b in zip(prior, target, strict=True))
    rate = _finite(cost_rate, "cost rate")
    cost = _positive(wealth, "wealth") * rate * gross
    if rate < 0 or not math.isfinite(cost) or cost >= wealth:
        raise VolatilityManagedError("invalid cost")
    return {"turnover": turnover, "gross_risky_trade": gross, "currency_cost": cost}


def reconcile_contributions(initial: float, terminal: float, btc: float, eth: float) -> None:
    diff = _finite(terminal, "terminal") - _finite(initial, "initial")
    if abs(_finite(btc, "btc") + _finite(eth, "eth") - diff) > 1e-10 * max(1.0, abs(diff)):
        raise VolatilityManagedError("contribution reconciliation failure")


def validate_registry(
    prior_sharpes: Sequence[float], current_sharpes: Sequence[float], calendar_slots: int = 7
) -> tuple[float, ...]:
    if len(prior_sharpes) != 28 or len(current_sharpes) != 7 or calendar_slots != 7:
        raise VolatilityManagedError("42-attempt registry must have 35 observed and 7 slots")
    values = tuple(map(float, (*prior_sharpes, *current_sharpes)))
    if any(not math.isfinite(x) for x in values):
        raise VolatilityManagedError("invalid observed sharpes")
    return values


def stationary_bootstrap(
    values: Sequence[float], block: int, resamples: int = 2000
) -> Mapping[str, Any]:
    if (
        len(values) < 2
        or block not in (10, 20, 40)
        or resamples <= 0
        or any(not math.isfinite(float(x)) for x in values)
    ):
        raise VolatilityManagedError("bootstrap degeneracy")
    seed = int.from_bytes(
        hashlib.sha256(f"{EXPERIMENT_ID}|stationary-bootstrap|{block}".encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    n = len(values)
    means = []
    for _ in range(resamples):
        i = rng.randrange(n)
        sample = [float(values[i])]
        for _ in range(1, n):
            i = rng.randrange(n) if rng.random() < 1 / block else (i + 1) % n
            sample.append(float(values[i]))
        means.append(sum(sample) / n)
    means.sort()

    def linear_percentile(q: float) -> float:
        rank = q * (len(means) - 1)
        lo = math.floor(rank)
        hi = math.ceil(rank)
        return means[lo] + (means[hi] - means[lo]) * (rank - lo)

    return {"seed": seed, "lower": linear_percentile(0.025), "upper": linear_percentile(0.975)}


def _sharpe(values: Sequence[float]) -> float:
    if len(values) < 2 or any(not math.isfinite(float(x)) for x in values):
        raise VolatilityManagedError("sharpe degeneracy")
    mean = sum(values) / len(values)
    sd = math.sqrt(sum((x - mean) ** 2 for x in values) / (len(values) - 1))
    if sd == 0:
        return math.inf if mean > 0 else -math.inf
    return mean / sd


def _bias_false_moments(values: Sequence[float]) -> tuple[float, float]:
    """Return bias-corrected sample skew and Pearson (nonexcess) kurtosis."""

    n = len(values)
    if n < 4:
        raise VolatilityManagedError("insufficient observations for moments")
    mean = sum(values) / n
    centered = [float(value) - mean for value in values]
    m2 = sum(value**2 for value in centered) / n
    if not math.isfinite(m2) or m2 <= 0:
        raise VolatilityManagedError("moment variance degeneracy")
    m3 = sum(value**3 for value in centered) / n
    m4 = sum(value**4 for value in centered) / n
    skew = math.sqrt(n * (n - 1)) / (n - 2) * (m3 / (m2**1.5))
    excess = (n - 1) / ((n - 2) * (n - 3)) * ((n + 1) * (m4 / (m2**2) - 3) + 6)
    kurtosis = excess + 3
    if not math.isfinite(skew) or not math.isfinite(kurtosis):
        raise VolatilityManagedError("invalid moments")
    return skew, kurtosis


def deflated_sharpe(primary: Sequence[float], registry: Sequence[float]) -> float:
    try:
        values = validate_registry(registry[:28], registry[28:])
        observed = _sharpe(primary)
        mean = sum(values) / 35
        sigma = math.sqrt(sum((x - mean) ** 2 for x in values) / 34)
        if sigma <= 0 or len(primary) < 30 or not math.isfinite(observed):
            return 0.0
        centered = [x - sum(primary) / len(primary) for x in primary]
        denom = sum(x * x for x in centered)
        if denom <= 0:
            return 0.0
        rhos = [
            sum(centered[i] * centered[i - k] for i in range(k, len(centered))) / denom
            for k in range(1, 29)
        ]
        vif = max(1.0, 1 + 2 * sum((1 - (k + 1) / 29) * rho for k, rho in enumerate(rhos)))
        teff = len(primary) / vif
        if teff < 30:
            return 0.0
        sr0 = sigma * (
            (1 - 0.5772156649) * NormalDist().inv_cdf(1 - 1 / 42)
            + 0.5772156649 * NormalDist().inv_cdf(1 - 1 / (42 * math.e))
        )
        skew, kurtosis = _bias_false_moments(primary)
        correction = 1 - skew * observed + ((kurtosis - 1) / 4) * observed**2
        if not math.isfinite(correction) or correction <= 0:
            return 0.0
        z = (observed - sr0) * math.sqrt(teff - 1) / math.sqrt(correction)
        return NormalDist().cdf(z) if math.isfinite(z) else 0.0
    except (VolatilityManagedError, ValueError, ZeroDivisionError):
        return 0.0


def pbo(matrix: Sequence[Sequence[float]]) -> float:
    try:
        if (
            len(matrix) != 7
            or len({len(row) for row in matrix}) != 1
            or len(matrix[0]) < 8
            or any(not math.isfinite(float(x)) for row in matrix for x in row)
        ):
            return 1.0
        count = len(matrix[0])
        quotient, remainder = divmod(count, 8)
        sizes = [quotient + 1] * remainder + [quotient] * (8 - remainder)
        blocks: list[list[int]] = []
        cursor = 0
        for size in sizes:
            blocks.append(list(range(cursor, cursor + size)))
            cursor += size
        if any(not b for b in blocks):
            return 1.0
        events = 0
        for train_blocks in itertools.combinations(range(8), 4):
            train = [i for b in train_blocks for i in blocks[b]]
            test = [i for b in range(8) if b not in train_blocks for i in blocks[b]]
            scores = [_sharpe([row[i] for i in train]) for row in matrix]
            selected = max(range(7), key=lambda i: scores[i])
            test_scores = [_sharpe([row[i] for i in test]) for row in matrix]
            selected_score = test_scores[selected]
            rank = (
                1
                + sum(x < selected_score for x in test_scores)
                + (sum(x == selected_score for x in test_scores) - 1) / 2
            )
            if math.log((rank / 8) / (1 - rank / 8)) <= 0:
                events += 1
        return events / 70
    except (VolatilityManagedError, ValueError, ZeroDivisionError):
        return 1.0


def regime_labels(closes: Sequence[float | None]) -> tuple[str | None, ...]:
    prior_returns: list[float] = []
    vols: list[float] = []
    clean: list[float] = []
    result: list[str | None] = []
    for close in closes:
        if close is None:
            prior_returns = []
            vols = []
            clean = []
            result.append(None)
            continue
        close = _positive(close, "close")
        clean.append(close)
        if len(clean) > 1:
            prior_returns.append(clean[-1] / clean[-2] - 1)
        vol = None
        if len(prior_returns) >= 60:
            s = prior_returns[-60:]
            m = sum(s) / 60
            vol = math.sqrt(sum((x - m) ** 2 for x in s) / 59)
        result.append(
            ("up" if close / clean[-121] - 1 > 0 else "down")
            + "_"
            + ("high" if vol > median(vols) else "low")
            if len(clean) >= 121 and vol is not None and len(vols) >= 120
            else None
        )
        if vol is not None:
            vols.append(vol)
    return tuple(result)


def assign_regime_currency_pnl(
    labels: Sequence[str | None],
    price_pnl: Sequence[float],
    costs: Sequence[float],
    safety_pnl: Sequence[float],
) -> Mapping[str, float]:
    if not (len(labels) == len(price_pnl) == len(costs) == len(safety_pnl)):
        raise VolatilityManagedError("regime assignment length mismatch")
    totals: dict[str, float] = {}
    for label, gross, cost, safety in zip(labels, price_pnl, costs, safety_pnl, strict=True):
        if label is None:
            continue
        value = _finite(gross, "price pnl") - _finite(cost, "cost") + _finite(
            safety, "safety pnl"
        )
        totals[label] = totals.get(label, 0.0) + value
    return totals


def holdout_minima(*, holdout: bool) -> Mapping[str, int]:
    return (
        {"days": 150, "total_rebalances": 20, "per_fold": 8, "positive_folds": 2}
        if holdout
        else {"days": 320, "total_rebalances": 40, "per_fold": 8, "positive_folds": 3}
    )


def validate_latch(
    latch: Mapping[str, Any],
    authorization: Mapping[str, str],
    *,
    path: str = FIXED_LATCH_PATH,
    post_arm_error: bool = False,
) -> None:
    if (
        path != FIXED_LATCH_PATH
        or latch.get("schema_version") != "1.0"
        or latch.get("experiment_id") != EXPERIMENT_ID
        or latch.get("authorization_hashes") != dict(authorization)
        or latch.get("accessed") is not True
        or not isinstance(latch.get("first_access_at_utc"), str)
        or post_arm_error
    ):
        raise VolatilityManagedError("holdout latch terminal failure")


def validate_latch_creation(*, path_exists: bool, path: str = FIXED_LATCH_PATH) -> None:
    if path != FIXED_LATCH_PATH or path_exists:
        raise VolatilityManagedError("holdout latch terminal failure")


def prospective_keys(sundays: Sequence[datetime]) -> tuple[str, ...]:
    seen: set[str] = set()
    output = []
    for value in sundays:
        value = _utc(value)
        if value.weekday() != 6:
            raise VolatilityManagedError("prospective decision is not Sunday")
        key = f"{EXPERIMENT_ID}|{value.isoformat()}"
        if key not in seen:
            seen.add(key)
            output.append(key)
    return tuple(output)
