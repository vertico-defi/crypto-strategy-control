"""Fail-closed, development-only plumbing for the frozen calendar study.

This module intentionally has no parquet, exchange, clock, or network dependency.
Callers provide a partition opener and already decoded minute records.  The guard is
deliberately placed *before* the opener so a holdout path cannot even be inspected.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import PurePath
from typing import Any

from strategy_control.calendar_seasonality import (
    ASSETS,
    TRIALS,
    CalendarIntegrityError,
    JointVector,
    Portfolio,
    TrialSpec,
    deadline_for,
    exact_joint_vector,
    rebalance,
    schedule_for_interval,
)

DEVELOPMENT_START = datetime(2025, 1, 1, tzinfo=UTC)
DEVELOPMENT_END = datetime(2026, 1, 1, tzinfo=UTC)
OBSERVATION_START = datetime(2024, 7, 1, tzinfo=UTC)
FOLDS = (
    (DEVELOPMENT_START, datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), DEVELOPMENT_END),
)
PREREGISTRATION_SHA256 = "1e67b67cc3d89f9ddd6315cf35bc5c1e5b71353bdaf073a43dbf4b462025509e"
EFFECTIVE_CONTRACT_SHA256 = "102a0de94ded6d4c7fa7667702c07e8e8328c57feda5be597ec75d8efb8c7b96"
EFFECTIVE_CONTRACT_BYTE_SHA256 = "fe8accad9269272e6534a014462e672d827ebf440d60c864603dd0139acf1db6"
DATA_CONTRACT_SHA256 = "d2a02bca439359ca93bcb503bc5888fe4d6297b6f2115ac17c09d8da78f89183"
PARTITION_RE = re.compile(
    r"^canonical/venue=binance/symbol=(BTCUSDT|ETHUSDT)/"
    r"year=(2024|2025|2026)/month=(\d{2})/observations\.parquet$"
)
FROZEN_GATE_NAMES = frozenset(
    {
        "aggregate_net_return_gt",
        "positive_development_folds_minimum",
        "annualized_daily_net_sharpe_gte",
        "maximum_drawdown_lte",
        "doubled_cost_net_return_gt",
        "fifth_minute_delay_net_return_gt",
        "positive_parameter_neighbors_minimum",
        "bootstrap_all_three_lower_bounds_gt",
        "DSR_probability_gte",
        "within_family_PBO_lte",
        "baseline_superiority",
        "asset_sensitivity",
        "regime_gate",
        "leave_one_bucket_out",
        "sufficiency",
        "concentration",
        "data_integrity",
    }
)


class CalendarPipelineError(ValueError):
    """A frozen data, timing, or evaluation invariant was not met."""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise CalendarPipelineError("timestamp must be UTC")
    return value


def _finite_positive(value: float) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def canonical_json_hash(payload: Mapping[str, Any], omitted_field: str) -> str:
    """Hash a contract by its recorded compact, sorted JSON rule."""
    copied = dict(payload)
    copied.pop(omitted_field, None)
    return hashlib.sha256(
        json.dumps(copied, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def verify_preregistration(
    wrapper: Mapping[str, Any],
    effective: Mapping[str, Any],
    *,
    effective_bytes: bytes | None = None,
) -> None:
    """Verify both frozen hashes without loading any data source."""
    if wrapper.get("status") != "FROZEN":
        raise CalendarPipelineError("preregistration is not frozen")
    wrapper_hash = canonical_json_hash(wrapper, "preregistration_sha256")
    if wrapper_hash != wrapper.get("preregistration_sha256") or (
        wrapper_hash != PREREGISTRATION_SHA256
    ):
        raise CalendarPipelineError("wrapper self-hash mismatch")
    contract = wrapper.get("effective_contract", {})
    if canonical_json_hash(effective, "draft_sha256") != contract.get("canonical_sha256"):
        raise CalendarPipelineError("effective contract canonical hash mismatch")
    if contract.get("canonical_sha256") != EFFECTIVE_CONTRACT_SHA256:
        raise CalendarPipelineError("unexpected effective contract")
    if contract.get("byte_sha256") != EFFECTIVE_CONTRACT_BYTE_SHA256:
        raise CalendarPipelineError("unexpected effective-contract byte hash")
    if effective_bytes is not None and hashlib.sha256(effective_bytes).hexdigest() != (
        EFFECTIVE_CONTRACT_BYTE_SHA256
    ):
        raise CalendarPipelineError("effective-contract bytes mismatch")


def verify_data_contract(contract: Mapping[str, Any]) -> None:
    """Bind the reused fixed-pair contract before any partition is opened."""

    observed = hashlib.sha256(
        json.dumps(contract, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if observed != DATA_CONTRACT_SHA256:
        raise CalendarPipelineError("reused data-contract hash mismatch")
    if (
        contract.get("status") != "PASS"
        or contract.get("holdout_opened") is not False
        or contract.get("holdout_parquet_footers_or_values_read") is not False
        or contract.get("canonical_partition_count") != 48
    ):
        raise CalendarPipelineError("reused data contract is not eligible")


@dataclass(frozen=True)
class Partition:
    relative_path: str
    symbol: str
    month: str
    verification_scope: str


def development_partitions(contract: Mapping[str, Any]) -> tuple[Partition, ...]:
    """Select exactly the allowlisted 2024--25 metadata-only partitions."""

    verify_data_contract(contract)
    raw_partitions = contract.get("partitions")
    if not isinstance(raw_partitions, list) or len(raw_partitions) != 48:
        raise CalendarPipelineError("expected 48 frozen partitions")
    result: list[Partition] = []
    all_identities: set[tuple[str, int, int]] = set()
    for item in raw_partitions:
        if not isinstance(item, Mapping):
            raise CalendarPipelineError("malformed frozen partition")
        path = str(item.get("relative_path", ""))
        match = PARTITION_RE.fullmatch(path)
        if match is None:
            raise CalendarPipelineError("unexpected frozen partition path")
        symbol, year_text, month_text = match.groups()
        year, month = int(year_text), int(month_text)
        identity = (symbol, year, month)
        if identity in all_identities:
            raise CalendarPipelineError("duplicate frozen partition identity")
        all_identities.add(identity)
        if (
            item.get("symbol") != symbol
            or item.get("month") != f"{year:04d}-{month:02d}"
            or not 1 <= month <= 12
        ):
            raise CalendarPipelineError("partition identity metadata mismatch")
        scope = item.get("verification_scope")
        if year == 2026:
            if scope != "BYTE_HASH_ONLY_NO_PARQUET_PARSE":
                raise CalendarPipelineError("holdout partition scope mismatch")
            continue
        if scope != "HASH_AND_SCHEMA_METADATA_ONLY":
            raise CalendarPipelineError("development partition scope mismatch")
        result.append(
            Partition(
                path,
                symbol,
                str(item.get("month")),
                str(scope),
            )
        )
    result.sort(key=lambda item: (item.symbol, item.month, item.relative_path))
    if (
        len(result) != 36
        or {item.symbol for item in result} != set(ASSETS)
        or any(sum(item.symbol == symbol for item in result) != 18 for symbol in ASSETS)
    ):
        raise CalendarPipelineError("expected exactly 36 pre-2026 development partitions")
    if any("year=2026" in item.relative_path for item in result):
        raise CalendarPipelineError("holdout partition selected")
    return tuple(result)


def load_development_metadata(
    contract: Mapping[str, Any], opener: Callable[[str], Any]
) -> tuple[Any, ...]:
    """Open only selected development paths; callers may use this for metadata checks."""
    partitions = development_partitions(contract)
    # This second check must remain immediately adjacent to the only opener call.
    if any("year=2026" in item.relative_path for item in partitions):
        raise CalendarPipelineError("refusing holdout before opener")
    return tuple(open_development_partition(item.relative_path, opener) for item in partitions)


def open_development_partition(path: str, opener: Callable[[str], Any]) -> Any:
    """The only opening seam: reject a holdout label before touching the opener."""
    if "year=2026" in PurePath(path).parts or "year=2026" in path:
        raise CalendarPipelineError("refusing 2026 partition before open or footer parse")
    return opener(path)


@dataclass(frozen=True)
class MinuteRecord:
    symbol: str
    open_timestamp: datetime
    event_timestamp: datetime
    available_at: datetime
    open: float
    high: float
    low: float
    close: float

    def valid(self) -> bool:
        try:
            _utc(self.open_timestamp)
            _utc(self.event_timestamp)
            _utc(self.available_at)
        except CalendarPipelineError:
            return False
        return (
            self.symbol in ASSETS
            and self.open_timestamp.second == self.open_timestamp.microsecond == 0
            and self.event_timestamp == self.open_timestamp + timedelta(minutes=1)
            and self.available_at >= self.event_timestamp
            and all(_finite_positive(x) for x in (self.open, self.high, self.low, self.close))
            and self.low <= min(self.open, self.close)
            and max(self.open, self.close) <= self.high
        )


def _record_index(records: Iterable[MinuteRecord]) -> dict[tuple[str, datetime], MinuteRecord]:
    indexed: dict[tuple[str, datetime], MinuteRecord] = {}
    for row in records:
        if not row.valid():
            raise CalendarPipelineError("invalid minute observation")
        key = (row.symbol, row.open_timestamp)
        if key in indexed:
            raise CalendarPipelineError("duplicate valid minute observation")
        indexed[key] = row
    return indexed


def canonical_joint_vectors(records: Iterable[MinuteRecord]) -> tuple[JointVector, ...]:
    """Build only atomic BTC/ETH vectors at real minute labels."""
    indexed = _record_index(records)
    stamps = sorted({stamp for _, stamp in indexed})
    vectors = []
    for stamp in stamps:
        btc, eth = (indexed.get((asset, stamp)) for asset in ASSETS)
        if btc is not None and eth is not None:
            vectors.append(JointVector(stamp, float(btc.open), float(eth.open)))
    return tuple(vectors)


def exact_hour_vector(records: Iterable[MinuteRecord], hour: datetime) -> JointVector:
    hour = _utc(hour)
    if hour.minute or hour.second or hour.microsecond:
        raise CalendarPipelineError("hour must be exact")
    return exact_joint_vector(canonical_joint_vectors(records), hour)


def fifth_valid_vector(records: Iterable[MinuteRecord], hour: datetime) -> JointVector:
    """Return the fifth synchronized event strictly after ``hour``, before its end."""
    hour = _utc(hour)
    vectors = [
        v
        for v in canonical_joint_vectors(records)
        if hour < v.timestamp < hour + timedelta(hours=1)
    ]
    if len(vectors) < 5:
        raise CalendarPipelineError("delayed fill timed out")
    return vectors[4]


def causal_interval_valid(
    records: Iterable[MinuteRecord], start: datetime, refresh: datetime
) -> bool:
    """Require endpoints and all availability facts before the fitting refresh."""
    start, refresh = _utc(start), _utc(refresh)
    indexed = _record_index(records)
    end = start + timedelta(hours=1)
    parts = [indexed.get((asset, stamp)) for asset in ASSETS for stamp in (start, end)]
    return all(row is not None and row.available_at < refresh for row in parts)


def effective_schedule_hour(interval: datetime) -> datetime:
    return schedule_for_interval(interval)


def materialize_target(interval: datetime, materialized: datetime) -> None:
    if _utc(materialized) > deadline_for(_utc(interval)):
        raise CalendarPipelineError("decision missed frozen deadline")


def fold_source_prefix(records: Sequence[MinuteRecord], end: datetime) -> tuple[MinuteRecord, ...]:
    """Hard half-open source slice; use before any estimator/state-machine call."""
    end = _utc(end)
    return tuple(
        row for row in records if _utc(row.open_timestamp) < end and _utc(row.available_at) < end
    )


def fold_inputs(
    records: Sequence[MinuteRecord], start: datetime, end: datetime
) -> tuple[MinuteRecord, ...]:
    """Return an isolated fold prefix, including pre-start estimator warmup only."""
    start, end = _utc(start), _utc(end)
    if not DEVELOPMENT_START <= start < end <= DEVELOPMENT_END:
        raise CalendarPipelineError("fold crosses the frozen development boundary")
    return fold_source_prefix(records, end)


def terminal_cash(
    portfolio: Portfolio, records: Iterable[MinuteRecord], end: datetime
) -> Portfolio:
    """Liquidate using only C_(E-1h), never a post-end mark."""
    end = _utc(end)
    fill = exact_hour_vector(records, end - timedelta(hours=1))
    return rebalance(portfolio, (0.0, 0.0, 1.0), fill)


@dataclass(frozen=True)
class TrialRun:
    name: str
    daily_returns: Mapping[datetime, float]
    metrics: Mapping[str, Any]
    complete: bool = True


@dataclass(frozen=True)
class EvaluationResult:
    status: str
    trial_order: tuple[str, ...]
    common_daily_returns: Mapping[str, tuple[tuple[datetime, float], ...]]
    gates: Mapping[str, bool]
    failures: tuple[str, ...]
    performance_claim_permitted: bool


def common_daily_panel(runs: Sequence[TrialRun]) -> dict[str, tuple[tuple[datetime, float], ...]]:
    if len(runs) != 7:
        raise CalendarPipelineError("all seven frozen trials are required")
    common = set.intersection(*(set(run.daily_returns) for run in runs))
    return {
        run.name: tuple((day, run.daily_returns[day]) for day in sorted(common)) for run in runs
    }


def evaluate_development(
    records: Sequence[MinuteRecord],
    trial_evaluator: Callable[[TrialSpec, Sequence[MinuteRecord], datetime, datetime], TrialRun],
    *,
    gate_evaluator: Callable[
        [Mapping[str, TrialRun], Mapping[str, tuple[tuple[datetime, float], ...]]],
        Mapping[str, bool],
    ]
    | None = None,
    prior_dsr_records: Sequence[float] | None = None,
) -> EvaluationResult:
    """Run the immutable seven-trial order and fail closed on every absent gate.

    The injected evaluator is deliberately the integration seam for the frozen core;
    it receives only the pre-2026, half-open development prefix.
    """
    source = fold_source_prefix(records, DEVELOPMENT_END)
    expected = tuple(trial.name for trial in TRIALS)
    failures: list[str] = []
    runs: dict[str, TrialRun] = {}
    try:
        for trial in TRIALS:
            run = trial_evaluator(trial, source, DEVELOPMENT_START, DEVELOPMENT_END)
            if run.name != trial.name or not run.complete:
                raise CalendarPipelineError("incomplete or reordered trial result")
            runs[trial.name] = run
        panel = common_daily_panel(tuple(runs[name] for name in expected))
    except (CalendarPipelineError, CalendarIntegrityError, ValueError) as exc:
        return EvaluationResult("HISTORICAL_NO_GO", expected, {}, {}, (str(exc),), False)
    if prior_dsr_records is None or len(prior_dsr_records) != 28:
        failures.append("missing immutable 28-record DSR prior registry")
    if gate_evaluator is None:
        failures.append("missing frozen gate evaluator")
        gates: Mapping[str, bool] = {}
    else:
        try:
            gates = dict(gate_evaluator(runs, panel))
        except Exception as exc:  # an evaluator error is a terminal gate failure
            failures.append(f"gate evaluator failure: {exc}")
            gates = {}
    if not gates:
        failures.append("no frozen gates evaluated")
    missing_gates = FROZEN_GATE_NAMES - set(gates)
    if missing_gates:
        failures.append("missing frozen gates: " + ",".join(sorted(missing_gates)))
    failures.extend(name for name, passed in gates.items() if passed is not True)
    passed = not failures and all(gates.values())
    return EvaluationResult(
        "PASS" if passed else "HISTORICAL_NO_GO",
        expected,
        panel,
        gates,
        tuple(failures),
        False,
    )
