"""Fail-closed, non-economic production adapter for frozen mean-reversion v2."""

from __future__ import annotations

import hashlib
import importlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any

from strategy_control.mean_reversion_v2 import (
    ASSETS,
    MeanReversionV2Error,
    canonical_hash,
    causal_gap_segments,
    guard_development_relative_path,
)

REUSED_CONTRACT_CANONICAL_SHA256 = (
    "d2a02bca439359ca93bcb503bc5888fe4d6297b6f2115ac17c09d8da78f89183"
)
REUSED_CONTRACT_BYTE_SHA256 = "47dcf7e5bec24ec73be45cdeb200e25671807e7a28c3c1204499a4e20ae972b7"
SOURCE_COMMIT = "d1d6066a6042b0c2e1c6af75047f5ebf935c739f"
FREEZE_MANIFEST_SHA256 = "243d875979df2991ef3c941d06e13d608c30e44df0eab512afdbb3fb6b0a07ad"
CANONICAL_INVENTORY_SHA256 = "1a5d5eaab533892b17b296750a2750514e51455d503456285b31b1845e63cc2f"
ALLOWLIST_SHA256 = "40bb5cf5b7bd3a8ac30e2a3b1d022462fe45888790b1ba58a7068a1982cdc6bd"
ALLOWLIST_COUNT = 36
PARQUET_COLUMNS = [
    "event_timestamp",
    "available_timestamp",
    "source_provenance",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


class ProductionIntegrationError(MeanReversionV2Error):
    """A production identity, parser, or causal-input invariant failed."""


@dataclass(frozen=True)
class AllowlistEntry:
    bytes: int
    month: str
    relative_path: str
    sha256: str
    symbol: str


@dataclass(frozen=True)
class VerifiedBuffer:
    entry: AllowlistEntry
    payload: bytes


@dataclass(frozen=True)
class MinuteRow:
    relative_path: str
    file_sha256: str
    row_index: int
    source_provenance: str
    event_timestamp: datetime
    available_timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def identity(self) -> str:
        return canonical_hash(
            {
                "relative_path": self.relative_path,
                "file_sha256": self.file_sha256,
                "row_index": self.row_index,
                "source_provenance": self.source_provenance,
                "event_timestamp": self.event_timestamp,
                "available_timestamp": self.available_timestamp,
            }
        )


@dataclass(frozen=True)
class ProductionRowIndex:
    """Immutable exact row maps for one strict half-open production boundary."""

    boundary: datetime
    rows_by_asset: Mapping[str, Mapping[datetime, MinuteRow]]
    session_rows_by_asset: Mapping[str, Mapping[datetime, Mapping[datetime, MinuteRow]]]
    retained_row_count: int


@dataclass(frozen=True)
class JointSession:
    session: datetime
    complete: bool
    information_cutoff: datetime | None
    closes: Mapping[str, float]
    segment: int | None

    @property
    def identity(self) -> str:
        return canonical_hash(
            {
                "session": self.session,
                "complete": self.complete,
                "information_cutoff": self.information_cutoff,
                "closes": self.closes,
                "segment": self.segment,
            }
        )


@dataclass(frozen=True)
class FillIdentity:
    session: datetime
    fill_index: int
    base_timestamp: datetime
    delayed_timestamp: datetime | None
    base_prices: Mapping[str, float]
    base_row_identities: Mapping[str, str]
    delayed_prices: Mapping[str, float]
    delayed_row_identities: Mapping[str, str]

    @property
    def identity(self) -> str:
        return canonical_hash(
            {
                "session": self.session,
                "fill_index": self.fill_index,
                "base_timestamp": self.base_timestamp,
                "delayed_timestamp": self.delayed_timestamp,
                "base_prices": self.base_prices,
                "base_row_identities": self.base_row_identities,
                "delayed_prices": self.delayed_prices,
                "delayed_row_identities": self.delayed_row_identities,
            }
        )


@dataclass(frozen=True)
class MechanicalEvidence:
    row_manifest: str
    session_manifest: str
    fill_manifest: str
    trace_manifest: str
    cost_manifest: str
    representative_return_manifest: str


@dataclass(frozen=True)
class RepresentativeAccounting:
    units: Mapping[str, float]
    cash: float
    prior_prices: Mapping[str, float]
    current_prices: Mapping[str, float]
    target_weights: Mapping[str, float]
    cost_rate: float


@dataclass(frozen=True)
class RepresentativeAccountingReconciliation:
    prior_postcost_equity: float
    pretrade_equity: float
    turnover: float
    cost: float
    postcost_equity: float
    units: Mapping[str, float]
    cash: float
    interval_return: float
    identity: str


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ProductionIntegrationError(f"{field} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_json_hash(value: bytes) -> str:
    try:
        parsed = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProductionIntegrationError("contract is not valid UTF-8 JSON") from error
    return _sha256(json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def verify_source_identity(
    *,
    contract_bytes: bytes,
    source_commit: str,
    freeze_manifest_sha256: str,
    inventory_sha256: str,
    entries: Sequence[AllowlistEntry],
) -> tuple[AllowlistEntry, ...]:
    """Verify all global identity claims before any selected path is resolved."""
    if (
        _sha256(contract_bytes) != REUSED_CONTRACT_BYTE_SHA256
        or _canonical_json_hash(contract_bytes) != REUSED_CONTRACT_CANONICAL_SHA256
        or source_commit != SOURCE_COMMIT
        or freeze_manifest_sha256 != FREEZE_MANIFEST_SHA256
        or inventory_sha256 != CANONICAL_INVENTORY_SHA256
    ):
        raise ProductionIntegrationError("source contract identity rejected before resolution")
    normalized: list[AllowlistEntry] = []
    for entry in entries:
        if entry.symbol not in ASSETS or entry.bytes < 1 or len(entry.sha256) != 64:
            raise ProductionIntegrationError("invalid allowlist entry before resolution")
        guard_development_relative_path(entry.relative_path)
        normalized.append(entry)
    serialized = [
        {
            "bytes": item.bytes,
            "month": item.month,
            "relative_path": item.relative_path,
            "sha256": item.sha256,
            "symbol": item.symbol,
        }
        for item in normalized
    ]
    if len(normalized) != ALLOWLIST_COUNT or canonical_hash(serialized) != ALLOWLIST_SHA256:
        raise ProductionIntegrationError(
            "36-entry development allowlist rejected before resolution"
        )
    return tuple(normalized)


def verify_entry_buffer(entry: AllowlistEntry, payload: bytes) -> VerifiedBuffer:
    guard_development_relative_path(entry.relative_path)
    if len(payload) != entry.bytes or _sha256(payload) != entry.sha256:
        raise ProductionIntegrationError("source byte count or SHA-256 mismatch before parse")
    return VerifiedBuffer(entry, payload)


def read_verified_entry(root: Path, entry: AllowlistEntry) -> VerifiedBuffer:
    relative = guard_development_relative_path(entry.relative_path)
    candidate = root.joinpath(*relative.split("/"))
    if not candidate.is_file():
        raise ProductionIntegrationError("allowlisted path is not a regular file")
    return verify_entry_buffer(entry, candidate.read_bytes())


def _schema_names(table: Any) -> tuple[str, ...]:
    names = getattr(getattr(table, "schema", None), "names", None)
    if not isinstance(names, (list, tuple)):
        raise ProductionIntegrationError("PyArrow result has no schema names")
    return tuple(names)


def _validate_arrow_schema(table: Any) -> None:
    if _schema_names(table) != tuple(PARQUET_COLUMNS):
        raise ProductionIntegrationError("PyArrow schema column order mismatch")
    field = getattr(table.schema, "field", None)
    if not callable(field):
        raise ProductionIntegrationError("PyArrow schema has no typed fields")
    types = {name: str(field(name).type).lower() for name in PARQUET_COLUMNS}
    if (
        any(
            "timestamp" not in types[name] or "utc" not in types[name]
            for name in PARQUET_COLUMNS[:2]
        )
        or "string" not in types["source_provenance"]
        or any(
            "float" not in types[name] and "double" not in types[name]
            for name in PARQUET_COLUMNS[3:]
        )
    ):
        raise ProductionIntegrationError("PyArrow schema type mismatch")


def parse_verified_parquet(
    verified: VerifiedBuffer,
    *,
    parquet_module: Any | None = None,
    pyarrow_module: Any | None = None,
) -> Any:
    """Parse precisely the verified buffer with PyArrow's list-valued selector."""
    guard_development_relative_path(verified.entry.relative_path)
    if parquet_module is None or pyarrow_module is None:
        try:
            pyarrow_module = importlib.import_module("pyarrow")
            parquet_module = importlib.import_module("pyarrow.parquet")
        except ImportError as error:
            raise ProductionIntegrationError(
                "PyArrow is required for production parsing"
            ) from error
    columns = list(PARQUET_COLUMNS)
    table = parquet_module.read_table(
        pyarrow_module.BufferReader(verified.payload), columns=columns
    )
    _validate_arrow_schema(table)
    return table


def _validate_row(row: MinuteRow) -> MinuteRow:
    event = _utc(row.event_timestamp, "event timestamp")
    available = _utc(row.available_timestamp, "available timestamp")
    numeric = (row.open, row.high, row.low, row.close, row.volume)
    if (
        guard_development_relative_path(row.relative_path) != row.relative_path
        or len(row.file_sha256) != 64
        or row.row_index < 0
        or not row.source_provenance
        or event.second != 0
        or event.microsecond != 0
        or available < event
        or any(not math.isfinite(value) for value in numeric)
        or row.open <= 0
        or row.high <= 0
        or row.low <= 0
        or row.close <= 0
        or row.volume < 0
        or row.high < max(row.open, row.close)
        or row.low > min(row.open, row.close)
    ):
        raise ProductionIntegrationError("invalid minute row")
    return row


def materialize_rows(table: Any, verified: VerifiedBuffer) -> tuple[MinuteRow, ...]:
    _validate_arrow_schema(table)
    columns = {name: table.column(name).to_pylist() for name in PARQUET_COLUMNS}
    lengths = {len(value) for value in columns.values()}
    if len(lengths) != 1:
        raise ProductionIntegrationError("column lengths differ")
    rows: list[MinuteRow] = []
    for index in range(lengths.pop()):
        event, available, provenance = (columns[name][index] for name in PARQUET_COLUMNS[:3])
        numbers = [columns[name][index] for name in PARQUET_COLUMNS[3:]]
        if (
            not isinstance(event, datetime)
            or not isinstance(available, datetime)
            or not isinstance(provenance, str)
            or any(
                isinstance(value, bool) or not isinstance(value, (int, float)) for value in numbers
            )
        ):
            raise ProductionIntegrationError("parsed row type mismatch before conversion")
        rows.append(
            _validate_row(
                MinuteRow(
                    verified.entry.relative_path,
                    verified.entry.sha256,
                    index,
                    provenance,
                    event,
                    available,
                    *(float(value) for value in numbers),
                )
            )
        )
    return tuple(rows)


def representative_row_hashes(rows: Sequence[MinuteRow]) -> tuple[str, ...]:
    if not rows:
        raise ProductionIntegrationError("representative rows require a nonempty file")
    return tuple(rows[index].identity for index in (0, len(rows) // 2, len(rows) - 1))


def _session_for_bar_end(timestamp: datetime) -> datetime:
    """A bar ending at T belongs to the UTC date of T minus one minute."""
    minute_before_end = _utc(timestamp, "event timestamp") - timedelta(minutes=1)
    return datetime(
        minute_before_end.year, minute_before_end.month, minute_before_end.day, tzinfo=UTC
    )


def build_production_row_index(
    rows_by_asset: Mapping[str, Sequence[MinuteRow]], *, end: datetime
) -> ProductionRowIndex:
    """Index retained rows once, without examining any suffix at or beyond ``end``."""
    boundary = _utc(end, "fold end")
    if set(rows_by_asset) != set(ASSETS):
        raise ProductionIntegrationError("both frozen assets are required")
    exact_rows: dict[str, dict[datetime, MinuteRow]] = {}
    session_rows: dict[str, dict[datetime, dict[datetime, MinuteRow]]] = {}
    retained_count = 0
    for asset in ASSETS:
        exact: dict[datetime, MinuteRow] = {}
        by_session: dict[datetime, dict[datetime, MinuteRow]] = {}
        previous: datetime | None = None
        for row in rows_by_asset[asset]:
            if not isinstance(row, MinuteRow):
                raise ProductionIntegrationError("malformed retained minute row")
            try:
                event = _utc(row.event_timestamp, "event timestamp")
            except (AttributeError, TypeError) as error:
                raise ProductionIntegrationError("malformed retained minute row") from error
            if event >= boundary:
                break
            validated = _validate_row(row)
            if previous is not None and event <= previous:
                raise ProductionIntegrationError("duplicate or nonmonotonic retained row")
            previous = event
            exact[event] = validated
            by_session.setdefault(_session_for_bar_end(event), {})[event] = validated
            retained_count += 1
        exact_rows[asset] = exact
        session_rows[asset] = by_session
    frozen_rows = MappingProxyType(
        {asset: MappingProxyType(rows) for asset, rows in exact_rows.items()}
    )
    frozen_sessions = MappingProxyType(
        {
            asset: MappingProxyType(
                {session: MappingProxyType(rows) for session, rows in sessions.items()}
            )
            for asset, sessions in session_rows.items()
        }
    )
    return ProductionRowIndex(boundary, frozen_rows, frozen_sessions, retained_count)


def _require_index_boundary(index: ProductionRowIndex, end: datetime, field: str) -> datetime:
    boundary = _utc(end, field)
    if boundary != index.boundary:
        raise ProductionIntegrationError("row index boundary mismatch")
    return boundary


def build_joint_sessions(
    index: ProductionRowIndex, *, end: datetime, recovery: int = 150
) -> tuple[JointSession, ...]:
    """Build exact UTC bar-end sessions from strict, order-validated row prefixes."""
    _require_index_boundary(index, end, "fold end")
    observed_session_days = sorted(
        {session for rows in index.session_rows_by_asset.values() for session in rows}
    )
    session_days: list[datetime] = []
    if observed_session_days:
        first_session = observed_session_days[0]
        last_session = observed_session_days[-1]
        session_days = [
            first_session + timedelta(days=offset)
            for offset in range((last_session - first_session).days + 1)
        ]
    raw: list[tuple[datetime, bool, datetime | None, Mapping[str, float]]] = []
    for session in session_days:
        expected = tuple(session + timedelta(minutes=index) for index in range(1, 1441))
        selected = {
            asset: index.session_rows_by_asset[asset].get(session, {}) for asset in ASSETS
        }
        complete = all(tuple(selected[asset]) == expected for asset in ASSETS)
        cutoff: datetime | None = None
        closes: Mapping[str, float] = {}
        if complete:
            used = [selected[asset][stamp] for asset in ASSETS for stamp in expected]
            cutoff = max(max(row.event_timestamp, row.available_timestamp) for row in used)
            closes = {asset: selected[asset][expected[-1]].close for asset in ASSETS}
        raw.append((session, complete, cutoff, closes))
    segments = causal_gap_segments(
        [(session, complete) for session, complete, _, _ in raw], recovery=recovery
    )
    return tuple(
        JointSession(session, complete, cutoff, closes, segment)
        for (session, complete, cutoff, closes), segment in zip(raw, segments, strict=True)
    )


def _exact_execution_rows(
    index: ProductionRowIndex, timestamp: datetime, boundary: datetime
) -> dict[str, MinuteRow]:
    fill_timestamp = _utc(timestamp, "execution timestamp")
    strict_boundary = _utc(boundary, "execution boundary")
    expected_event = fill_timestamp + timedelta(minutes=1)
    if expected_event >= strict_boundary:
        raise ProductionIntegrationError("execution row is outside the strict half-open boundary")
    result: dict[str, MinuteRow] = {}
    for asset in ASSETS:
        row = index.rows_by_asset[asset].get(expected_event)
        if row is None:
            raise ProductionIntegrationError(
                "missing exact ordinary execution row; forward scan prohibited"
            )
        if row.available_timestamp != row.event_timestamp:
            raise ProductionIntegrationError(
                "asynchronous execution row; exact synchronized fill rejected"
            )
        result[asset] = row
    return result


def fill_identities(
    sessions: Sequence[JointSession],
    index: ProductionRowIndex,
    *,
    end: datetime,
) -> tuple[FillIdentity, ...]:
    """Resolve each eligible base fill from the exact synchronized causal rows only."""
    ordered = list(sessions)
    boundary = _require_index_boundary(index, end, "fill boundary")
    if any(
        ordered[index].session >= ordered[index + 1].session for index in range(len(ordered) - 1)
    ):
        raise ProductionIntegrationError("duplicate or nonmonotonic session")
    output: list[FillIdentity] = []
    for session_index, session in enumerate(ordered):
        if not session.complete or session.segment is None:
            continue
        if session.information_cutoff is None:
            raise ProductionIntegrationError("complete session lacks information cutoff")
        base = session.information_cutoff.replace(second=0, microsecond=0) + timedelta(minutes=1)
        if base + timedelta(minutes=1) >= boundary:
            continue
        base_rows = _exact_execution_rows(index, base, boundary)
        delayed: datetime | None = None
        delayed_rows: dict[str, MinuteRow] = {}
        if session_index + 1 < len(ordered):
            successor = ordered[session_index + 1]
            if (
                successor.complete
                and successor.session == session.session + timedelta(days=1)
                and successor.segment == session.segment
            ):
                if successor.information_cutoff is None:
                    raise ProductionIntegrationError("complete successor lacks information cutoff")
                candidate = successor.information_cutoff.replace(
                    second=0, microsecond=0
                ) + timedelta(minutes=1)
                if candidate + timedelta(minutes=1) < boundary:
                    delayed = candidate
                    delayed_rows = _exact_execution_rows(index, delayed, boundary)
        output.append(
            FillIdentity(
                session.session,
                len(output),
                base,
                delayed,
                {asset: base_rows[asset].open for asset in ASSETS},
                {asset: base_rows[asset].identity for asset in ASSETS},
                {asset: delayed_rows[asset].open for asset in ASSETS} if delayed_rows else {},
                {asset: delayed_rows[asset].identity for asset in ASSETS}
                if delayed_rows
                else {},
            )
        )
    return tuple(output)


def terminal_fill_identity(
    fills: Sequence[FillIdentity],
    *,
    end: datetime,
) -> FillIdentity:
    """Select the final already-constructed fill strictly before ``end``."""
    if not fills:
        raise ProductionIntegrationError("no exact terminal fill inside half-open boundary")
    terminal = fills[-1]
    if terminal.base_timestamp >= _utc(end, "terminal boundary"):
        raise ProductionIntegrationError("terminal fill is outside half-open boundary")
    return terminal


def session_input_manifest(rows: Sequence[MinuteRow]) -> str:
    return canonical_hash([row.identity for row in rows])


def canonical_mechanical_evidence(
    *,
    rows: Sequence[MinuteRow],
    sessions: Sequence[JointSession],
    fills: Sequence[FillIdentity],
    trace_records: Sequence[Mapping[str, object]],
    cost_records: Sequence[Mapping[str, object]],
    representative_returns: Sequence[Mapping[str, object]],
) -> MechanicalEvidence:
    """Hash canonical mechanical evidence; this exposes no aggregate evaluator."""
    return MechanicalEvidence(
        session_input_manifest(rows),
        canonical_hash([item.identity for item in sessions]),
        canonical_hash([item.identity for item in fills]),
        canonical_hash(list(trace_records)),
        canonical_hash(list(cost_records)),
        canonical_hash(list(representative_returns)),
    )


def reconcile_representative_accounting(
    case: RepresentativeAccounting,
) -> RepresentativeAccountingReconciliation:
    """Independently reconstruct one bounded mechanical fill interval, never a series."""
    if (
        set(case.units) != set(ASSETS)
        or set(case.prior_prices) != set(ASSETS)
        or set(case.current_prices) != set(ASSETS)
        or set(case.target_weights) != set(ASSETS)
    ):
        raise ProductionIntegrationError("representative accounting requires both assets")
    values = [
        case.cash,
        case.cost_rate,
        *case.units.values(),
        *case.prior_prices.values(),
        *case.current_prices.values(),
        *case.target_weights.values(),
    ]
    if (
        any(not math.isfinite(value) for value in values)
        or case.cash < 0
        or case.cost_rate < 0
        or any(value <= 0 for value in [*case.prior_prices.values(), *case.current_prices.values()])
        or any(value < 0 for value in case.target_weights.values())
        or sum(case.target_weights.values()) > 1
    ):
        raise ProductionIntegrationError("invalid representative accounting input")
    if any(case.units[asset] < 0 for asset in ASSETS):
        raise ProductionIntegrationError("negative representative risky units")
    if any(case.target_weights[asset] not in (0.0, 0.5) for asset in ASSETS):
        raise ProductionIntegrationError("invalid frozen representative target")
    if case.cost_rate not in (0.0014, 0.0028):
        raise ProductionIntegrationError("undeclared representative cost rate")
    prior_postcost = case.cash + sum(
        case.units[asset] * case.prior_prices[asset] for asset in ASSETS
    )
    pretrade = case.cash + sum(case.units[asset] * case.current_prices[asset] for asset in ASSETS)
    if not math.isfinite(prior_postcost) or prior_postcost <= 0:
        raise ProductionIntegrationError("nonpositive representative prior postcost equity")
    if not math.isfinite(pretrade) or pretrade <= 0:
        raise ProductionIntegrationError("nonpositive representative pretrade equity")
    drifted = {asset: case.units[asset] * case.current_prices[asset] / pretrade for asset in ASSETS}
    turnover = sum(abs(case.target_weights[asset] - drifted[asset]) for asset in ASSETS)
    cost = pretrade * case.cost_rate * turnover
    postcost = pretrade - cost
    if not math.isfinite(postcost) or postcost <= 0:
        raise ProductionIntegrationError("nonpositive representative postcost equity")
    units = {
        asset: postcost * case.target_weights[asset] / case.current_prices[asset]
        for asset in ASSETS
    }
    cash = postcost * (1 - sum(case.target_weights.values()))
    interval_return = postcost / prior_postcost - 1
    payload: Mapping[str, object] = {
        "prior_postcost_equity": prior_postcost,
        "pretrade_equity": pretrade,
        "turnover": turnover,
        "cost": cost,
        "postcost_equity": postcost,
        "units": units,
        "cash": cash,
        "interval_return": interval_return,
    }
    return RepresentativeAccountingReconciliation(
        prior_postcost,
        pretrade,
        turnover,
        cost,
        postcost,
        units,
        cash,
        interval_return,
        canonical_hash(payload),
    )
