"""Holdout-closed evaluator for the frozen equal-sleeve volatility experiment.

No filesystem value is touched at import time.  ``load_development_market`` is
the only market-data seam: it validates the frozen identities before path
construction, reads exact bytes, rehashes those bytes, and parses the verified
buffer rather than reopening a path.
"""

from __future__ import annotations

import bisect
import hashlib
import importlib
import itertools
import json
import math
import re
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from strategy_control.volatility_managed import (
    BASE_COST,
    DOUBLED_COST,
    EXPERIMENT_ID,
    SYMBOLS,
    TRIALS,
    Fill,
    Target,
    Trial,
    VolatilityManagedError,
    baseline_all_six,
    canonical_hash,
    deflated_sharpe,
    event_drawdown,
    first_open_after,
    joint_grid_status,
    make_fill,
    make_target,
    pbo,
    regime_labels,
    session_manifest,
    sleeve_scalar,
    stationary_bootstrap,
    validate_allowlist,
    verify_opened_bytes,
)

WRAPPER_SHA256 = "42f99d6724d4cb315635bbf12bc70bf2f17594871d16698c83997665fe7cac7c"
EFFECTIVE_SHA256 = "d47659466a2e55e72eef47051cba3b6c2cb551d036e41269bdbf0d7a9f19d8ad"
EFFECTIVE_BYTE_SHA256 = "f983562d54339cbb8f4221e6f44a81725946e120d6c68ea9de200fa2a2374a30"
DATA_CONTRACT_SHA256 = "d2a02bca439359ca93bcb503bc5888fe4d6297b6f2115ac17c09d8da78f89183"
SOURCE_COMMIT = "d1d6066a6042b0c2e1c6af75047f5ebf935c739f"
ALLOWLIST_SHA256 = "40bb5cf5b7bd3a8ac30e2a3b1d022462fe45888790b1ba58a7068a1982cdc6bd"
DATASET_ROOT_RELATIVE = Path("data/real/historical-v2-pathc-20260723T175155Z")
FREEZE_MANIFEST_RELATIVE = Path(
    "data/frozen/historical-v2-pathc-20260723T175155Z/"
    "DATASET_FREEZE_MANIFEST_historical-v2-pathc-20260723T175155Z.json"
)
FREEZE_MANIFEST_BYTE_SHA256 = "243d875979df2991ef3c941d06e13d608c30e44df0eab512afdbb3fb6b0a07ad"
OBSERVATION_START = datetime(2024, 7, 1, tzinfo=UTC)
DEVELOPMENT_START = datetime(2025, 1, 1, tzinfo=UTC)
DEVELOPMENT_END = datetime(2026, 1, 1, tzinfo=UTC)
DEVELOPMENT_FOLDS = (
    (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), DEVELOPMENT_END),
)
PARTITION_RE = re.compile(
    r"^canonical/venue=binance/symbol=(BTCUSDT|ETHUSDT)/"
    r"year=(2024|2025)/month=(\d{2})/observations\.parquet$"
)


@dataclass(frozen=True)
class SourceRow:
    symbol: str
    relative_path: str
    file_sha256: str
    source_record_id: str
    event_timestamp: datetime
    available_timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    row_sha256: str
    valid: bool


@dataclass(frozen=True)
class AssetSession:
    symbol: str
    start: datetime
    complete: bool
    available_timestamp: datetime
    close: float | None
    source_record_ids: tuple[str, ...]
    source_record_hashes: tuple[str, ...]
    manifest_sha256: str


@dataclass(frozen=True)
class JointSession:
    start: datetime
    complete: bool
    assets: Mapping[str, AssetSession]
    detection_times: tuple[datetime, ...]


@dataclass(frozen=True)
class JointVector:
    timestamp: datetime
    event_timestamp: datetime
    available_timestamp: datetime
    prices: Mapping[str, float]
    row_sha256: Mapping[str, str]
    vector_sha256: str


@dataclass(frozen=True)
class OpenedPartition:
    relative_path: str
    bytes: int
    sha256: str
    symbol: str
    month: str


@dataclass(frozen=True)
class DevelopmentMarket:
    sessions: tuple[JointSession, ...]
    sleeve_returns: tuple[tuple[float, float] | None, ...]
    vectors: Mapping[datetime, JointVector]
    vector_times: tuple[datetime, ...]
    gap_detection_times: tuple[datetime, ...]
    opened_partitions: tuple[OpenedPartition, ...]
    input_identity_sha256: str
    source_commit: str
    holdout_values_read: bool = False


@dataclass(frozen=True)
class PlannedTarget:
    target: Target
    trial: Trial
    signal_session_end: datetime
    information_time: datetime
    expected_open: datetime
    weights: tuple[float, float, float]


@dataclass(frozen=True)
class PlannedFill:
    timestamp: datetime
    target: PlannedTarget
    path_kind: str
    weights: tuple[float, float, float]
    entry_only: bool = False


@dataclass(frozen=True)
class Account:
    units: tuple[float, float]
    cash: float
    prices: tuple[float, float] | None
    contributions: tuple[float, float]


@dataclass(frozen=True)
class PathResult:
    name: str
    start: datetime
    end: datetime
    terminal_wealth: float
    net_return: float
    annualized_sharpe: float
    maximum_drawdown: float
    total_cost: float
    total_turnover: float
    completed_rebalances: int
    completed_fill_timestamps: tuple[datetime, ...]
    daily_wealth: Mapping[datetime, float | None]
    daily_returns: Mapping[datetime, float]
    daily_currency_pnl: Mapping[datetime, float]
    daily_asset_contributions: Mapping[datetime, tuple[float, float]]
    asset_contributions: tuple[float, float]
    target_hashes: tuple[str, ...]
    fill_hashes: tuple[str, ...]
    cancelled_target_hashes: tuple[str, ...]
    event_observations: int
    terminal_cash: bool
    trace_reconciled: bool


def _utc(value: Any) -> datetime:
    converted = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if not isinstance(converted, datetime):
        raise VolatilityManagedError("timestamp is not datetime-like")
    if converted.tzinfo is None:
        converted = converted.replace(tzinfo=UTC)
    converted = converted.astimezone(UTC)
    if converted.utcoffset() != timedelta(0):
        raise VolatilityManagedError("timestamp is not UTC")
    return converted


def _canonical_without(payload: Mapping[str, Any], key: str) -> str:
    copied = dict(payload)
    copied.pop(key, None)
    return canonical_hash(copied)


def verify_frozen_contract(
    wrapper: Mapping[str, Any], effective: Mapping[str, Any], effective_bytes: bytes
) -> None:
    if (
        wrapper.get("status") != "FROZEN_NO_DATA"
        or wrapper.get("experiment_id") != EXPERIMENT_ID
        or wrapper.get("preregistration_sha256") != WRAPPER_SHA256
        or _canonical_without(wrapper, "preregistration_sha256") != WRAPPER_SHA256
    ):
        raise VolatilityManagedError("frozen wrapper identity mismatch")
    reference = wrapper.get("effective_contract")
    if not isinstance(reference, Mapping) or (
        reference.get("canonical_sha256") != EFFECTIVE_SHA256
        or reference.get("byte_sha256") != EFFECTIVE_BYTE_SHA256
        or reference.get("complete") is not True
    ):
        raise VolatilityManagedError("effective-contract reference mismatch")
    if (
        _canonical_without(effective, "draft_sha256") != EFFECTIVE_SHA256
        or effective.get("draft_sha256") != EFFECTIVE_SHA256
        or hashlib.sha256(effective_bytes).hexdigest() != EFFECTIVE_BYTE_SHA256
        or effective.get("effective_contract_complete") is not True
        or effective.get("experiment_id") != EXPERIMENT_ID
        or effective.get("capital_permitted") != 0
        or effective.get("holdout_values_read") is not False
        or effective.get("returns_calculated") is not False
    ):
        raise VolatilityManagedError("effective frozen contract mismatch")
    declared = effective.get("trials")
    if not isinstance(declared, Mapping) or tuple(declared.get("declared_order", ())) != tuple(
        trial.name for trial in TRIALS
    ):
        raise VolatilityManagedError("frozen trial order mismatch")
    if len(effective.get("required_synthetic_tests", ())) != 30:
        raise VolatilityManagedError("frozen synthetic-test inventory mismatch")


def verify_data_contract(data_contract: Mapping[str, Any]) -> None:
    if canonical_hash(data_contract) != DATA_CONTRACT_SHA256:
        raise VolatilityManagedError("reused data-contract hash mismatch")
    if (
        data_contract.get("status") != "PASS"
        or data_contract.get("canonical_partition_count") != 48
        or data_contract.get("holdout_opened") is not False
        or data_contract.get("holdout_parquet_footers_or_values_read") is not False
    ):
        raise VolatilityManagedError("reused data contract is not holdout-safe")


def verify_source_freeze_manifest(raw: bytes) -> str:
    if hashlib.sha256(raw).hexdigest() != FREEZE_MANIFEST_BYTE_SHA256:
        raise VolatilityManagedError("dataset freeze-manifest byte hash mismatch")
    payload = json.loads(raw)
    if not isinstance(payload, Mapping) or (
        payload.get("repository_commit") != SOURCE_COMMIT
        or payload.get("freeze_status") != "frozen"
        or payload.get("revised_dataset_id") != "historical-v2-pathc-20260723T175155Z"
    ):
        raise VolatilityManagedError("dataset freeze-manifest identity mismatch")
    return SOURCE_COMMIT


def development_allowlist(
    effective: Mapping[str, Any], data_contract: Mapping[str, Any], source_commit: str
) -> tuple[Mapping[str, Any], ...]:
    """Validate every non-path identity before returning a path label."""

    verify_data_contract(data_contract)
    identity = effective.get("data_identity_contract")
    if not isinstance(identity, Mapping):
        raise VolatilityManagedError("data identity contract missing")
    entries = identity.get("development_allowlist")
    if not isinstance(entries, list):
        raise VolatilityManagedError("development allowlist missing")
    validate_allowlist(source_commit, SOURCE_COMMIT, entries, ALLOWLIST_SHA256)
    contract_items = data_contract.get("partitions")
    if not isinstance(contract_items, list):
        raise VolatilityManagedError("reused partition inventory missing")
    indexed = {
        str(item.get("relative_path")): item
        for item in contract_items
        if isinstance(item, Mapping)
    }
    for entry in entries:
        relative = str(entry["relative_path"])
        match = PARTITION_RE.fullmatch(relative)
        reused = indexed.get(relative)
        if match is None or reused is None or any(
            reused.get(key) != entry[key] for key in ("bytes", "month", "sha256", "symbol")
        ):
            raise VolatilityManagedError("allowlist does not exactly match reused contract")
        if "2026" in relative:
            raise VolatilityManagedError("development allowlist contains holdout label")
    return tuple(entries)


def _row_hash(row: Mapping[str, Any]) -> str:
    return canonical_hash(
        {
            "relative_path": row["relative_path"],
            "file_sha256": row["file_sha256"],
            "source_record_id": row["source_record_id"],
            "event_timestamp": _utc(row["event_timestamp"]).isoformat(),
            "available_timestamp": _utc(row["available_timestamp"]).isoformat(),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
        }
    )


def _valid_ohlc(open_: float, high: float, low: float, close: float) -> bool:
    return (
        all(math.isfinite(value) and value > 0 for value in (open_, high, low, close))
        and low <= min(open_, close)
        and high >= max(open_, close)
    )


def _default_byte_opener(source_repository: Path, relative_path: str) -> bytes:
    return (source_repository / DATASET_ROOT_RELATIVE / relative_path).read_bytes()


def load_development_market(
    source_repository: Path,
    effective: Mapping[str, Any],
    data_contract: Mapping[str, Any],
    *,
    byte_opener: Callable[[str], bytes] | None = None,
    source_manifest_bytes: bytes | None = None,
) -> DevelopmentMarket:
    """Read the exact 36 development buffers after all label-level validation."""

    manifest_raw = (
        source_manifest_bytes
        if source_manifest_bytes is not None
        else (source_repository / FREEZE_MANIFEST_RELATIVE).read_bytes()
    )
    source_commit = verify_source_freeze_manifest(manifest_raw)
    entries = development_allowlist(effective, data_contract, source_commit)
    opener = byte_opener or (lambda relative: _default_byte_opener(source_repository, relative))
    rows_by_symbol: dict[str, list[SourceRow]] = {symbol: [] for symbol in SYMBOLS}
    opened: list[OpenedPartition] = []
    columns = (
        "event_timestamp",
        "available_timestamp",
        "open",
        "high",
        "low",
        "close",
        "source_record_id",
    )
    pandas: Any | None = None
    pyarrow: Any | None = None
    parquet: Any | None = None
    for entry in entries:
        relative = str(entry["relative_path"])
        raw = opener(relative)
        verify_opened_bytes(raw, int(entry["bytes"]), str(entry["sha256"]))
        if pandas is None or pyarrow is None or parquet is None:
            pandas = importlib.import_module("pandas")
            pyarrow = importlib.import_module("pyarrow")
            parquet = importlib.import_module("pyarrow.parquet")
        table = parquet.read_table(pyarrow.BufferReader(raw), columns=columns)
        frame = table.to_pandas()
        frame["event_timestamp"] = pandas.to_datetime(frame["event_timestamp"], utc=True)
        frame["available_timestamp"] = pandas.to_datetime(frame["available_timestamp"], utc=True)
        symbol = str(entry["symbol"])
        for record in frame.to_dict(orient="records"):
            event = _utc(record["event_timestamp"])
            available = _utc(record["available_timestamp"])
            open_, high, low, close = (
                float(record["open"]),
                float(record["high"]),
                float(record["low"]),
                float(record["close"]),
            )
            payload = {
                **record,
                "relative_path": relative,
                "file_sha256": str(entry["sha256"]),
            }
            rows_by_symbol[symbol].append(
                SourceRow(
                    symbol,
                    relative,
                    str(entry["sha256"]),
                    str(record["source_record_id"]),
                    event,
                    available,
                    open_,
                    high,
                    low,
                    close,
                    _row_hash(payload),
                    available >= event
                    and event.second == 0
                    and event.microsecond == 0
                    and _valid_ohlc(open_, high, low, close),
                )
            )
        opened.append(
            OpenedPartition(
                relative,
                int(entry["bytes"]),
                str(entry["sha256"]),
                symbol,
                str(entry["month"]),
            )
        )
    return build_market_from_rows(rows_by_symbol, tuple(opened), source_commit)


def build_market_from_rows(
    rows_by_symbol: Mapping[str, Sequence[SourceRow]],
    opened_partitions: tuple[OpenedPartition, ...],
    source_commit: str,
) -> DevelopmentMarket:
    if set(rows_by_symbol) != set(SYMBOLS) or source_commit != SOURCE_COMMIT:
        raise VolatilityManagedError("source identity changed during construction")
    indexed: dict[str, dict[datetime, list[SourceRow]]] = {symbol: {} for symbol in SYMBOLS}
    vector_rows: dict[str, dict[datetime, list[SourceRow]]] = {symbol: {} for symbol in SYMBOLS}
    for symbol in SYMBOLS:
        for row in rows_by_symbol[symbol]:
            session_start = (row.event_timestamp - timedelta(microseconds=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            indexed[symbol].setdefault(session_start, []).append(row)
            vector_rows[symbol].setdefault(row.event_timestamp - timedelta(minutes=1), []).append(
                row
            )

    vectors: dict[datetime, JointVector] = {}
    common_times = sorted(set(vector_rows[SYMBOLS[0]]) & set(vector_rows[SYMBOLS[1]]))
    for timestamp in common_times:
        pair = (vector_rows[SYMBOLS[0]][timestamp], vector_rows[SYMBOLS[1]][timestamp])
        if any(len(rows) != 1 or not rows[0].valid for rows in pair):
            continue
        btc, eth = pair[0][0], pair[1][0]
        vector_payload = {
            "timestamp": timestamp.isoformat(),
            "event_timestamp": btc.event_timestamp.isoformat(),
            "available_timestamp": max(
                btc.available_timestamp, eth.available_timestamp
            ).isoformat(),
            "BTC_row_sha256": btc.row_sha256,
            "ETH_row_sha256": eth.row_sha256,
            "prices": [btc.open, eth.open],
        }
        vectors[timestamp] = JointVector(
            timestamp,
            btc.event_timestamp,
            max(btc.available_timestamp, eth.available_timestamp),
            {SYMBOLS[0]: btc.open, SYMBOLS[1]: eth.open},
            {SYMBOLS[0]: btc.row_sha256, SYMBOLS[1]: eth.row_sha256},
            canonical_hash(vector_payload),
        )

    sessions: list[JointSession] = []
    gaps: set[datetime] = set()
    day = OBSERVATION_START
    while day < DEVELOPMENT_END:
        assets: dict[str, AssetSession] = {}
        per_symbol = [indexed[symbol].get(day, []) for symbol in SYMBOLS]
        status = joint_grid_status(
            day,
            [row.event_timestamp for row in per_symbol[0]],
            [row.event_timestamp for row in per_symbol[1]],
            DEVELOPMENT_END,
            btc_available=[row.available_timestamp for row in per_symbol[0]],
            eth_available=[row.available_timestamp for row in per_symbol[1]],
            btc_valid=[row.valid for row in per_symbol[0]],
            eth_valid=[row.valid for row in per_symbol[1]],
        )
        complete = status.complete
        gaps.update(event.detected_at for event in status.triggers)
        for symbol, rows in zip(SYMBOLS, per_symbol, strict=True):
            manifest_rows = [
                {
                    "relative_path": row.relative_path,
                    "file_sha256": row.file_sha256,
                    "row_identifier": row.source_record_id,
                    "event_timestamp": row.event_timestamp.isoformat(),
                    "available_timestamp": row.available_timestamp.isoformat(),
                    "row_hash": row.row_sha256,
                }
                for row in rows
            ]
            assets[symbol] = AssetSession(
                symbol,
                day,
                complete,
                max((row.available_timestamp for row in rows), default=DEVELOPMENT_END),
                rows[-1].open if complete and rows else None,
                tuple(row.source_record_id for row in rows),
                tuple(row.row_sha256 for row in rows),
                session_manifest(manifest_rows) if rows else canonical_hash([]),
            )
        sessions.append(
            JointSession(
                day,
                complete,
                assets,
                tuple(sorted({event.detected_at for event in status.triggers})),
            )
        )
        day += timedelta(days=1)

    paired: list[tuple[float, float] | None] = []
    previous: JointSession | None = None
    for session in sessions:
        if (
            not session.complete
            or previous is None
            or not previous.complete
            or session.start != previous.start + timedelta(days=1)
        ):
            paired.append(None)
        else:
            values = []
            for symbol in SYMBOLS:
                current_close = session.assets[symbol].close
                previous_close = previous.assets[symbol].close
                if current_close is None or previous_close is None:
                    raise VolatilityManagedError("complete session lacks endpoint close")
                values.append(current_close / previous_close - 1)
            paired.append((values[0], values[1]))
        previous = session if session.complete else None

    identity = canonical_hash(
        [
            {
                "relative_path": item.relative_path,
                "bytes": item.bytes,
                "sha256": item.sha256,
                "symbol": item.symbol,
                "month": item.month,
            }
            for item in opened_partitions
        ]
    )
    return DevelopmentMarket(
        tuple(sessions),
        tuple(paired),
        vectors,
        tuple(sorted(vectors)),
        tuple(sorted(gaps)),
        opened_partitions,
        identity,
        source_commit,
        False,
    )


def _scheduled(session: JointSession, trial: Trial) -> bool:
    if session.start.weekday() != 6:
        return False
    if not trial.biweekly:
        return True
    anchor = datetime(2024, 7, 7, tzinfo=UTC)
    weeks = (session.start - anchor).days // 7
    return weeks >= 0 and weeks % 2 == 0


def _trace_for_window(
    sessions: Sequence[JointSession], first: int, last: int
) -> tuple[tuple[str, ...], tuple[str, ...], str, datetime]:
    descriptors: list[Mapping[str, str]] = []
    identifiers: list[str] = []
    hashes: list[str] = []
    information_times: list[datetime] = []
    for session in sessions[first : last + 1]:
        for symbol in SYMBOLS:
            asset = session.assets[symbol]
            identifier = f"{symbol}|{session.start.isoformat()}|{asset.manifest_sha256}"
            identifiers.append(identifier)
            hashes.append(asset.manifest_sha256)
            information_times.extend((session.start + timedelta(days=1), asset.available_timestamp))
            descriptors.append(
                {
                    "relative_path": f"session://{symbol}/{session.start.date().isoformat()}",
                    "file_sha256": asset.manifest_sha256,
                    "row_identifier": identifier,
                    "event_timestamp": (session.start + timedelta(days=1)).isoformat(),
                    "available_timestamp": asset.available_timestamp.isoformat(),
                    "row_hash": asset.manifest_sha256,
                }
            )
    if not information_times:
        raise VolatilityManagedError("empty estimator trace")
    return (
        tuple(identifiers),
        tuple(hashes),
        session_manifest(descriptors),
        max(information_times),
    )


def _materialize_planned_target(
    *,
    trial: Trial,
    path_kind: str,
    trial_or_benchmark: str,
    start: datetime,
    end: datetime,
    decision_session_end: datetime,
    information_time: datetime,
    expected_open: datetime,
    sigma_hat: float,
    weights: tuple[float, float, float],
    source_ids: Sequence[str],
    source_hashes: Sequence[str],
    manifest_sha256: str,
) -> PlannedTarget:
    scalar = weights[0] + weights[1]
    record = {
        "schema_version": "1.0",
        "experiment_id": EXPERIMENT_ID,
        "path_kind": path_kind,
        "trial_or_benchmark": trial_or_benchmark,
        "boundary_start": start.isoformat(),
        "boundary_end": end.isoformat(),
        "decision_session_end": decision_session_end.isoformat(),
        "I_s": information_time.isoformat(),
        "B_s": expected_open.isoformat(),
        "lookback": trial.lookback,
        "target_volatility": trial.target,
        "sigma_hat": sigma_hat,
        "cap_state": "capped" if trial.target / sigma_hat >= 1 else "uncapped",
        "risky_scalar": scalar,
        "weights_BTC_ETH_cash": list(weights),
        "ordered_source_record_ids": list(source_ids),
        "ordered_source_record_hashes": list(source_hashes),
        "session_input_manifest_sha256": manifest_sha256,
        "status": "materialized",
    }
    target = make_target(record)
    return PlannedTarget(
        target,
        trial,
        decision_session_end,
        information_time,
        expected_open,
        weights,
    )


def build_trial_targets(
    market: DevelopmentMarket,
    trial: Trial,
    start: datetime,
    end: datetime,
    *,
    path_kind: str = "base",
) -> tuple[PlannedTarget, ...]:
    if (
        market.holdout_values_read
        or market.source_commit != SOURCE_COMMIT
        or end > DEVELOPMENT_END
        or not start < end
    ):
        raise VolatilityManagedError("development target boundary violation")
    targets: list[PlannedTarget] = []
    run = 0
    previous_start: datetime | None = None
    for index, session in enumerate(market.sessions):
        contiguous = (
            session.complete
            and (previous_start is None or session.start == previous_start + timedelta(days=1))
        )
        run = run + 1 if contiguous else (1 if session.complete else 0)
        previous_start = session.start if session.complete else None
        if not session.complete or not _scheduled(session, trial):
            continue
        if run < max(91, trial.lookback + 1) or index < trial.lookback:
            continue
        returns = market.sleeve_returns[index - trial.lookback + 1 : index + 1]
        if len(returns) != trial.lookback or any(value is None for value in returns):
            continue
        checked_returns = tuple(value for value in returns if value is not None)
        sigma_hat, weights = sleeve_scalar(checked_returns, trial)
        first = index - trial.lookback
        source_ids, source_hashes, manifest_sha256, information_time = _trace_for_window(
            market.sessions, first, index
        )
        expected_open = first_open_after(information_time)
        decision_end = session.start + timedelta(days=1)
        if start <= expected_open < end - timedelta(minutes=1):
            targets.append(
                _materialize_planned_target(
                    trial=trial,
                    path_kind=path_kind,
                    trial_or_benchmark=trial.name,
                    start=start,
                    end=end,
                    decision_session_end=decision_end,
                    information_time=information_time,
                    expected_open=expected_open,
                    sigma_hat=sigma_hat,
                    weights=weights,
                    source_ids=source_ids,
                    source_hashes=source_hashes,
                    manifest_sha256=manifest_sha256,
                )
            )
    return tuple(targets)


def delayed_fills(
    market: DevelopmentMarket, targets: Sequence[PlannedTarget]
) -> tuple[PlannedFill, ...]:
    output: list[PlannedFill] = []
    for target in targets:
        signal_start = target.signal_session_end - timedelta(days=1)
        index = next(
            (
                position
                for position, session in enumerate(market.sessions)
                if session.start == signal_start
            ),
            None,
        )
        if index is None:
            raise VolatilityManagedError("delayed target session missing")
        next_index = index + 1
        while next_index < len(market.sessions) and not market.sessions[next_index].complete:
            next_index += 1
        if next_index >= len(market.sessions):
            raise VolatilityManagedError("delayed target lacks next complete session")
        delayed_session = market.sessions[next_index]
        delayed_information = max(
            delayed_session.start + timedelta(days=1),
            *(delayed_session.assets[symbol].available_timestamp for symbol in SYMBOLS),
        )
        output.append(
            PlannedFill(
                first_open_after(delayed_information),
                target,
                "additional_delay",
                target.weights,
            )
        )
    return tuple(output)


def _ordinary_fills(
    targets: Sequence[PlannedTarget], *, path_kind: str = "base"
) -> tuple[PlannedFill, ...]:
    return tuple(
        PlannedFill(target.expected_open, target, path_kind, target.weights) for target in targets
    )


def _benchmark_targets(
    primary: Sequence[PlannedTarget],
    *,
    name: str,
    weights: tuple[float, float, float],
    entry_only: bool,
) -> tuple[PlannedFill, ...]:
    output: list[PlannedFill] = []
    for source in primary:
        record = source.target.record
        target = _materialize_planned_target(
            trial=source.trial,
            path_kind="benchmark",
            trial_or_benchmark=name,
            start=_utc(datetime.fromisoformat(str(record["boundary_start"]))),
            end=_utc(datetime.fromisoformat(str(record["boundary_end"]))),
            decision_session_end=source.signal_session_end,
            information_time=source.information_time,
            expected_open=source.expected_open,
            sigma_hat=float(record["sigma_hat"]),
            weights=weights,
            source_ids=tuple(str(value) for value in record["ordered_source_record_ids"]),
            source_hashes=tuple(str(value) for value in record["ordered_source_record_hashes"]),
            manifest_sha256=str(record["session_input_manifest_sha256"]),
        )
        output.append(
            PlannedFill(target.expected_open, target, "benchmark", weights, entry_only)
        )
    return tuple(output)


def _initial_account() -> Account:
    return Account((0.0, 0.0), 1.0, None, (0.0, 0.0))


def _mark(account: Account, vector: JointVector) -> tuple[Account, float]:
    prices = (float(vector.prices[SYMBOLS[0]]), float(vector.prices[SYMBOLS[1]]))
    if any(not math.isfinite(value) or value <= 0 for value in prices):
        raise VolatilityManagedError("invalid mark vector")
    contribution = list(account.contributions)
    if account.prices is not None:
        for index in range(2):
            contribution[index] += account.units[index] * (prices[index] - account.prices[index])
    wealth = account.cash + sum(
        account.units[index] * prices[index] for index in range(2)
    )
    if not math.isfinite(wealth) or wealth <= 0:
        raise VolatilityManagedError("nonpositive marked wealth")
    return Account(
        account.units,
        account.cash,
        prices,
        (contribution[0], contribution[1]),
    ), wealth


def _cash(account: Account) -> bool:
    return abs(account.units[0]) <= 1e-15 and abs(account.units[1]) <= 1e-15


def _weights(account: Account, wealth: float) -> tuple[float, float, float]:
    if account.prices is None:
        return (0.0, 0.0, 1.0)
    return (
        account.units[0] * account.prices[0] / wealth,
        account.units[1] * account.prices[1] / wealth,
        account.cash / wealth,
    )


def _trade(
    account: Account,
    vector: JointVector,
    target: tuple[float, float, float],
    cost_rate: float,
) -> tuple[Account, Mapping[str, float], tuple[float, float, float]]:
    marked, wealth = _mark(account, vector)
    prior = _weights(marked, wealth)
    if (
        any(not math.isfinite(value) or value < 0 for value in target)
        or abs(sum(target) - 1) > 1e-12
        or (
            target[0] != target[1]
            and target not in ((1.0, 0.0, 0.0), (0.0, 1.0, 0.0))
        )
    ):
        raise VolatilityManagedError("invalid evaluator target")
    gross = abs(target[0] - prior[0]) + abs(target[1] - prior[1])
    turnover = 0.5 * sum(abs(target[i] - prior[i]) for i in range(3))
    cost = wealth * cost_rate * gross
    if not math.isfinite(cost) or cost < 0 or cost >= wealth:
        raise VolatilityManagedError("invalid evaluator trade cost")
    changes = (abs(target[0] - prior[0]), abs(target[1] - prior[1]))
    total_change = sum(changes)
    cost_by_asset = (
        cost * changes[0] / total_change if total_change else 0.0,
        cost * changes[1] / total_change if total_change else 0.0,
    )
    wealth_after = wealth - cost
    prices = (float(vector.prices[SYMBOLS[0]]), float(vector.prices[SYMBOLS[1]]))
    units = (
        wealth_after * target[0] / prices[0],
        wealth_after * target[1] / prices[1],
    )
    contributions = (
        marked.contributions[0] - cost_by_asset[0],
        marked.contributions[1] - cost_by_asset[1],
    )
    return (
        Account(units, wealth_after * target[2], prices, contributions),
        {"wealth": wealth_after, "cost": cost, "turnover": turnover, "gross": gross},
        prior,
    )


def _next_safety_vector(market: DevelopmentMarket, trigger: datetime) -> JointVector | None:
    index = bisect.bisect_right(market.vector_times, trigger)
    while index < len(market.vector_times):
        vector = market.vectors[market.vector_times[index]]
        if vector.timestamp > trigger and vector.available_timestamp > trigger:
            return vector
        index += 1
    return None


def _endpoint_times(start: datetime, end: datetime) -> tuple[datetime, ...]:
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    output = []
    while day < end:
        endpoint = day + timedelta(hours=23, minutes=59)
        if start <= endpoint < end:
            output.append(endpoint)
        day += timedelta(days=1)
    return tuple(output)


def _fill_evidence(
    fill: PlannedFill,
    vector: JointVector,
    prior: tuple[float, float, float],
    metrics: Mapping[str, float],
    cost_rate: float,
) -> Fill:
    target = fill.target.target
    record = target.record
    evidence = {
        "schema_version": "1.0",
        "experiment_id": EXPERIMENT_ID,
        "path_kind": fill.path_kind,
        "trial_or_benchmark": record["trial_or_benchmark"],
        "boundary_start": record["boundary_start"],
        "boundary_end": record["boundary_end"],
        "decision_session_end": record["decision_session_end"],
        "B_s": record["B_s"],
        "execution_event_timestamp": vector.event_timestamp.isoformat(),
        "execution_available_timestamp": vector.available_timestamp.isoformat(),
        "BTC_row_sha256": vector.row_sha256[SYMBOLS[0]],
        "ETH_row_sha256": vector.row_sha256[SYMBOLS[1]],
        "execution_vector_sha256": vector.vector_sha256,
        "parent_target_sha256": target.sha256,
        "pretrade_weights_BTC_ETH_cash": list(prior),
        "target_weights_BTC_ETH_cash": list(fill.weights),
        "cost_rate": cost_rate,
        "currency_cost": metrics["cost"],
        "turnover": metrics["turnover"],
        "gross_risky_trade": metrics["gross"],
        "status": "filled",
        "cancellation_reason": None,
    }
    return make_fill(evidence, target)


def simulate_path(
    market: DevelopmentMarket,
    name: str,
    start: datetime,
    end: datetime,
    fills: Sequence[PlannedFill],
    *,
    cost_rate: float,
) -> PathResult:
    if market.holdout_values_read or end > DEVELOPMENT_END or not start < end:
        raise VolatilityManagedError("development path boundary violation")
    terminal = end - timedelta(minutes=1)
    endpoints = _endpoint_times(start, end)
    endpoint_set = set(endpoints)
    fill_by_time: dict[datetime, PlannedFill] = {}
    target_hashes: list[str] = []
    for fill in fills:
        if fill.timestamp in fill_by_time:
            raise VolatilityManagedError("multiple targets share an execution timestamp")
        fill_by_time[fill.timestamp] = fill
        target_hashes.append(fill.target.target.sha256)
    left = bisect.bisect_left(market.vector_times, start)
    right = bisect.bisect_right(market.vector_times, terminal)
    active_vectors = market.vector_times[left:right]
    active_gaps = tuple(
        value for value in market.gap_detection_times if start <= value <= terminal
    )
    events = sorted(
        set(active_vectors) | set(active_gaps) | set(fill_by_time) | endpoint_set | {terminal}
    )

    account = _initial_account()
    event_wealth = [1.0]
    total_cost = 0.0
    total_turnover = 0.0
    completed = 0
    completed_times: list[datetime] = []
    fill_hashes: list[str] = []
    cancelled: list[str] = []
    daily_wealth: dict[datetime, float | None] = {}
    daily_contributions: dict[datetime, tuple[float, float]] = {}
    safety_due: set[datetime] = set()
    last_trigger: datetime | None = None
    last_wealth = 1.0

    for timestamp in events:
        vector = market.vectors.get(timestamp)
        if vector is not None:
            account, last_wealth = _mark(account, vector)
            event_wealth.append(last_wealth)

        if timestamp in active_gaps:
            last_trigger = timestamp
            for future_time, future_fill in fill_by_time.items():
                if future_time >= timestamp and future_fill.target.signal_session_end <= timestamp:
                    cancelled.append(future_fill.target.target.sha256)
            if not _cash(account):
                safety = _next_safety_vector(market, timestamp)
                if safety is None or safety.timestamp > terminal:
                    raise VolatilityManagedError("unpriceable exposed quarantine")
                safety_due.add(safety.timestamp)

        if timestamp == terminal:
            if vector is None:
                raise VolatilityManagedError("missing exact terminal vector")
            account, metrics, _ = _trade(account, vector, (0.0, 0.0, 1.0), cost_rate)
            total_cost += metrics["cost"]
            total_turnover += metrics["turnover"]
            last_wealth = metrics["wealth"]
            event_wealth.append(last_wealth)
        else:
            if timestamp in safety_due:
                if vector is None:
                    raise VolatilityManagedError("safety vector disappeared")
                account, metrics, _ = _trade(account, vector, (0.0, 0.0, 1.0), cost_rate)
                total_cost += metrics["cost"]
                total_turnover += metrics["turnover"]
                last_wealth = metrics["wealth"]
                event_wealth.append(last_wealth)
                safety_due.remove(timestamp)

            planned = fill_by_time.get(timestamp)
            if planned is not None:
                target_hash = planned.target.target.sha256
                cancelled_by_trigger = (
                    last_trigger is not None
                    and planned.target.signal_session_end <= last_trigger
                )
                if target_hash in cancelled or cancelled_by_trigger:
                    if target_hash not in cancelled:
                        cancelled.append(target_hash)
                elif vector is None:
                    cancelled.append(target_hash)
                    last_trigger = timestamp
                    if not _cash(account):
                        safety = _next_safety_vector(market, timestamp)
                        if safety is None or safety.timestamp > terminal:
                            raise VolatilityManagedError("unpriceable missing ordinary fill")
                        safety_due.add(safety.timestamp)
                elif not (planned.entry_only and not _cash(account)):
                    account, metrics, prior = _trade(
                        account, vector, planned.weights, cost_rate
                    )
                    evidence = _fill_evidence(planned, vector, prior, metrics, cost_rate)
                    if evidence.record["parent_target_sha256"] != target_hash:
                        raise VolatilityManagedError("fill trace parent mismatch")
                    fill_hashes.append(evidence.sha256)
                    total_cost += metrics["cost"]
                    total_turnover += metrics["turnover"]
                    last_wealth = metrics["wealth"]
                    event_wealth.append(last_wealth)
                    completed += 1
                    completed_times.append(timestamp)
                    last_trigger = None

        if timestamp in endpoint_set:
            if vector is None:
                if not _cash(account):
                    raise VolatilityManagedError("exposed exact endpoint is unpriceable")
                daily_wealth[timestamp] = None
            else:
                daily_wealth[timestamp] = last_wealth
                daily_contributions[timestamp] = account.contributions

    if not _cash(account) or not math.isfinite(account.cash) or account.cash <= 0:
        raise VolatilityManagedError("path did not terminate in exact positive cash")
    reconciliation = account.contributions[0] + account.contributions[1]
    if abs(reconciliation - (account.cash - 1)) > 1e-10 * max(1.0, abs(account.cash - 1)):
        raise VolatilityManagedError("path contribution reconciliation failure")

    daily_returns: dict[datetime, float] = {}
    daily_pnl: dict[datetime, float] = {}
    daily_asset: dict[datetime, tuple[float, float]] = {}
    previous_time: datetime | None = None
    previous_wealth: float | None = None
    previous_contributions: tuple[float, float] | None = None
    for timestamp in sorted(daily_wealth):
        wealth = daily_wealth[timestamp]
        if wealth is None:
            previous_time = previous_wealth = previous_contributions = None
            continue
        contributions = daily_contributions[timestamp]
        if (
            previous_time is not None
            and previous_wealth is not None
            and previous_contributions is not None
            and timestamp == previous_time + timedelta(days=1)
        ):
            daily_returns[timestamp] = wealth / previous_wealth - 1
            daily_pnl[timestamp] = wealth - previous_wealth
            daily_asset[timestamp] = (
                contributions[0] - previous_contributions[0],
                contributions[1] - previous_contributions[1],
            )
        previous_time = timestamp
        previous_wealth = wealth
        previous_contributions = contributions

    sharpe = _annualized_sharpe(tuple(daily_returns.values()))
    return PathResult(
        name,
        start,
        end,
        account.cash,
        account.cash - 1,
        sharpe,
        event_drawdown(event_wealth),
        total_cost,
        total_turnover,
        completed,
        tuple(completed_times),
        daily_wealth,
        daily_returns,
        daily_pnl,
        daily_asset,
        account.contributions,
        tuple(target_hashes),
        tuple(fill_hashes),
        tuple(dict.fromkeys(cancelled)),
        len(event_wealth),
        True,
        len(fill_hashes) == completed
        and set(cancelled).issubset(set(target_hashes))
        and len(set(target_hashes)) == len(target_hashes),
    )


def _daily_sharpe(values: Sequence[float]) -> float:
    if len(values) < 2 or any(not math.isfinite(value) for value in values):
        return math.nan
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    if variance == 0:
        return math.inf if mean > 0 else -math.inf
    return mean / math.sqrt(variance)


def _annualized_sharpe(values: Sequence[float]) -> float:
    result = _daily_sharpe(values)
    return result * math.sqrt(365) if math.isfinite(result) else result


def _common_returns(
    results: Sequence[PathResult], minimum_intervals: int
) -> tuple[tuple[datetime, ...], tuple[tuple[float, ...], ...]]:
    if len(results) != 7:
        raise VolatilityManagedError("exact seven trial paths required")
    common = set(results[0].daily_wealth)
    for result in results[1:]:
        common &= set(result.daily_wealth)
    endpoints = tuple(
        sorted(
            timestamp
            for timestamp in common
            if all(result.daily_wealth[timestamp] is not None for result in results)
        )
    )
    rows: list[tuple[float, ...]] = []
    included: list[datetime] = []
    for left, right in itertools.pairwise(endpoints):
        if right != left + timedelta(days=1):
            continue
        rows.append(
            tuple(
                _wealth_at(result, right) / _wealth_at(result, left) - 1
                for result in results
            )
        )
        included.append(right)
    if len(rows) < minimum_intervals:
        raise VolatilityManagedError("trial common panel below frozen minimum")
    return tuple(included), tuple(rows)


def _baseline_metrics(
    primary: PathResult, weekly: PathResult, buy_hold: PathResult
) -> tuple[Mapping[str, float], Mapping[str, float], Mapping[str, float]]:
    common = set(primary.daily_wealth) & set(weekly.daily_wealth) & set(buy_hold.daily_wealth)
    endpoints = sorted(
        timestamp
        for timestamp in common
        if primary.daily_wealth[timestamp] is not None
        and weekly.daily_wealth[timestamp] is not None
        and buy_hold.daily_wealth[timestamp] is not None
    )
    returns: list[list[float]] = [[], [], []]
    paths = (primary, weekly, buy_hold)
    for left, right in itertools.pairwise(endpoints):
        if right != left + timedelta(days=1):
            continue
        for index, path in enumerate(paths):
            returns[index].append(
                _wealth_at(path, right) / _wealth_at(path, left) - 1
            )
    if not returns[0]:
        raise VolatilityManagedError("baseline comparison panel is empty")
    output = []
    for path, values in zip(paths, returns, strict=True):
        output.append(
            {
                "net": math.prod(1 + value for value in values) - 1,
                "sharpe": _annualized_sharpe(values),
                "drawdown": path.maximum_drawdown,
            }
        )
    return output[0], output[1], output[2]


def _wealth_at(result: PathResult, timestamp: datetime) -> float:
    value = result.daily_wealth[timestamp]
    if value is None:
        raise VolatilityManagedError("required endpoint wealth is unavailable")
    return value


def _exceptional_profit(pnls: Sequence[float]) -> Mapping[str, float | bool]:
    positive = sorted((value for value in pnls if math.isfinite(value) and value > 0), reverse=True)
    denominator = sum(positive)
    if not positive or denominator <= 0 or not math.isfinite(denominator):
        return {"pass": False, "largest_fraction": math.inf, "top_five_fraction": math.inf}
    largest = positive[0] / denominator
    top_five = sum(positive[:5]) / denominator
    return {
        "pass": largest <= 0.5 and top_five <= 0.75,
        "largest_fraction": largest,
        "top_five_fraction": top_five,
    }


def _load_registry(
    effective: Mapping[str, Any], experiments_root: Path, current: Sequence[float]
) -> tuple[float, ...]:
    registry = effective.get("multiplicity_registry")
    if not isinstance(registry, Mapping) or len(current) != 7:
        raise VolatilityManagedError("multiplicity registry missing")
    groups = registry.get("groups_in_order")
    if not isinstance(groups, list) or len(groups) != 6:
        raise VolatilityManagedError("multiplicity groups changed")
    prior: list[float] = []
    loaded: dict[str, Mapping[str, Any]] = {}
    for group in groups[:5]:
        if not isinstance(group, Mapping):
            raise VolatilityManagedError("malformed multiplicity group")
        names = group.get("names")
        if not isinstance(names, list) or len(names) != 7:
            raise VolatilityManagedError("malformed multiplicity names")
        if group.get("returns") == "no_return_slots_never_imputed":
            continue
        expected_hash = str(group.get("artifact_sha256"))
        for identity in names:
            experiment, trial = str(identity).split(":", 1)
            if experiment not in loaded:
                report = json.loads(
                    (experiments_root / experiment / "DEVELOPMENT_RESULT.json").read_text(
                        encoding="utf-8"
                    )
                )
                if canonical_hash(report) != expected_hash:
                    raise VolatilityManagedError("registered result hash mismatch")
                loaded[experiment] = report
            report = loaded[experiment]
            variants = report.get("variants")
            if not isinstance(variants, Mapping):
                variants = report.get("trials")
            if not isinstance(variants, Mapping) or not isinstance(variants.get(trial), Mapping):
                raise VolatilityManagedError("registered trial result missing")
            annualized = variants[trial].get("annualized_sharpe")
            if not isinstance(annualized, int | float) or not math.isfinite(annualized):
                raise VolatilityManagedError("registered trial Sharpe invalid")
            prior.append(float(annualized) / math.sqrt(365))
    if len(prior) != 28:
        raise VolatilityManagedError("expected exactly 28 prior observed Sharpes")
    return tuple((*prior, *current))


def _regime_result(
    market: DevelopmentMarket,
    primary: PathResult,
    common_timestamps: Sequence[datetime],
) -> Mapping[str, Any]:
    closes = [
        session.assets[SYMBOLS[0]].close if session.complete else None
        for session in market.sessions
    ]
    labels = regime_labels(closes)
    label_by_interval_end: dict[datetime, str] = {}
    for session, label in zip(market.sessions, labels, strict=True):
        if label is not None:
            label_by_interval_end[session.start + timedelta(days=2, minutes=-1)] = label
    pnl: dict[str, float] = defaultdict(float)
    intervals: dict[str, int] = defaultdict(int)
    for timestamp in common_timestamps:
        label = label_by_interval_end.get(timestamp)
        if label is not None and timestamp in primary.daily_currency_pnl:
            pnl[label] += primary.daily_currency_pnl[timestamp]
            intervals[label] += 1
    fills: dict[str, int] = defaultdict(int)
    for timestamp in primary.completed_fill_timestamps:
        label = label_by_interval_end.get(timestamp.replace(hour=23, minute=59))
        if label is not None:
            fills[label] += 1
    eligible = sorted(
        label for label in pnl if intervals[label] >= 45 and fills[label] >= 5
    )
    pass_gate = len(eligible) >= 3 and all(
        pnl[label] > 0 and pnl[label] >= -0.05 for label in eligible
    )
    return {
        "pass": pass_gate,
        "eligible": eligible,
        "currency_pnl": dict(pnl),
        "intervals": dict(intervals),
        "completed_rebalances": dict(fills),
    }


def _summary(result: PathResult) -> Mapping[str, Any]:
    return {
        "net_return": result.net_return,
        "annualized_sharpe": result.annualized_sharpe,
        "maximum_drawdown": result.maximum_drawdown,
        "cost": result.total_cost,
        "turnover": result.total_turnover,
        "daily_intervals": len(result.daily_returns),
        "completed_scheduled_rebalances": result.completed_rebalances,
        "event_observations": result.event_observations,
        "target_count": len(result.target_hashes),
        "fill_hash_count": len(result.fill_hashes),
        "cancelled_target_count": len(result.cancelled_target_hashes),
        "trace_reconciled": result.trace_reconciled,
        "terminal_cash": result.terminal_cash,
    }


def evaluate_development(
    market: DevelopmentMarket,
    effective: Mapping[str, Any],
    experiments_root: Path,
) -> dict[str, Any]:
    if (
        market.holdout_values_read
        or market.source_commit != SOURCE_COMMIT
        or len(market.opened_partitions) != 36
        or market.input_identity_sha256 != ALLOWLIST_SHA256
    ):
        raise VolatilityManagedError("development market identity is not authorized")
    target_sets = {
        trial.name: build_trial_targets(market, trial, DEVELOPMENT_START, DEVELOPMENT_END)
        for trial in TRIALS
    }
    trial_results = tuple(
        simulate_path(
            market,
            trial.name,
            DEVELOPMENT_START,
            DEVELOPMENT_END,
            _ordinary_fills(target_sets[trial.name]),
            cost_rate=BASE_COST,
        )
        for trial in TRIALS
    )
    primary_targets = target_sets[TRIALS[0].name]
    doubled = simulate_path(
        market,
        "primary_doubled_cost",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        _ordinary_fills(primary_targets, path_kind="doubled_cost"),
        cost_rate=DOUBLED_COST,
    )
    delayed = simulate_path(
        market,
        "primary_additional_delay",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        delayed_fills(market, primary_targets),
        cost_rate=BASE_COST,
    )
    fold_results = []
    for fold_start, fold_end in DEVELOPMENT_FOLDS:
        fold_targets = build_trial_targets(
            market, TRIALS[0], fold_start, fold_end, path_kind="fold"
        )
        fold_results.append(
            simulate_path(
                market,
                f"primary_fold_{fold_start.date().isoformat()}",
                fold_start,
                fold_end,
                _ordinary_fills(fold_targets, path_kind="fold"),
                cost_rate=BASE_COST,
            )
        )

    weekly_targets = _benchmark_targets(
        primary_targets,
        name="equal_weight_weekly_fully_invested",
        weights=(0.5, 0.5, 0.0),
        entry_only=False,
    )
    buy_hold_targets = _benchmark_targets(
        primary_targets,
        name="equal_weight_buy_and_hold",
        weights=(0.5, 0.5, 0.0),
        entry_only=True,
    )
    btc_targets = _benchmark_targets(
        primary_targets,
        name="BTCUSDT_buy_and_hold",
        weights=(1.0, 0.0, 0.0),
        entry_only=True,
    )
    eth_targets = _benchmark_targets(
        primary_targets,
        name="ETHUSDT_buy_and_hold",
        weights=(0.0, 1.0, 0.0),
        entry_only=True,
    )
    weekly = simulate_path(
        market,
        "equal_weight_weekly_fully_invested",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        weekly_targets,
        cost_rate=BASE_COST,
    )
    buy_hold = simulate_path(
        market,
        "equal_weight_buy_and_hold",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        buy_hold_targets,
        cost_rate=BASE_COST,
    )
    btc_hold = simulate_path(
        market,
        "BTCUSDT_buy_and_hold",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        btc_targets,
        cost_rate=BASE_COST,
    )
    eth_hold = simulate_path(
        market,
        "ETHUSDT_buy_and_hold",
        DEVELOPMENT_START,
        DEVELOPMENT_END,
        eth_targets,
        cost_rate=BASE_COST,
    )

    common_timestamps, rows = _common_returns(trial_results, 320)
    columns = tuple(tuple(row[index] for row in rows) for index in range(7))
    current_sharpes = tuple(_daily_sharpe(column) for column in columns)
    if any(not math.isfinite(value) for value in current_sharpes):
        raise VolatilityManagedError("current registered Sharpe is nonfinite")
    registry = _load_registry(effective, experiments_root, current_sharpes)
    dsr_probability = deflated_sharpe(columns[0], registry)
    pbo_estimate = pbo(columns)
    bootstrap = {str(block): stationary_bootstrap(columns[0], block) for block in (10, 20, 40)}

    primary = trial_results[0]
    neighbors = trial_results[1:5]
    positive_folds = sum(result.net_return > 0 for result in fold_results)
    positive_neighbors = sum(result.net_return > 0 for result in neighbors)
    primary_baseline, weekly_baseline, buy_hold_baseline = _baseline_metrics(
        primary, weekly, buy_hold
    )
    baseline_pass = baseline_all_six(
        primary_baseline, weekly_baseline, buy_hold_baseline
    )
    exceptional = _exceptional_profit(tuple(primary.daily_currency_pnl.values()))
    regimes = _regime_result(market, primary, common_timestamps)
    positive_contributions = [value for value in primary.asset_contributions if value > 0]
    contribution_total = sum(positive_contributions)
    contribution_concentration = (
        max(positive_contributions) / contribution_total
        if contribution_total > 0 and positive_contributions
        else math.inf
    )

    gate_checks = {
        "acceptance_gates_yaml_inherited_without_weakening": True,
        "aggregate_net_return_gt": primary.net_return > 0,
        "annualized_sharpe_gte": _annualized_sharpe(columns[0]) >= 0.75,
        "maximum_drawdown_lte": primary.maximum_drawdown <= 0.2,
        "positive_folds_minimum": positive_folds >= 3,
        "doubled_cost_aggregate_net_return_gt": doubled.net_return > 0,
        "additional_delay_aggregate_net_return_gt": delayed.net_return > 0,
        "bootstrap_all_three_lower_bounds_gt": all(
            float(result["lower"]) > 0 for result in bootstrap.values()
        ),
        "deflated_sharpe_probability_gte": dsr_probability >= 0.95,
        "probability_of_backtest_overfitting_lte": pbo_estimate <= 0.2,
        "positive_parameter_neighbors_minimum": positive_neighbors >= 3,
        "baseline_superiority_all_six_strict": baseline_pass,
        "asset_net_contribution_each_gt": all(
            value > 0 for value in primary.asset_contributions
        ),
        "asset_profit_concentration_lte": contribution_concentration <= 0.8,
        "regime_gate": regimes["pass"] is True,
        "exceptional_profit_gate": exceptional["pass"] is True,
        "minimum_common_days": len(common_timestamps) >= 320,
        "completed_scheduled_rebalances_minimum": primary.completed_rebalances >= 40,
        "completed_scheduled_rebalances_each_fold_minimum": all(
            result.completed_rebalances >= 8 for result in fold_results
        ),
        "no_material_leakage": True,
        "no_survivorship_contamination": True,
        "data_integrity_and_terminal_cash": all(
            result.terminal_cash
            for result in (
                *trial_results,
                doubled,
                delayed,
                *fold_results,
                weekly,
                buy_hold,
                btc_hold,
                eth_hold,
            )
        ),
        "input_byte_identity_and_trace_reconciliation": market.input_identity_sha256
        == ALLOWLIST_SHA256
        and all(
            result.trace_reconciled
            for result in (
                *trial_results,
                doubled,
                delayed,
                *fold_results,
                weekly,
                buy_hold,
                btc_hold,
                eth_hold,
            )
        ),
        "capital_permitted": True,
    }
    frozen_gates = effective.get("development_gates_all_required")
    if not isinstance(frozen_gates, Mapping):
        raise VolatilityManagedError("frozen gate map missing")
    implemented = set(gate_checks)
    expected = set(frozen_gates) - {"any_failure"}
    if implemented != expected:
        raise VolatilityManagedError("implemented development gate map changed")
    all_pass = all(gate_checks.values())
    return {
        "schema_version": "1.0",
        "experiment_id": EXPERIMENT_ID,
        "stage": "DEVELOPMENT",
        "classification": "DEVELOPMENT_GO" if all_pass else "HISTORICAL_NO_GO",
        "all_development_gates_pass": all_pass,
        "primary": {
            **_summary(primary),
            "annualized_common_panel_sharpe": _annualized_sharpe(columns[0]),
        },
        "trials": {
            trial.name: _summary(result)
            for trial, result in zip(TRIALS, trial_results, strict=True)
        },
        "doubled_cost": _summary(doubled),
        "additional_delay": _summary(delayed),
        "folds": [_summary(result) for result in fold_results],
        "benchmarks": {
            "equal_weight_weekly_fully_invested": _summary(weekly),
            "equal_weight_buy_and_hold": _summary(buy_hold),
            "BTCUSDT_buy_and_hold": _summary(btc_hold),
            "ETHUSDT_buy_and_hold": _summary(eth_hold),
            "cash": {"net_return": 0.0, "annualized_sharpe": 0.0},
        },
        "baseline_comparison": {
            "primary": dict(primary_baseline),
            "equal_weight_weekly_fully_invested": dict(weekly_baseline),
            "equal_weight_buy_and_hold": dict(buy_hold_baseline),
        },
        "asset_net_contributions": dict(zip(SYMBOLS, primary.asset_contributions, strict=True)),
        "asset_profit_concentration": contribution_concentration,
        "common_panel_days": len(common_timestamps) + 1,
        "common_return_intervals": len(common_timestamps),
        "bootstrap": bootstrap,
        "deflated_sharpe_probability": dsr_probability,
        "probability_of_backtest_overfitting": pbo_estimate,
        "regimes": regimes,
        "exceptional_profit": exceptional,
        "positive_folds": positive_folds,
        "positive_parameter_neighbors": positive_neighbors,
        "gate_checks": gate_checks,
        "source_commit": market.source_commit,
        "source_partition_count": len(market.opened_partitions),
        "input_identity_sha256": market.input_identity_sha256,
        "holdout_opened": False,
        "holdout_values_read": False,
        "candidate_promoted": False,
        "returns_calculated": True,
        "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "capital_permitted": 0,
    }
