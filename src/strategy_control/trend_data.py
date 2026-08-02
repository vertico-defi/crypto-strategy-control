"""Deterministic, holdout-safe verification of the frozen BTC/ETH source dataset."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DATASET_ID = "historical-v2-pathc-20260723T175155Z"
FREEZE_MANIFEST_RELATIVE = Path(
    "data/frozen/historical-v2-pathc-20260723T175155Z/"
    "DATASET_FREEZE_MANIFEST_historical-v2-pathc-20260723T175155Z.json"
)
SPLIT_MANIFEST_RELATIVE = Path(
    "data/frozen/historical-v2-pathc-20260723T175155Z/splits/"
    "CHRONOLOGICAL_SPLIT_MANIFEST_historical-v2-pathc-20260723T175155Z.json"
)
DATASET_ROOT_RELATIVE = Path("data/real/historical-v2-pathc-20260723T175155Z")
EXPECTED_FREEZE_SHA256 = "243d875979df2991ef3c941d06e13d608c30e44df0eab512afdbb3fb6b0a07ad"
EXPECTED_SPLIT_SHA256 = "2d06417526ecc6e0eca7938440669a78d83eb7f8df833c66d17a75e590e43753"
EXPECTED_COLUMNS = (
    "venue",
    "symbol",
    "event_timestamp",
    "available_timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "mark_price",
    "index_price",
    "source_provenance",
    "source_record_id",
)
PARTITION_PATTERN = re.compile(
    r"^canonical/venue=binance/symbol=(BTCUSDT|ETHUSDT)/"
    r"year=(2024|2025|2026)/month=(\d{2})/observations\.parquet$"
)


class TrendDataError(RuntimeError):
    """Raised when the frozen source contract cannot be reproduced."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TrendDataError(f"cannot read frozen metadata: {path.name}") from exc
    if not isinstance(value, dict):
        raise TrendDataError(f"frozen metadata is not an object: {path.name}")
    return value


def partition_identity(relative: str) -> tuple[str, int, int]:
    """Parse one allowlisted canonical partition path."""

    match = PARTITION_PATTERN.fullmatch(relative)
    if match is None:
        raise TrendDataError(f"unexpected canonical partition path: {relative}")
    symbol, year, month = match.groups()
    month_number = int(month)
    if month_number < 1 or month_number > 12:
        raise TrendDataError(f"invalid canonical partition month: {relative}")
    return symbol, int(year), month_number


def _schema_names(path: Path) -> tuple[str, ...]:
    parquet = importlib.import_module("pyarrow.parquet")
    parquet_file = parquet.ParquetFile(path)
    return tuple(str(name) for name in parquet_file.schema_arrow.names)


def verify_trend_data(
    source_repository: Path,
    *,
    expected_freeze_sha256: str = EXPECTED_FREEZE_SHA256,
    expected_split_sha256: str = EXPECTED_SPLIT_SHA256,
    schema_reader: Callable[[Path], tuple[str, ...]] = _schema_names,
    observed_at_utc: str | None = None,
) -> dict[str, Any]:
    """Verify hashes and development schemas without parsing holdout values or footers."""

    freeze_path = source_repository / FREEZE_MANIFEST_RELATIVE
    split_path = source_repository / SPLIT_MANIFEST_RELATIVE
    freeze_hash = _sha256_file(freeze_path)
    split_hash = _sha256_file(split_path)
    if freeze_hash != expected_freeze_sha256:
        raise TrendDataError("dataset freeze manifest hash mismatch")
    if split_hash != expected_split_sha256:
        raise TrendDataError("chronological split manifest hash mismatch")
    freeze = _load_object(freeze_path)
    split = _load_object(split_path)
    if split.get("dataset_id") != DATASET_ID or split.get("freeze_status") != "frozen":
        raise TrendDataError("chronological split identity or freeze status mismatch")
    splits = split.get("splits")
    if not isinstance(splits, dict):
        raise TrendDataError("chronological split definitions missing")
    holdout = splits.get("holdout")
    if not isinstance(holdout, dict) or (
        holdout.get("calendar_start_utc") != "2026-01-01T00:00:00+00:00"
        or holdout.get("calendar_end_exclusive_utc") != "2026-07-01T00:00:00+00:00"
    ):
        raise TrendDataError("holdout boundary mismatch")

    inventory = freeze.get("file_inventory_sha256_manifest")
    if not isinstance(inventory, list):
        raise TrendDataError("frozen file inventory missing")
    canonical = [item for item in inventory if str(item.get("path", "")).startswith("canonical/")]
    if len(canonical) != 48:
        raise TrendDataError(f"expected 48 canonical partitions, observed {len(canonical)}")

    seen: set[tuple[str, int, int]] = set()
    verified: list[dict[str, Any]] = []
    development_schema_checks = 0
    holdout_hash_only_checks = 0
    data_root = source_repository / DATASET_ROOT_RELATIVE
    for item in canonical:
        relative = str(item.get("path"))
        expected_hash = str(item.get("sha256"))
        expected_bytes = item.get("bytes")
        identity = partition_identity(relative)
        if identity in seen:
            raise TrendDataError(f"duplicate canonical partition identity: {relative}")
        seen.add(identity)
        path = data_root / relative
        if not path.is_file():
            raise TrendDataError(f"canonical partition missing: {relative}")
        if not isinstance(expected_bytes, int) or path.stat().st_size != expected_bytes:
            raise TrendDataError(f"canonical partition size mismatch: {relative}")
        observed_hash = _sha256_file(path)
        if observed_hash != expected_hash:
            raise TrendDataError(f"canonical partition hash mismatch: {relative}")
        symbol, year, month = identity
        if year < 2026:
            if schema_reader(path) != EXPECTED_COLUMNS:
                raise TrendDataError(f"development partition schema mismatch: {relative}")
            development_schema_checks += 1
            verification_scope = "HASH_AND_SCHEMA_METADATA_ONLY"
        else:
            holdout_hash_only_checks += 1
            verification_scope = "BYTE_HASH_ONLY_NO_PARQUET_PARSE"
        verified.append(
            {
                "symbol": symbol,
                "month": f"{year:04d}-{month:02d}",
                "relative_path": relative,
                "sha256": observed_hash,
                "bytes": expected_bytes,
                "verification_scope": verification_scope,
            }
        )
    expected_identities = {
        (symbol, year, month)
        for symbol in ("BTCUSDT", "ETHUSDT")
        for year, months in ((2024, range(7, 13)), (2025, range(1, 13)), (2026, range(1, 7)))
        for month in months
    }
    if seen != expected_identities:
        raise TrendDataError("canonical partition month coverage mismatch")

    completeness = freeze.get("completeness_by_series")
    quality_gates = freeze.get("quality_gates")
    if not isinstance(completeness, dict) or set(completeness) != {"BTCUSDT", "ETHUSDT"}:
        raise TrendDataError("source completeness evidence missing")
    if not isinstance(quality_gates, dict):
        raise TrendDataError("source quality-gate evidence missing")
    for symbol in ("BTCUSDT", "ETHUSDT"):
        evidence = completeness[symbol]
        if not isinstance(evidence, dict):
            raise TrendDataError(f"source completeness malformed: {symbol}")
        if evidence.get("duplicates") != 0 or evidence.get("missing_timestamps") != 2:
            raise TrendDataError(f"unexpected frozen completeness result: {symbol}")
    required_passes = (
        "canonical_data_integrity",
        "causal_feature_validity",
        "exact_label_validity",
    )
    if any(
        not isinstance(quality_gates.get(name), dict)
        or quality_gates[name].get("status") != "pass"
        for name in required_passes
    ):
        raise TrendDataError("required frozen quality gate is not pass")

    inventory_digest = hashlib.sha256(
        json.dumps(verified, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-vol-targeted-trend-v1",
        "dataset_id": DATASET_ID,
        "status": "PASS",
        "observed_at_utc": observed_at_utc
        or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "dataset_freeze_manifest_sha256": freeze_hash,
        "chronological_split_manifest_sha256": split_hash,
        "source_repository_commit_in_freeze_manifest": freeze.get("repository_commit"),
        "canonical_partition_count": len(verified),
        "development_schema_metadata_checks": development_schema_checks,
        "holdout_byte_hash_only_checks": holdout_hash_only_checks,
        "holdout_parquet_footers_or_values_read": False,
        "holdout_opened": False,
        "returns_calculated": False,
        "performance_claim_made": False,
        "known_missing_minutes_per_asset": 2,
        "known_gap_rule": "quarantine_incomplete_session_and_reset_150_completed_sessions",
        "canonical_inventory_sha256": inventory_digest,
        "partitions": verified,
        "capital_permitted": 0,
    }
