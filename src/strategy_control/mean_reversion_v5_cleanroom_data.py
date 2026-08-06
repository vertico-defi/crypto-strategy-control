"""Manifest-bound, development-only loader for the v5 clean-room evaluator."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .mean_reversion_v5_cleanroom import ASSETS, CleanRow

DATASET_ID = "historical-v2-pathc-20260723T175155Z"
EXPECTED_FREEZE_SHA256 = "243d875979df2991ef3c941d06e13d608c30e44df0eab512afdbb3fb6b0a07ad"
EXPECTED_SOURCE_COMMIT = "d1d6066a6042b0c2e1c6af75047f5ebf935c739f"
EXPECTED_ALLOWLIST_COUNT = 36
EXPECTED_ALLOWLIST_SHA256 = "40bb5cf5b7bd3a8ac30e2a3b1d022462fe45888790b1ba58a7068a1982cdc6bd"
EXPECTED_COLUMNS = (
    "event_timestamp",
    "available_timestamp",
    "source_provenance",
    "open",
    "high",
    "low",
    "close",
    "volume",
)
_PATH_RE = re.compile(
    r"^canonical/venue=binance/symbol=(BTCUSDT|ETHUSDT)/"
    r"year=(2024|2025)/month=(\d{2})/observations\.parquet$"
)


class CleanRoomDataError(ValueError):
    """A development identity, schema, or causal-boundary invariant failed."""


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise CleanRoomDataError(f"metadata is not an object: {path}")
    return value


def _development_inventory(freeze: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    inventory = freeze.get("file_inventory_sha256_manifest")
    if not isinstance(inventory, list):
        raise CleanRoomDataError("frozen inventory is missing")
    selected: list[dict[str, Any]] = []
    for item in inventory:
        if not isinstance(item, dict):
            raise CleanRoomDataError("malformed inventory item")
        relative = item.get("path")
        if not isinstance(relative, str):
            raise CleanRoomDataError("inventory path is not text")
        match = _PATH_RE.fullmatch(relative)
        if match is None:
            # Holdout and noncanonical paths are deliberately never resolved.
            continue
        symbol, year, month = match.groups()
        if not 1 <= int(month) <= 12:
            raise CleanRoomDataError("invalid development month")
        selected.append(
            {
                "bytes": item.get("bytes"),
                "relative_path": relative,
                "sha256": item.get("sha256"),
                "symbol": symbol,
                "month": f"{year}-{month}",
            }
        )
    ordered = tuple(sorted(selected, key=lambda item: str(item["relative_path"])))
    allowlist = json.dumps(ordered, sort_keys=True, separators=(",", ":")).encode()
    if (
        len(ordered) != EXPECTED_ALLOWLIST_COUNT
        or _sha256_bytes(allowlist) != EXPECTED_ALLOWLIST_SHA256
    ):
        raise CleanRoomDataError("development allowlist identity mismatch")
    return ordered


def load_development_rows(
    source_repository: Path,
    *,
    selected_months: Iterable[str] | None = None,
) -> tuple[tuple[CleanRow, ...], dict[str, Any]]:
    """Verify and parse only pre-2026 partitions; no holdout path is resolved."""
    freeze_path = source_repository / "data/frozen" / DATASET_ID / (
        f"DATASET_FREEZE_MANIFEST_{DATASET_ID}.json"
    )
    if _sha256_file(freeze_path) != EXPECTED_FREEZE_SHA256:
        raise CleanRoomDataError("freeze manifest hash mismatch")
    freeze = _load_json(freeze_path)
    if freeze.get("repository_commit") != EXPECTED_SOURCE_COMMIT:
        raise CleanRoomDataError("source commit mismatch")
    selected = set(selected_months) if selected_months is not None else None
    inventory = _development_inventory(freeze)
    chosen = tuple(item for item in inventory if selected is None or item["month"] in selected)
    if not chosen:
        raise CleanRoomDataError("no development partitions selected")
    rows: list[CleanRow] = []
    import importlib

    parquet = importlib.import_module("pyarrow.parquet")
    pyarrow = importlib.import_module("pyarrow")
    for item in chosen:
        relative = str(item["relative_path"])
        match = _PATH_RE.fullmatch(relative)
        if match is None:
            raise CleanRoomDataError("non-development path reached parser")
        symbol = str(item["symbol"])
        path = source_repository / "data/real" / DATASET_ID / relative
        if not path.is_file() or path.stat().st_size != item["bytes"]:
            raise CleanRoomDataError(f"development file identity mismatch: {relative}")
        raw = path.read_bytes()
        if _sha256_bytes(raw) != item["sha256"]:
            raise CleanRoomDataError(f"development hash mismatch: {relative}")
        table = parquet.read_table(pyarrow.BufferReader(raw), columns=list(EXPECTED_COLUMNS))
        if tuple(str(name) for name in table.column_names) != EXPECTED_COLUMNS:
            raise CleanRoomDataError(f"schema mismatch: {relative}")
        columns = {name: table[name].to_pylist() for name in EXPECTED_COLUMNS}
        for row_index, timestamp in enumerate(columns["event_timestamp"]):
            if not isinstance(timestamp, datetime):
                raise CleanRoomDataError("event timestamp is not datetime")
            timestamp = timestamp.astimezone(UTC)
            rows.append(
                CleanRow(
                    symbol,
                    timestamp,
                    float(columns["close"][row_index]),
                    relative,
                    row_index,
                )
            )
    return tuple(rows), {
        "dataset_id": DATASET_ID,
        "development_allowlist_count": len(inventory),
        "selected_partition_count": len(chosen),
        "selected_partitions": tuple(str(item["relative_path"]) for item in chosen),
        "row_counts": {asset: sum(row.asset == asset for row in rows) for asset in ASSETS},
        "holdout_path_resolution_count": 0,
        "holdout_opened": False,
    }
