from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from strategy_control import trend_data


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture_source(tmp_path: Path) -> tuple[Path, str, str, list[Path]]:
    source = tmp_path / "direction"
    dataset_root = source / trend_data.DATASET_ROOT_RELATIVE
    inventory: list[dict[str, object]] = []
    development_paths: list[Path] = []
    for symbol in ("BTCUSDT", "ETHUSDT"):
        for year, months in (
            (2024, range(7, 13)),
            (2025, range(1, 13)),
            (2026, range(1, 7)),
        ):
            for month in months:
                relative = (
                    f"canonical/venue=binance/symbol={symbol}/year={year:04d}/"
                    f"month={month:02d}/observations.parquet"
                )
                path = dataset_root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(relative.encode())
                inventory.append(
                    {"path": relative, "bytes": path.stat().st_size, "sha256": _sha(path)}
                )
                if year < 2026:
                    development_paths.append(path)
    completeness = {
        symbol: {"duplicates": 0, "missing_timestamps": 2}
        for symbol in ("BTCUSDT", "ETHUSDT")
    }
    quality = {
        name: {"status": "pass"}
        for name in (
            "canonical_data_integrity",
            "causal_feature_validity",
            "exact_label_validity",
        )
    }
    freeze_path = source / trend_data.FREEZE_MANIFEST_RELATIVE
    freeze_path.parent.mkdir(parents=True, exist_ok=True)
    freeze_path.write_text(
        json.dumps(
            {
                "repository_commit": "source",
                "file_inventory_sha256_manifest": inventory,
                "completeness_by_series": completeness,
                "quality_gates": quality,
            }
        )
    )
    split_path = source / trend_data.SPLIT_MANIFEST_RELATIVE
    split_path.parent.mkdir(parents=True, exist_ok=True)
    split_path.write_text(
        json.dumps(
            {
                "dataset_id": trend_data.DATASET_ID,
                "freeze_status": "frozen",
                "splits": {
                    "holdout": {
                        "calendar_start_utc": "2026-01-01T00:00:00+00:00",
                        "calendar_end_exclusive_utc": "2026-07-01T00:00:00+00:00",
                    }
                },
            }
        )
    )
    return source, _sha(freeze_path), _sha(split_path), development_paths


def test_trend_data_verification_never_parses_holdout_parquet(tmp_path: Path) -> None:
    source, freeze_hash, split_hash, development_paths = _fixture_source(tmp_path)
    schema_calls: list[Path] = []

    def schema_reader(path: Path) -> tuple[str, ...]:
        schema_calls.append(path)
        return trend_data.EXPECTED_COLUMNS

    report = trend_data.verify_trend_data(
        source,
        expected_freeze_sha256=freeze_hash,
        expected_split_sha256=split_hash,
        schema_reader=schema_reader,
        observed_at_utc="2026-08-02T00:00:00Z",
    )
    assert report["status"] == "PASS"
    assert report["canonical_partition_count"] == 48
    assert report["development_schema_metadata_checks"] == 36
    assert report["holdout_byte_hash_only_checks"] == 12
    assert report["holdout_parquet_footers_or_values_read"] is False
    assert schema_calls == development_paths


def test_trend_data_verification_fails_on_partition_mutation(tmp_path: Path) -> None:
    source, freeze_hash, split_hash, development_paths = _fixture_source(tmp_path)
    development_paths[0].write_bytes(b"mutated")
    with pytest.raises(trend_data.TrendDataError, match=r"size mismatch|hash mismatch"):
        trend_data.verify_trend_data(
            source,
            expected_freeze_sha256=freeze_hash,
            expected_split_sha256=split_hash,
            schema_reader=lambda _: trend_data.EXPECTED_COLUMNS,
        )


def test_partition_identity_rejects_non_allowlisted_path() -> None:
    with pytest.raises(trend_data.TrendDataError, match="unexpected"):
        trend_data.partition_identity("canonical/venue=other/symbol=BTCUSDT/file.parquet")
