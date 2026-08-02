from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from strategy_control.archive_audit import canonical_sha256

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT = ROOT / "experiments" / "cs-ranking-binance-spot-archive-ptu-audit-v1"


def load_manifest() -> dict[str, object]:
    value = json.loads((EXPERIMENT / "SYMBOL_MANIFEST.json").read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_archive_manifest_hash_and_required_fields_are_reproducible() -> None:
    manifest = load_manifest()
    expected = str(manifest.pop("manifest_sha256"))
    assert canonical_sha256(manifest) == expected
    rows = manifest["symbols"]
    assert isinstance(rows, list) and len(rows) >= 25
    required = {
        "symbol",
        "episode_id",
        "first_valid_bar_open_time",
        "last_valid_bar_open_time",
        "observed_months",
        "missing_months",
        "unexplained_gaps",
        "uncertainty_status",
        "source_urls",
        "source_hashes",
    }
    assert all(isinstance(row, dict) and required <= row.keys() for row in rows)


def test_archive_manifest_has_unique_episodes_and_no_current_metadata_inputs() -> None:
    manifest = load_manifest()
    rows = manifest["symbols"]
    assert isinstance(rows, list)
    symbols = [str(row["symbol"]) for row in rows]
    episodes = [str(row["episode_id"]) for row in rows]
    assert len(symbols) == len(set(symbols))
    assert len(episodes) == len(set(episodes))
    assert manifest["current_exchange_info_requests"] == 0
    assert manifest["market_capitalization_inputs"] == 0
    assert manifest["end_of_sample_survival_filter"] is False


def test_archive_manifest_retains_historical_non_survivors_and_quarantines_uncertainty() -> None:
    manifest = load_manifest()
    rows = manifest["symbols"]
    assert isinstance(rows, list)
    sample_end = datetime(2026, 6, 30, tzinfo=UTC)
    validated = [row for row in rows if row["last_valid_bar_open_time"] is not None]
    assert validated
    assert any(
        datetime.fromisoformat(str(row["last_valid_bar_open_time"]).replace("Z", "+00:00"))
        < sample_end
        for row in validated
    )
    uncertain = [row for row in rows if str(row["uncertainty_status"]).startswith("QUARANTINED")]
    assert uncertain
    assert all(row["uncertainty_status"] != "CLEAR" for row in uncertain)


def test_archive_manifest_provenance_is_official_and_publication_safe() -> None:
    manifest = load_manifest()
    root_pages = manifest["root_listing_pages"]
    assert isinstance(root_pages, list) and root_pages
    assert all(
        str(page["request_url"]).startswith(
            "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?"
        )
        and page["http_status"] == 200
        for page in root_pages
    )
    rows = manifest["symbols"]
    assert isinstance(rows, list)
    urls = [str(url) for row in rows for url in row["source_urls"]]
    assert urls
    assert all(url.startswith("https://data.binance.vision/") for url in urls)
    serialized = json.dumps(manifest, ensure_ascii=False)
    assert "/home/" not in serialized
    assert "exchangeInfo" not in serialized
