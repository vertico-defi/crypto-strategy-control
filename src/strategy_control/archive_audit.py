"""Official Binance archive enumeration for a causal, archive-observed universe.

This module has no exchange API, credential, wallet, order, or strategy-return
surface.  It retrieves public archive metadata and boundary files only.  Raw
ZIP/XML responses remain in memory; committed artifacts contain hashes and
sanitized provenance.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from strategy_control.archive_universe import normalize_open_time

BUCKET_ENDPOINT = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DOWNLOAD_ORIGIN = "https://data.binance.vision"
ROOT_PREFIX = "data/spot/monthly/klines/"
MONTH_PATTERN = re.compile(r"^(?P<symbol>[A-Z0-9]+)-1d-(?P<month>\d{4}-\d{2})\.zip$")


class ArchiveAuditError(RuntimeError):
    """Raised when public archive evidence violates the frozen contract."""


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def bytes_sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@dataclass(frozen=True)
class FetchResult:
    data: bytes
    status: int
    retrieved_at_utc: str


@dataclass(frozen=True)
class ObjectRecord:
    key: str
    etag: str
    size: int
    last_modified: str


@dataclass(frozen=True)
class ListingPage:
    request_url: str
    response_sha256: str
    retrieved_at_utc: str
    http_status: int
    prefix: str
    marker: str
    next_marker: str | None
    is_truncated: bool
    common_prefixes: tuple[str, ...]
    objects: tuple[ObjectRecord, ...]


@dataclass(frozen=True)
class ListingSnapshot:
    pages: tuple[ListingPage, ...]
    common_prefixes: tuple[str, ...]
    objects: tuple[ObjectRecord, ...]


def default_fetch(url: str, *, timeout_seconds: int = 30) -> FetchResult:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "crypto-strategy-control/0.1 archive-audit"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status = int(response.status)
            data = response.read()
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        raise ArchiveAuditError(f"public archive request failed for {url}: {exc}") from exc
    if status != 200:
        raise ArchiveAuditError(f"unexpected HTTP status {status} for {url}")
    return FetchResult(data=data, status=status, retrieved_at_utc=_now())


class ArchiveClient:
    """Minimal S3 v1 listing and public-object client with injected fetching."""

    def __init__(self, fetch: Callable[[str], FetchResult] = default_fetch) -> None:
        self.fetch = fetch

    def list_prefix(self, prefix: str, *, delimiter: str | None = None) -> ListingSnapshot:
        marker = ""
        pages: list[ListingPage] = []
        prefixes: list[str] = []
        objects: list[ObjectRecord] = []
        seen_markers: set[str] = set()
        while True:
            if marker in seen_markers:
                raise ArchiveAuditError(f"listing marker loop for prefix {prefix}")
            seen_markers.add(marker)
            query: dict[str, str | int] = {"prefix": prefix, "max-keys": 1000}
            if delimiter is not None:
                query["delimiter"] = delimiter
            if marker:
                query["marker"] = marker
            url = f"{BUCKET_ENDPOINT}?{urllib.parse.urlencode(query)}"
            result = self.fetch(url)
            page = parse_listing_page(
                result,
                request_url=url,
                expected_prefix=prefix,
                expected_marker=marker,
            )
            pages.append(page)
            prefixes.extend(page.common_prefixes)
            objects.extend(page.objects)
            if not page.is_truncated:
                break
            if not page.next_marker:
                raise ArchiveAuditError(f"truncated listing omitted NextMarker for {prefix}")
            if page.next_marker <= marker:
                raise ArchiveAuditError(f"non-increasing NextMarker for {prefix}")
            marker = page.next_marker
        unique_prefixes = tuple(sorted(set(prefixes)))
        unique_objects = deduplicate_objects(objects)
        return ListingSnapshot(tuple(pages), unique_prefixes, unique_objects)

    def get_object(self, key: str) -> tuple[str, FetchResult]:
        quoted = urllib.parse.quote(key, safe="/")
        url = f"{DOWNLOAD_ORIGIN}/{quoted}"
        return url, self.fetch(url)


def parse_listing_page(
    result: FetchResult,
    *,
    request_url: str,
    expected_prefix: str,
    expected_marker: str,
) -> ListingPage:
    try:
        root = ElementTree.fromstring(result.data)
    except ElementTree.ParseError as exc:
        raise ArchiveAuditError("invalid S3 listing XML") from exc
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

    def text(path: str, default: str = "") -> str:
        node = root.find(path, namespace)
        return node.text if node is not None and node.text is not None else default

    if text("s3:Name") != "data.binance.vision":
        raise ArchiveAuditError("unexpected S3 bucket name")
    prefix = text("s3:Prefix")
    marker = text("s3:Marker")
    if prefix != expected_prefix or marker != expected_marker:
        raise ArchiveAuditError("S3 response prefix or marker mismatch")
    truncated_text = text("s3:IsTruncated").lower()
    if truncated_text not in {"true", "false"}:
        raise ArchiveAuditError("invalid IsTruncated value")
    common_prefixes = tuple(
        node.text or ""
        for node in root.findall("s3:CommonPrefixes/s3:Prefix", namespace)
    )
    objects: list[ObjectRecord] = []
    for node in root.findall("s3:Contents", namespace):
        key = node.findtext("s3:Key", default="", namespaces=namespace)
        etag = node.findtext("s3:ETag", default="", namespaces=namespace).strip('"')
        size_text = node.findtext("s3:Size", default="-1", namespaces=namespace)
        modified = node.findtext("s3:LastModified", default="", namespaces=namespace)
        try:
            size = int(size_text)
        except ValueError as exc:
            raise ArchiveAuditError(f"invalid object size for {key}") from exc
        if not key.startswith(prefix) or size < 0:
            raise ArchiveAuditError(f"invalid object record for {key}")
        objects.append(ObjectRecord(key=key, etag=etag, size=size, last_modified=modified))
    next_marker = text("s3:NextMarker") or None
    return ListingPage(
        request_url=request_url,
        response_sha256=bytes_sha256(result.data),
        retrieved_at_utc=result.retrieved_at_utc,
        http_status=result.status,
        prefix=prefix,
        marker=marker,
        next_marker=next_marker,
        is_truncated=truncated_text == "true",
        common_prefixes=common_prefixes,
        objects=tuple(objects),
    )


def deduplicate_objects(objects: Iterable[ObjectRecord]) -> tuple[ObjectRecord, ...]:
    by_key: dict[str, ObjectRecord] = {}
    for item in objects:
        prior = by_key.get(item.key)
        if prior is not None and prior != item:
            raise ArchiveAuditError(f"conflicting duplicate object key: {item.key}")
        by_key[item.key] = item
    return tuple(by_key[key] for key in sorted(by_key))


def enumerate_usdt_symbols(snapshot: ListingSnapshot) -> tuple[str, ...]:
    symbols: list[str] = []
    for prefix in snapshot.common_prefixes:
        if not prefix.startswith(ROOT_PREFIX) or not prefix.endswith("/"):
            raise ArchiveAuditError(f"unexpected symbol prefix: {prefix}")
        symbol = prefix[len(ROOT_PREFIX) : -1]
        if "/" in symbol or not symbol:
            raise ArchiveAuditError(f"invalid archive symbol directory: {prefix}")
        if symbol.endswith("USDT"):
            symbols.append(symbol)
    if len(symbols) != len(set(symbols)):
        raise ArchiveAuditError("duplicate USDT symbol directory")
    return tuple(sorted(symbols))


def month_range(first: str, last: str) -> tuple[str, ...]:
    year, month = (int(value) for value in first.split("-"))
    last_year, last_month = (int(value) for value in last.split("-"))
    values: list[str] = []
    while (year, month) <= (last_year, last_month):
        values.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return tuple(values)


def select_monthly_archives(
    symbol: str,
    objects: Iterable[ObjectRecord],
    *,
    first_month: str,
    last_month: str,
) -> dict[str, dict[str, ObjectRecord]]:
    selected: dict[str, dict[str, ObjectRecord]] = {}
    prefix = f"{ROOT_PREFIX}{symbol}/1d/"
    for item in objects:
        if not item.key.startswith(prefix):
            raise ArchiveAuditError(f"object escaped symbol prefix: {item.key}")
        filename = item.key.removeprefix(prefix)
        checksum = filename.endswith(".CHECKSUM")
        zip_filename = filename.removesuffix(".CHECKSUM") if checksum else filename
        match = MONTH_PATTERN.fullmatch(zip_filename)
        if match is None or match.group("symbol") != symbol:
            continue
        month = match.group("month")
        if not first_month <= month <= last_month:
            continue
        slot = selected.setdefault(month, {})
        kind = "checksum" if checksum else "zip"
        if kind in slot and slot[kind] != item:
            raise ArchiveAuditError(f"conflicting {kind} for {symbol} {month}")
        slot[kind] = item
    return selected


def verify_checksum(zip_bytes: bytes, checksum_bytes: bytes, *, filename: str) -> str:
    try:
        line = checksum_bytes.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ArchiveAuditError(f"non-UTF8 checksum for {filename}") from exc
    parts = line.split()
    if len(parts) != 2 or parts[1].lstrip("*") != filename:
        raise ArchiveAuditError(f"malformed checksum file for {filename}")
    expected = parts[0].lower()
    actual = bytes_sha256(zip_bytes)
    if expected != actual:
        raise ArchiveAuditError(f"checksum mismatch for {filename}")
    return actual


def parse_boundary_zip(value: bytes, *, expected_filename: str) -> tuple[int, int, int]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(value))
    except zipfile.BadZipFile as exc:
        raise ArchiveAuditError(f"invalid ZIP: {expected_filename}") from exc
    names = archive.namelist()
    expected_csv = expected_filename.removesuffix(".zip") + ".csv"
    if names != [expected_csv]:
        raise ArchiveAuditError(f"unexpected ZIP members for {expected_filename}: {names}")
    rows: list[int] = []
    with archive.open(expected_csv) as raw_stream:
        text_stream = io.TextIOWrapper(raw_stream, encoding="utf-8", newline="")
        for row in csv.reader(text_stream):
            if not row:
                continue
            try:
                raw_open = int(row[0])
                open_price = float(row[1])
                close_price = float(row[4])
            except (ValueError, IndexError):
                if row[0].lower() in {"open_time", "open time"}:
                    continue
                raise ArchiveAuditError(f"invalid kline row in {expected_filename}") from None
            if len(row) != 12 or open_price <= 0 or close_price <= 0:
                raise ArchiveAuditError(f"invalid kline shape/value in {expected_filename}")
            rows.append(normalize_open_time(raw_open))
    if not rows or rows != sorted(set(rows)):
        raise ArchiveAuditError(f"empty, duplicate, or unsorted bars in {expected_filename}")
    return rows[0], rows[-1], len(rows)


def milliseconds_to_utc(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


def _page_evidence(snapshot: ListingSnapshot) -> list[dict[str, Any]]:
    return [
        {
            "request_url": page.request_url,
            "response_sha256": page.response_sha256,
            "retrieved_at_utc": page.retrieved_at_utc,
            "http_status": page.http_status,
            "marker": page.marker,
            "next_marker": page.next_marker,
            "is_truncated": page.is_truncated,
        }
        for page in snapshot.pages
    ]


def build_symbol_record(
    client: ArchiveClient,
    symbol: str,
    snapshot: ListingSnapshot,
    *,
    first_month: str,
    last_month: str,
) -> dict[str, Any]:
    selected = select_monthly_archives(
        symbol, snapshot.objects, first_month=first_month, last_month=last_month
    )
    complete_months = sorted(
        month for month, files in selected.items() if set(files) == {"zip", "checksum"}
    )
    incomplete_months = sorted(
        month for month, files in selected.items() if set(files) != {"zip", "checksum"}
    )
    if not complete_months:
        return {
            "symbol": symbol,
            "episode_id": f"{symbol}#archive-1",
            "first_valid_bar_open_time": None,
            "last_valid_bar_open_time": None,
            "observed_months": [],
            "missing_months": [],
            "unexplained_gaps": [],
            "uncertainty_status": "QUARANTINED_NO_COMPLETE_IN_SAMPLE_ARCHIVE",
            "source_urls": [],
            "source_hashes": [],
            "listing_pages": _page_evidence(snapshot),
        }
    boundary_months = [complete_months[0]]
    if complete_months[-1] != complete_months[0]:
        boundary_months.append(complete_months[-1])
    parsed: dict[str, dict[str, Any]] = {}
    source_urls: list[str] = []
    source_hashes: list[str] = []
    for month in boundary_months:
        files = selected[month]
        zip_record = files["zip"]
        checksum_record = files["checksum"]
        zip_url, zip_result = client.get_object(zip_record.key)
        checksum_url, checksum_result = client.get_object(checksum_record.key)
        filename = Path(zip_record.key).name
        digest = verify_checksum(zip_result.data, checksum_result.data, filename=filename)
        first_open, last_open, rows = parse_boundary_zip(
            zip_result.data, expected_filename=filename
        )
        parsed[month] = {
            "first_open": first_open,
            "last_open": last_open,
            "rows": rows,
            "zip_sha256": digest,
            "checksum_sha256": bytes_sha256(checksum_result.data),
        }
        source_urls.extend([zip_url, checksum_url])
        source_hashes.extend([digest, bytes_sha256(checksum_result.data)])
    expected_months = set(month_range(complete_months[0], complete_months[-1]))
    missing_months = sorted(expected_months - set(complete_months))
    gaps: list[dict[str, Any]] = [
        {"type": "MISSING_ARCHIVE_MONTH", "month": month, "treatment": "QUARANTINE"}
        for month in missing_months
    ]
    gaps.extend(
        {"type": "INCOMPLETE_ARCHIVE_PAIR", "month": month, "treatment": "QUARANTINE"}
        for month in incomplete_months
    )
    gaps.append(
        {
            "type": "INTERNAL_BARS_NOT_YET_SCANNED",
            "treatment": "QUARANTINE_UNTIL_STRATEGY_DATA_ACQUISITION",
        }
    )
    status = (
        "QUARANTINED_CATALOG_GAPS_AND_INTERNAL_BARS_PENDING"
        if missing_months or incomplete_months
        else "BOUNDARIES_VALID_INTERNAL_BARS_QUARANTINED_PENDING"
    )
    return {
        "symbol": symbol,
        "episode_id": f"{symbol}#archive-1",
        "first_valid_bar_open_time": milliseconds_to_utc(
            parsed[complete_months[0]]["first_open"]
        ),
        "last_valid_bar_open_time": milliseconds_to_utc(
            parsed[complete_months[-1]]["last_open"]
        ),
        "observed_months": complete_months,
        "missing_months": missing_months,
        "unexplained_gaps": gaps,
        "uncertainty_status": status,
        "source_urls": sorted(source_urls),
        "source_hashes": sorted(source_hashes),
        "boundary_validation": parsed,
        "listing_pages": _page_evidence(snapshot),
    }


def run_archive_observed_audit(
    *,
    client: ArchiveClient | None = None,
    first_month: str = "2017-08",
    last_month: str = "2026-06",
    minimum_symbols: int = 25,
    max_workers: int = 16,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Enumerate and boundary-validate the frozen archive-observed universe."""

    active_client = client or ArchiveClient()
    started = _now()
    root_snapshot = active_client.list_prefix(ROOT_PREFIX, delimiter="/")
    symbols = enumerate_usdt_symbols(root_snapshot)
    if len(symbols) < minimum_symbols:
        raise ArchiveAuditError(
            f"only {len(symbols)} archive-observed USDT symbols; minimum is {minimum_symbols}"
        )
    listings: dict[str, ListingSnapshot] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        listing_futures = {
            executor.submit(
                active_client.list_prefix, f"{ROOT_PREFIX}{symbol}/1d/"
            ): symbol
            for symbol in symbols
        }
        for listing_future in as_completed(listing_futures):
            symbol = listing_futures[listing_future]
            try:
                listings[symbol] = listing_future.result()
            except Exception as exc:  # fail-closed evidence capture
                errors[symbol] = f"{type(exc).__name__}: {exc}"
    if errors:
        raise ArchiveAuditError(
            f"symbol listing failures: {json.dumps(errors, sort_keys=True)}"
        )
    records: list[dict[str, Any]] = []
    boundary_errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        boundary_futures = {
            executor.submit(
                build_symbol_record,
                active_client,
                symbol,
                listings[symbol],
                first_month=first_month,
                last_month=last_month,
            ): symbol
            for symbol in symbols
        }
        for boundary_future in as_completed(boundary_futures):
            symbol = boundary_futures[boundary_future]
            try:
                records.append(boundary_future.result())
            except Exception as exc:  # fail-closed evidence capture
                boundary_errors[symbol] = f"{type(exc).__name__}: {exc}"
    if boundary_errors:
        raise ArchiveAuditError(
            f"boundary validation failures: {json.dumps(boundary_errors, sort_keys=True)}"
        )
    records.sort(key=lambda item: str(item["symbol"]))
    in_sample = [item for item in records if item["first_valid_bar_open_time"] is not None]
    if len(in_sample) < minimum_symbols:
        raise ArchiveAuditError(
            f"only {len(in_sample)} boundary-valid in-sample symbols; minimum is {minimum_symbols}"
        )
    manifest_body = {
        "schema_version": "1.0",
        "manifest_type": "BINANCE_SPOT_ARCHIVE_OBSERVED_USDT_UNIVERSE",
        "claim_scope": "archive_observed_not_formally_complete",
        "interval": "1d",
        "first_month": first_month,
        "last_month": last_month,
        "current_exchange_info_requests": 0,
        "market_capitalization_inputs": 0,
        "end_of_sample_survival_filter": False,
        "root_listing_pages": _page_evidence(root_snapshot),
        "symbols": records,
    }
    manifest = dict(manifest_body)
    manifest["manifest_sha256"] = canonical_sha256(manifest_body)
    report = {
        "schema_version": "1.0",
        "experiment_id": "cs-ranking-binance-spot-archive-ptu-audit-v1",
        "started_at_utc": started,
        "ended_at_utc": _now(),
        "data_contract_result": "TECHNICAL_ROUTE_VALIDATED_PENDING_INDEPENDENT_AUDIT",
        "archive_observed_symbol_directories": len(symbols),
        "boundary_valid_in_sample_symbols": len(in_sample),
        "root_listing_page_count": len(root_snapshot.pages),
        "manifest_sha256": manifest["manifest_sha256"],
        "current_exchange_info_requests": 0,
        "holdout_opened": False,
        "returns_calculated": False,
        "performance_claim_made": False,
        "capital_permitted": 0,
        "limitations": [
            "The bucket listing cannot prove formal exchange-wide archive completeness.",
            "Archive objects may be revised later; this result is bound to recorded hashes.",
            (
                "Internal bars remain quarantined until checksum and row-level "
                "validation during strategy-data acquisition."
            ),
            (
                "Symbol names do not prove asset identity across renames or migrations; "
                "episodes remain separate unless causal evidence is added."
            ),
        ],
    }
    return manifest, report


def listing_snapshot_as_dict(snapshot: ListingSnapshot) -> dict[str, Any]:
    """Expose deterministic structured evidence for tests and diagnostics."""

    return {
        "pages": [asdict(page) for page in snapshot.pages],
        "common_prefixes": list(snapshot.common_prefixes),
        "objects": [asdict(item) for item in snapshot.objects],
    }
