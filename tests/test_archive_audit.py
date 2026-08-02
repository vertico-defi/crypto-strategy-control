from __future__ import annotations

import hashlib
import io
import zipfile

import pytest

from strategy_control.archive_audit import (
    ArchiveAuditError,
    FetchResult,
    ObjectRecord,
    deduplicate_objects,
    enumerate_usdt_symbols,
    month_range,
    normalize_open_time,
    parse_boundary_zip,
    parse_listing_page,
    quarantined_boundary_record,
    select_monthly_archives,
    verify_checksum,
)


def listing_xml(
    *,
    prefix: str,
    marker: str = "",
    next_marker: str = "",
    truncated: bool = False,
    prefixes: tuple[str, ...] = (),
    objects: tuple[tuple[str, str, int], ...] = (),
) -> bytes:
    common = "".join(
        f"<CommonPrefixes><Prefix>{value}</Prefix></CommonPrefixes>" for value in prefixes
    )
    contents = "".join(
        "<Contents>"
        f"<Key>{key}</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>"
        f'<ETag>"{etag}"</ETag><Size>{size}</Size>'
        "</Contents>"
        for key, etag, size in objects
    )
    next_xml = f"<NextMarker>{next_marker}</NextMarker>" if next_marker else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<Name>data.binance.vision</Name><Prefix>{prefix}</Prefix><Marker>{marker}</Marker>"
        f"<MaxKeys>1000</MaxKeys><IsTruncated>{str(truncated).lower()}</IsTruncated>"
        f"{next_xml}{common}{contents}</ListBucketResult>"
    ).encode()


def test_listing_page_and_usdt_symbol_enumeration() -> None:
    payload = listing_xml(
        prefix="data/spot/monthly/klines/",
        prefixes=(
            "data/spot/monthly/klines/OLDUSDT/",
            "data/spot/monthly/klines/BTCUSDT/",
            "data/spot/monthly/klines/ETHBTC/",
        ),
    )
    page = parse_listing_page(
        FetchResult(payload, 200, "2026-01-01T00:00:00Z"),
        request_url="https://example.test",
        expected_prefix="data/spot/monthly/klines/",
        expected_marker="",
    )
    from strategy_control.archive_audit import ListingSnapshot

    snapshot = ListingSnapshot((page,), page.common_prefixes, page.objects)
    assert enumerate_usdt_symbols(snapshot) == ("BTCUSDT", "OLDUSDT")
    assert page.response_sha256 == hashlib.sha256(payload).hexdigest()


def test_listing_rejects_prefix_or_marker_mismatch() -> None:
    payload = listing_xml(prefix="wrong/", marker="later")
    with pytest.raises(ArchiveAuditError, match="prefix or marker"):
        parse_listing_page(
            FetchResult(payload, 200, "2026-01-01T00:00:00Z"),
            request_url="https://example.test",
            expected_prefix="expected/",
            expected_marker="",
        )


def test_duplicate_objects_deduplicate_identical_and_reject_conflicts() -> None:
    item = ObjectRecord("key", "etag", 3, "date")
    assert deduplicate_objects([item, item]) == (item,)
    with pytest.raises(ArchiveAuditError, match="conflicting duplicate"):
        deduplicate_objects([item, ObjectRecord("key", "other", 3, "date")])


def test_month_selection_and_missing_month_range() -> None:
    prefix = "data/spot/monthly/klines/OLDUSDT/1d/"
    objects = [
        ObjectRecord(f"{prefix}OLDUSDT-1d-2020-01.zip", "a", 10, "date"),
        ObjectRecord(f"{prefix}OLDUSDT-1d-2020-01.zip.CHECKSUM", "b", 10, "date"),
        ObjectRecord(f"{prefix}OLDUSDT-1d-2020-03.zip", "c", 10, "date"),
        ObjectRecord(f"{prefix}OLDUSDT-1d-2020-03.zip.CHECKSUM", "d", 10, "date"),
    ]
    selected = select_monthly_archives(
        "OLDUSDT", objects, first_month="2020-01", last_month="2020-12"
    )
    assert sorted(selected) == ["2020-01", "2020-03"]
    assert month_range("2020-01", "2020-03") == ("2020-01", "2020-02", "2020-03")


def make_zip(filename: str, rows: str) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(filename.removesuffix(".zip") + ".csv", rows)
    return stream.getvalue()


def test_checksum_and_boundary_zip_accept_millisecond_and_microsecond_times() -> None:
    filename = "BTCUSDT-1d-2025-01.zip"
    zipped = make_zip(
        filename,
        "1735689600000000,1,2,0.5,1.5,10,1735775999999999,15,2,4,6,0\n"
        "1735776000000000,1.5,2,1,1.8,10,1735862399999999,18,2,4,6,0\n",
    )
    digest = hashlib.sha256(zipped).hexdigest()
    assert verify_checksum(
        zipped, f"{digest}  {filename}\n".encode(), filename=filename
    ) == digest
    first, last, count = parse_boundary_zip(zipped, expected_filename=filename)
    assert (first, last, count) == (1735689600000, 1735776000000, 2)
    assert normalize_open_time(1499040000000) == 1499040000000


def test_boundary_zip_rejects_same_bar_duplicates() -> None:
    filename = "BTCUSDT-1d-2020-01.zip"
    row = "1577836800000,1,2,0.5,1.5,10,1577923199999,15,2,4,6,0\n"
    with pytest.raises(ArchiveAuditError, match="duplicate"):
        parse_boundary_zip(make_zip(filename, row + row), expected_filename=filename)


def test_boundary_validation_failure_is_explicitly_quarantined() -> None:
    from strategy_control.archive_audit import ListingSnapshot

    snapshot = ListingSnapshot((), (), ())
    record = quarantined_boundary_record(
        "KLAYUSDT", snapshot, "post-cutover archive timestamp must be microseconds"
    )
    assert record["first_valid_bar_open_time"] is None
    assert record["uncertainty_status"] == "QUARANTINED_BOUNDARY_VALIDATION_FAILURE"
    assert record["unexplained_gaps"][0]["exact_error"].startswith("post-cutover")
