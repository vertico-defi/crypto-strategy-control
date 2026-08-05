"""Synthetic, non-economic validation for the frozen v2 production adapter."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import cast

import pytest

import strategy_control.mean_reversion_v2_pipeline as pipeline
from strategy_control.mean_reversion_v2 import (
    MeanReversionV2Error,
    canonical_hash,
    causal_gap_segments,
)
from strategy_control.mean_reversion_v2_pipeline import (
    ALLOWLIST_COUNT,
    AllowlistEntry,
    JointSession,
    MinuteRow,
    ProductionIntegrationError,
    ProductionRowIndex,
    RepresentativeAccounting,
    build_joint_sessions,
    build_production_row_index,
    canonical_mechanical_evidence,
    fill_identities,
    materialize_rows,
    parse_verified_parquet,
    read_verified_entry,
    reconcile_representative_accounting,
    representative_row_hashes,
    serialize_utc_evidence_timestamp,
    terminal_fill_identity,
    verify_entry_buffer,
    verify_source_identity,
)


def stamp(day: int, minute: int) -> datetime:
    return datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=day, minutes=minute)


def test_utc_evidence_timestamp_is_deterministic_and_json_safe() -> None:
    boundary = datetime(2026, 1, 1, tzinfo=UTC)
    observed = serialize_utc_evidence_timestamp(boundary)
    assert observed == "2026-01-01T00:00:00Z"
    assert json.loads(json.dumps({"boundary": observed})) == {"boundary": observed}


@pytest.mark.parametrize(
    "invalid",
    [datetime(2026, 1, 1), datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1)))],
)
def test_evidence_timestamp_rejects_non_utc_or_naive_values(invalid: datetime) -> None:
    with pytest.raises(ProductionIntegrationError, match="timezone-aware UTC"):
        serialize_utc_evidence_timestamp(invalid)


def minute(
    day: int, offset: int, asset: str, *, opening: float = 100.0, available_offset: int = 0
) -> MinuteRow:
    event = stamp(day, offset)
    return MinuteRow(
        f"canonical/symbol={asset}/year=2025/month=01/{day:02}.parquet",
        "a" * 64 if asset == "BTCUSDT" else "b" * 64,
        day * 1440 + offset,
        f"synthetic:{asset}:{day}:{offset}",
        event,
        event + timedelta(minutes=available_offset),
        opening,
        opening + 1,
        opening - 1,
        opening + 0.5,
        1.0,
    )


def full_day(day: int, asset: str) -> list[MinuteRow]:
    return [minute(day, offset, asset) for offset in range(1, 1441)]


def indexed(rows: Mapping[str, Sequence[MinuteRow]], end: datetime) -> ProductionRowIndex:
    return build_production_row_index(rows, end=end)


def scan_reference_sessions(
    rows: Mapping[str, Sequence[MinuteRow]], boundary: datetime
) -> tuple[JointSession, ...]:
    """Small-fixture reference that deliberately scans retained rows per session."""
    retained = {
        asset: [row for row in rows[asset] if row.event_timestamp < boundary]
        for asset in ("BTCUSDT", "ETHUSDT")
    }
    observed = sorted(
        {
            pipeline._session_for_bar_end(row.event_timestamp)
            for asset_rows in retained.values()
            for row in asset_rows
        }
    )
    if not observed:
        return ()
    days = [
        observed[0] + timedelta(days=offset)
        for offset in range((observed[-1] - observed[0]).days + 1)
    ]
    raw: list[JointSession] = []
    for session in days:
        expected = tuple(session + timedelta(minutes=offset) for offset in range(1, 1441))
        selected = {
            asset: {
                row.event_timestamp: row
                for row in retained[asset]
                if pipeline._session_for_bar_end(row.event_timestamp) == session
            }
            for asset in ("BTCUSDT", "ETHUSDT")
        }
        complete = all(tuple(selected[asset]) == expected for asset in selected)
        used = (
            [selected[asset][stamp] for asset in selected for stamp in expected]
            if complete
            else []
        )
        raw.append(
            JointSession(
                session,
                complete,
                max(max(row.event_timestamp, row.available_timestamp) for row in used)
                if used
                else None,
                {asset: selected[asset][expected[-1]].close for asset in selected}
                if complete
                else {},
                None,
            )
        )
    segments = causal_gap_segments([(item.session, item.complete) for item in raw])
    return tuple(
        JointSession(item.session, item.complete, item.information_cutoff, item.closes, segment)
        for item, segment in zip(raw, segments, strict=True)
    )


def scan_reference_execution_rows(
    rows: Mapping[str, Sequence[MinuteRow]], fill_timestamp: datetime, boundary: datetime
) -> Mapping[str, MinuteRow]:
    """Small-fixture exact-lookup reference; it never substitutes a later row."""
    expected_event = fill_timestamp + timedelta(minutes=1)
    if expected_event >= boundary:
        raise ProductionIntegrationError("execution row is outside the strict half-open boundary")
    selected: dict[str, MinuteRow] = {}
    for asset in ("BTCUSDT", "ETHUSDT"):
        matches = [
            row
            for row in rows[asset]
            if row.event_timestamp < boundary and row.event_timestamp == expected_event
        ]
        if len(matches) != 1:
            raise ProductionIntegrationError(
                "missing exact ordinary execution row; forward scan prohibited"
            )
        if matches[0].available_timestamp != matches[0].event_timestamp:
            raise ProductionIntegrationError(
                "asynchronous execution row; exact synchronized fill rejected"
            )
        selected[asset] = matches[0]
    return selected


def entries() -> list[AllowlistEntry]:
    return [
        AllowlistEntry(
            1,
            "2025-01",
            f"canonical/symbol={asset}/year=2025/month={month:02}/x.parquet",
            "a" * 64,
            asset,
        )
        for asset in ("BTCUSDT", "ETHUSDT")
        for month in range(1, 19)
    ]


def identity_constants(monkeypatch: pytest.MonkeyPatch, supplied: list[AllowlistEntry]) -> bytes:
    contract = b'{"synthetic":"contract"}'
    monkeypatch.setattr(
        pipeline, "REUSED_CONTRACT_BYTE_SHA256", hashlib.sha256(contract).hexdigest()
    )
    monkeypatch.setattr(
        pipeline, "REUSED_CONTRACT_CANONICAL_SHA256", hashlib.sha256(contract).hexdigest()
    )
    monkeypatch.setattr(
        pipeline,
        "ALLOWLIST_SHA256",
        canonical_hash(
            [
                {
                    "bytes": item.bytes,
                    "month": item.month,
                    "relative_path": item.relative_path,
                    "sha256": item.sha256,
                    "symbol": item.symbol,
                }
                for item in supplied
            ]
        ),
    )
    return contract


def test_source_contract_and_36_entry_allowlist_hash_mismatch_rejected_before_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supplied = entries()
    contract = identity_constants(monkeypatch, supplied)
    assert verify_source_identity(
        contract_bytes=contract,
        source_commit=pipeline.SOURCE_COMMIT,
        freeze_manifest_sha256=pipeline.FREEZE_MANIFEST_SHA256,
        inventory_sha256=pipeline.CANONICAL_INVENTORY_SHA256,
        entries=supplied,
    ) == tuple(supplied)
    with pytest.raises(ProductionIntegrationError, match="before resolution"):
        verify_source_identity(
            contract_bytes=b"bad",
            source_commit=pipeline.SOURCE_COMMIT,
            freeze_manifest_sha256=pipeline.FREEZE_MANIFEST_SHA256,
            inventory_sha256=pipeline.CANONICAL_INVENTORY_SHA256,
            entries=supplied,
        )
    with pytest.raises(MeanReversionV2Error, match="before access"):
        verify_source_identity(
            contract_bytes=contract,
            source_commit=pipeline.SOURCE_COMMIT,
            freeze_manifest_sha256=pipeline.FREEZE_MANIFEST_SHA256,
            inventory_sha256=pipeline.CANONICAL_INVENTORY_SHA256,
            entries=[
                *supplied[:-1],
                AllowlistEntry(1, "2026-01", "year=2026/x", "a" * 64, "BTCUSDT"),
            ],
        )


def test_verified_buffer_parse_uses_list_columns_and_materializes_distinct_buffers() -> None:
    class Field:
        def __init__(self, value: str) -> None:
            self.type = value

    class Column:
        def __init__(self, value: list[object]) -> None:
            self.value = value

        def to_pylist(self) -> list[object]:
            return self.value

    class Table:
        class schema:
            names = pipeline.PARQUET_COLUMNS

            @staticmethod
            def field(name: str) -> Field:
                if "timestamp" in name:
                    return Field("timestamp[us, tz=UTC]")
                return Field("string" if name == "source_provenance" else "double")

        def __init__(self, index: int) -> None:
            value = stamp(0, index + 1)
            self.values: dict[str, list[object]] = {
                "event_timestamp": [value],
                "available_timestamp": [value],
                "source_provenance": [f"buffer:{index}"],
                "open": [100.0 + index],
                "high": [101.0 + index],
                "low": [99.0 + index],
                "close": [100.5 + index],
                "volume": [1.0],
            }

        def column(self, name: str) -> Column:
            return Column(self.values[name])

    parsed: list[bytes] = []

    class Arrow:
        @staticmethod
        def BufferReader(payload: bytes) -> bytes:
            return payload

    class Parquet:
        @staticmethod
        def read_table(source: bytes, *, columns: list[str]) -> Table:
            assert columns == pipeline.PARQUET_COLUMNS
            parsed.append(source)
            return Table(source[0])

    rows: list[MinuteRow] = []
    for index in range(ALLOWLIST_COUNT):
        payload = bytes([index])
        entry = AllowlistEntry(
            1,
            "2025-01",
            f"canonical/year=2025/{index}.parquet",
            hashlib.sha256(payload).hexdigest(),
            "BTCUSDT" if index % 2 == 0 else "ETHUSDT",
        )
        verified = verify_entry_buffer(entry, payload)
        rows.extend(
            materialize_rows(
                parse_verified_parquet(verified, parquet_module=Parquet, pyarrow_module=Arrow),
                verified,
            )
        )
    assert parsed == [bytes([index]) for index in range(ALLOWLIST_COUNT)]
    assert len({row.relative_path for row in rows}) == ALLOWLIST_COUNT
    assert len(representative_row_hashes(rows[:1])) == 3


def test_future_or_holdout_path_rejected_before_filesystem_access() -> None:
    class NeverRoot:
        def joinpath(self, *parts: str) -> Path:
            raise AssertionError(f"resolution occurred: {parts}")

    with pytest.raises(MeanReversionV2Error, match="before access"):
        read_verified_entry(
            cast(Path, NeverRoot()),
            AllowlistEntry(1, "2026-01", "canonical/year=2026/x", "a" * 64, "BTCUSDT"),
        )


def test_bar_end_sessions_order_validation_and_no_spurious_midnight_session() -> None:
    rows = {asset: full_day(0, asset) + full_day(1, asset) for asset in ("BTCUSDT", "ETHUSDT")}
    sessions = build_joint_sessions(indexed(rows, stamp(2, 1)), end=stamp(2, 1))
    assert [item.session for item in sessions] == [stamp(0, 0), stamp(1, 0)]
    assert [item.complete for item in sessions] == [True, True]
    bad = {**rows, "BTCUSDT": [rows["BTCUSDT"][1], rows["BTCUSDT"][0], *rows["BTCUSDT"][2:]]}
    with pytest.raises(MeanReversionV2Error, match="nonmonotonic"):
        build_joint_sessions(indexed(bad, stamp(1, 1)), end=stamp(1, 1))
    ignored_suffix = {
        **rows,
        "BTCUSDT": [
            *full_day(0, "BTCUSDT"),
            MinuteRow(
                "canonical/year=2026/never-opened.parquet",
                "x",
                -1,
                "invalid-suffix",
                stamp(1, 1),
                stamp(1, 1),
                float("nan"),
                0.0,
                0.0,
                0.0,
                -1.0,
            ),
        ],
        "ETHUSDT": full_day(0, "ETHUSDT"),
    }
    isolated = build_joint_sessions(indexed(ignored_suffix, stamp(1, 1)), end=stamp(1, 1))
    assert isolated == build_joint_sessions(
        indexed({asset: full_day(0, asset) for asset in ("BTCUSDT", "ETHUSDT")}, stamp(1, 1)),
        end=stamp(1, 1),
    )


def test_fully_missing_calendar_session_is_materialized_and_quarantined() -> None:
    rows = {
        asset: full_day(0, asset) + full_day(2, asset)
        for asset in ("BTCUSDT", "ETHUSDT")
    }
    sessions = build_joint_sessions(indexed(rows, stamp(3, 1)), end=stamp(3, 1))
    assert [item.session for item in sessions] == [stamp(0, 0), stamp(1, 0), stamp(2, 0)]
    assert [item.complete for item in sessions] == [True, False, True]
    assert sessions[1].information_cutoff is None
    assert sessions[1].closes == {}
    assert all(item.segment is None for item in sessions)


def test_risky_gap_and_150_session_recovery_are_nonbridging() -> None:
    sessions = tuple(
        JointSession(
            stamp(index, 0),
            index != 1,
            stamp(index + 1, 0) if index != 1 else None,
            {"BTCUSDT": 1.0, "ETHUSDT": 1.0} if index != 1 else {},
            None,
        )
        for index in range(152)
    )
    labels = causal_gap_segments([(item.session, item.complete) for item in sessions])
    assert labels[0] is None and labels[1] is None and labels[150] is None and labels[151] == 1
    boundary = (
        JointSession(stamp(0, 0), True, stamp(1, 0), {"BTCUSDT": 1.0, "ETHUSDT": 1.0}, 0),
        JointSession(stamp(1, 0), False, None, {}, None),
    )
    execution_rows = {
        asset: [minute(1, 2, asset)] for asset in ("BTCUSDT", "ETHUSDT")
    }
    fills = fill_identities(boundary, indexed(execution_rows, stamp(2, 0)), end=stamp(2, 0))
    assert len(fills) == 1
    assert fills[0].delayed_timestamp is None


def test_exact_synchronized_execution_open_prices_no_forward_scan_and_terminal_boundary() -> None:
    sessions = (
        JointSession(stamp(0, 0), True, stamp(1, 0), {"BTCUSDT": 7.0, "ETHUSDT": 8.0}, 0),
        JointSession(stamp(1, 0), True, stamp(2, 0), {"BTCUSDT": 9.0, "ETHUSDT": 10.0}, 0),
    )
    rows = {
        asset: [
            minute(1, 2, asset, opening=101.0 if asset == "BTCUSDT" else 202.0),
            minute(2, 2, asset, opening=103.0 if asset == "BTCUSDT" else 204.0),
        ]
        for asset in ("BTCUSDT", "ETHUSDT")
    }
    index = indexed(rows, stamp(3, 0))
    identities = fill_identities(sessions, index, end=stamp(3, 0))
    assert len(identities) == 2
    assert identities[0].base_prices == {"BTCUSDT": 101.0, "ETHUSDT": 202.0}
    assert identities[0].delayed_prices == {"BTCUSDT": 103.0, "ETHUSDT": 204.0}
    assert set(identities[0].base_row_identities) == {"BTCUSDT", "ETHUSDT"}
    assert set(identities[0].delayed_row_identities) == {"BTCUSDT", "ETHUSDT"}
    assert identities[0].base_timestamp == stamp(1, 1)
    assert identities[0].delayed_timestamp == stamp(2, 1)
    incomplete = {**rows, "ETHUSDT": [rows["ETHUSDT"][1]]}
    with pytest.raises(ProductionIntegrationError, match="forward scan prohibited"):
        fill_identities(sessions, indexed(incomplete, stamp(3, 0)), end=stamp(3, 0))
    asynchronous = {
        **rows,
        "ETHUSDT": [minute(1, 2, "ETHUSDT", opening=202.0, available_offset=1), rows["ETHUSDT"][1]],
    }
    with pytest.raises(ProductionIntegrationError, match="asynchronous"):
        fill_identities(sessions, indexed(asynchronous, stamp(3, 0)), end=stamp(3, 0))
    terminal = terminal_fill_identity(identities, end=stamp(3, 0))
    assert terminal.base_timestamp == stamp(2, 1)
    assert terminal.delayed_timestamp is None
    with pytest.raises(ProductionIntegrationError, match="no exact terminal"):
        terminal_fill_identity((), end=stamp(2, 0))


def test_boundary_bound_index_rejects_retained_defects_and_ignores_invalid_suffix() -> None:
    retained = {asset: full_day(0, asset) for asset in ("BTCUSDT", "ETHUSDT")}
    boundary = stamp(1, 1)
    duplicate = {
        **retained,
        "BTCUSDT": [retained["BTCUSDT"][0], retained["BTCUSDT"][0], *retained["BTCUSDT"][1:]],
    }
    with pytest.raises(ProductionIntegrationError, match="duplicate or nonmonotonic retained"):
        indexed(duplicate, boundary)
    malformed = {**retained, "ETHUSDT": [object(), *retained["ETHUSDT"]]}
    with pytest.raises(ProductionIntegrationError, match="malformed retained"):
        indexed(malformed, boundary)  # type: ignore[arg-type]
    invalid_suffix = {
        **retained,
        "BTCUSDT": [
            *retained["BTCUSDT"],
            MinuteRow(
                "year=2026/not-resolved.parquet",
                "bad",
                -1,
                "",
                boundary,
                boundary,
                float("nan"),
                0.0,
                0.0,
                0.0,
                -1.0,
            ),
        ],
    }
    clean_index = indexed(retained, boundary)
    suffix_index = indexed(invalid_suffix, boundary)
    assert suffix_index.rows_by_asset["BTCUSDT"] == clean_index.rows_by_asset["BTCUSDT"]
    with pytest.raises(ProductionIntegrationError, match="boundary mismatch"):
        build_joint_sessions(clean_index, end=stamp(2, 1))


def test_four_fold_indices_are_distinct_and_match_scan_reference() -> None:
    rows = {
        asset: [row for day in range(4) for row in full_day(day, asset)]
        for asset in ("BTCUSDT", "ETHUSDT")
    }
    boundaries = [stamp(day, 1) for day in range(1, 5)]
    indices = [indexed(rows, boundary) for boundary in boundaries]
    assert len({id(item) for item in indices}) == 4
    assert [item.retained_row_count for item in indices] == [2880, 5760, 8640, 11520]
    for boundary, index in zip(boundaries, indices, strict=True):
        sessions = build_joint_sessions(index, end=boundary)
        assert sessions == scan_reference_sessions(rows, boundary)


def test_indexed_execution_rows_match_separate_scan_reference() -> None:
    rows = {
        asset: [
            minute(1, 2, asset, opening=101.0 if asset == "BTCUSDT" else 202.0),
            minute(2, 2, asset, opening=103.0 if asset == "BTCUSDT" else 204.0),
        ]
        for asset in ("BTCUSDT", "ETHUSDT")
    }
    boundary = stamp(3, 0)
    index = indexed(rows, boundary)
    for fill_timestamp in (stamp(1, 1), stamp(2, 1)):
        indexed_rows = pipeline._exact_execution_rows(index, fill_timestamp, boundary)
        scanned_rows = scan_reference_execution_rows(rows, fill_timestamp, boundary)
        assert indexed_rows == scanned_rows
        assert all(indexed_rows[asset] is scanned_rows[asset] for asset in scanned_rows)


def test_index_reuse_avoids_rescanning_and_session_work_is_retained_plus_grid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OnePassRows(list[MinuteRow]):
        iterations = 0

        def __iter__(self):  # type: ignore[no-untyped-def]
            self.iterations += 1
            if self.iterations > 1:
                raise AssertionError("raw rows rescanned after index construction")
            return super().__iter__()

    class CountingRows(Mapping[datetime, MinuteRow]):
        def __init__(self, values: Mapping[datetime, MinuteRow]) -> None:
            self.mapping = values
            self.iterated = 0

        def __getitem__(self, key: datetime) -> MinuteRow:
            return self.mapping[key]

        def __iter__(self) -> Iterator[datetime]:
            for key in self.mapping:
                self.iterated += 1
                yield key

        def __len__(self) -> int:
            return len(self.mapping)

    rows = {
        asset: OnePassRows([row for day in range(3) for row in full_day(day, asset)])
        for asset in ("BTCUSDT", "ETHUSDT")
    }
    calls = 0
    original = pipeline._session_for_bar_end

    def counted(timestamp: datetime) -> datetime:
        nonlocal calls
        calls += 1
        return original(timestamp)

    monkeypatch.setattr(pipeline, "_session_for_bar_end", counted)
    boundary = stamp(3, 1)
    index = indexed(rows, boundary)
    assert calls == index.retained_row_count
    counted_sessions = {
        asset: {session: CountingRows(values) for session, values in per_session.items()}
        for asset, per_session in index.session_rows_by_asset.items()
    }
    instrumented = ProductionRowIndex(
        index.boundary, index.rows_by_asset, counted_sessions, index.retained_row_count
    )
    sessions = build_joint_sessions(instrumented, end=boundary)
    assert calls == index.retained_row_count
    retained_lookup_iterations = sum(
        values.iterated
        for per_session in counted_sessions.values()
        for values in per_session.values()
    )
    expected_grid_rows = len(sessions) * 1440
    assert retained_lookup_iterations + expected_grid_rows == 12960
    assert fill_identities(sessions, instrumented, end=boundary) == ()
    prepared = pipeline.FillIdentity(
        stamp(0, 0), 0, stamp(1, 1), None,
        {"BTCUSDT": 1.0, "ETHUSDT": 1.0},
        {"BTCUSDT": "a", "ETHUSDT": "b"}, {}, {},
    )
    monkeypatch.setattr(
        pipeline,
        "fill_identities",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()),
    )
    assert terminal_fill_identity((prepared,), end=boundary) is prepared
    assert all(item.iterations == 1 for item in rows.values())


def test_strict_fold_prefix_and_canonical_mechanical_evidence_reconcile() -> None:
    row = minute(0, 1, "BTCUSDT")
    session = JointSession(stamp(0, 0), True, stamp(1, 0), {"BTCUSDT": 1.0, "ETHUSDT": 1.0}, 0)
    fill = pipeline.FillIdentity(
        stamp(0, 0),
        0,
        stamp(1, 1),
        stamp(2, 1),
        {"BTCUSDT": 100.0, "ETHUSDT": 200.0},
        {"BTCUSDT": "a" * 64, "ETHUSDT": "b" * 64},
        {"BTCUSDT": 101.0, "ETHUSDT": 199.0},
        {"BTCUSDT": "c" * 64, "ETHUSDT": "d" * 64},
    )
    evidence = canonical_mechanical_evidence(
        rows=[row],
        sessions=[session],
        fills=[fill],
        trace_records=[{"decision": "x"}],
        cost_records=[{"cost": 0.1}],
        representative_returns=[{"interval": 0.0}],
    )
    assert all(len(value) == 64 for value in evidence.__dict__.values())
    case = RepresentativeAccounting(
        {"BTCUSDT": 0.005, "ETHUSDT": 0.0025},
        0.0,
        {"BTCUSDT": 100.0, "ETHUSDT": 200.0},
        {"BTCUSDT": 120.0, "ETHUSDT": 180.0},
        {"BTCUSDT": 0.5, "ETHUSDT": 0.5},
        0.0014,
    )
    result = reconcile_representative_accounting(case)
    assert result.prior_postcost_equity == pytest.approx(1.0)
    assert result.pretrade_equity == pytest.approx(1.05)
    assert result.cost == pytest.approx(0.00021)
    assert result.interval_return == pytest.approx(0.04979)
    assert len(result.identity) == 64
    assert not hasattr(pipeline, "evaluate_development")
    assert not hasattr(pipeline, "annualized_sharpe")
