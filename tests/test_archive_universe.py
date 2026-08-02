from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from strategy_control.archive_universe import (
    ArchiveBar,
    build_manifest,
    canonical_manifest_hash,
    causal_liquidity_ranking,
    eligible_symbols,
    next_real_bar,
    normalize_open_time,
)


def _time(day: int) -> int:
    return int((datetime(2024, 1, 1, tzinfo=UTC) + timedelta(days=day)).timestamp() * 1000)


def _bars(symbol: str, days: range, volume: float = 1.0) -> list[ArchiveBar]:
    return [
        ArchiveBar(symbol, _time(day), volume, f"archive/{symbol}", f"hash-{symbol}")
        for day in days
    ]


def test_future_informed_membership_is_prefix_invariant() -> None:
    history = _bars("AAAUSDT", range(40))
    as_of = _time(40)
    assert eligible_symbols(history, as_of) == eligible_symbols(
        history + _bars("ZZZUSDT", range(70, 100)), as_of
    )


def test_current_exchange_info_is_forbidden() -> None:
    with pytest.raises(ValueError, match="exchangeInfo"):
        build_manifest(_bars("AAAUSDT", range(2)), current_exchange_info={"symbols": []})


def test_survivorship_leakage_does_not_filter_a_past_member() -> None:
    as_of = _time(35)
    ended_later = _bars("OLDUSDT", range(40))
    assert eligible_symbols(ended_later, as_of) == ("OLDUSDT",)


def test_first_bar_listing_buffer_requires_completed_history() -> None:
    bars = _bars("AAAUSDT", range(30))
    assert eligible_symbols(bars, _time(30)) == ("AAAUSDT",)
    assert eligible_symbols(bars, _time(29)) == ()


def test_last_bar_has_no_next_fill() -> None:
    bars = _bars("AAAUSDT", range(3))
    assert next_real_bar(bars, _time(2)) is None


def test_missing_month_quarantines_and_requires_recovery() -> None:
    bars = _bars("AAAUSDT", range(31)) + _bars("AAAUSDT", range(62, 92))
    manifest = build_manifest(bars, expected_months={"AAAUSDT": ("2024-01", "2024-02", "2024-03")})
    assert manifest.entries[0].uncertainty_status == "QUARANTINED"
    assert manifest.entries[0].missing_months == ("2024-02",)
    assert eligible_symbols(bars, _time(91)) == ()
    assert eligible_symbols(bars, _time(92)) == ("AAAUSDT",)


def test_identical_duplicates_deduplicate_but_conflicts_quarantine() -> None:
    identical = _bars("AAAUSDT", range(2))
    assert build_manifest([*identical, identical[0]]).entries[0].uncertainty_status == "CLEAR"
    conflicting = ArchiveBar("AAAUSDT", _time(0), 2.0, "archive/other", "other-hash")
    assert build_manifest([*identical, conflicting]).entries[0].uncertainty_status == "QUARANTINED"


def test_renamed_or_migrated_symbols_are_separate_quarantined_episodes() -> None:
    manifest = build_manifest(
        _bars("OLDUSDT", range(2)) + _bars("NEWUSDT", range(2)),
        renamed_or_migrated=("OLDUSDT", "NEWUSDT"),
    )
    assert [entry.episode_id for entry in manifest.entries] == [
        "NEWUSDT:migration-1",
        "OLDUSDT:migration-1",
    ]
    assert all(entry.uncertainty_status == "QUARANTINED" for entry in manifest.entries)


def test_lagged_liquidity_ranking_ignores_current_and_future_mutations() -> None:
    history = _bars("AAAUSDT", range(30), 4.0) + _bars("BBBUSDT", range(30), 2.0)
    as_of = _time(30)
    expected = (("AAAUSDT", 4.0), ("BBBUSDT", 2.0))
    assert causal_liquidity_ranking(history, as_of) == expected
    mutated = [*history, ArchiveBar("BBBUSDT", as_of, 1_000_000.0, "later", "later")]
    assert causal_liquidity_ranking(mutated, as_of) == expected


def test_next_real_bar_execution_obeys_delay_and_does_not_interpolate() -> None:
    bars = _bars("AAAUSDT", range(1)) + _bars("AAAUSDT", range(3, 5))
    assert next_real_bar(bars, _time(0)).open_time == _time(3)
    assert next_real_bar(bars, _time(0), delay_bars=1).open_time == _time(4)
    assert next_real_bar(bars, _time(0), delay_bars=2) is None


def test_manifest_hash_and_timestamp_units_are_canonical() -> None:
    pre_cutover = int(datetime(2024, 12, 31, tzinfo=UTC).timestamp() * 1000)
    post_cutover = int(datetime(2025, 1, 1, tzinfo=UTC).timestamp() * 1000)
    assert normalize_open_time(pre_cutover) == pre_cutover
    assert normalize_open_time(post_cutover * 1000) == post_cutover
    manifest = build_manifest(_bars("AAAUSDT", range(2)))
    assert manifest.sha256 == canonical_manifest_hash(
        manifest.schema_version, tuple(reversed(manifest.entries))
    )
