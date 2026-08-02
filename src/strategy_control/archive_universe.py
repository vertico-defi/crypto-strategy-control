"""Deterministic, causal core for an archive-observed spot universe.

This module deliberately does not fetch archives or calculate strategy returns.  It
turns already-verified archive observations into a reproducible manifest and
offers the small set of causal queries needed before a backtest is permitted.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from itertools import pairwise

DAY_MS = 86_400_000
CUTOVER_MS = int(datetime(2025, 1, 1, tzinfo=UTC).timestamp() * 1000)
CUTOVER_US = CUTOVER_MS * 1000
MANIFEST_SCHEMA_VERSION = "2.0"


def normalize_open_time(value: int) -> int:
    """Return a millisecond UTC open time from Binance's documented units.

    Daily archives before 2025 use milliseconds and those from the cutover use
    microseconds.  Seconds are rejected rather than guessed.
    """
    if value < 1_000_000_000_000:
        raise ValueError("archive timestamp must be milliseconds or microseconds")
    if value >= CUTOVER_US:
        return value // 1000
    if value >= CUTOVER_MS:
        raise ValueError("post-cutover archive timestamp must be microseconds")
    return value


def _month(open_time: int) -> str:
    return datetime.fromtimestamp(open_time / 1000, UTC).strftime("%Y-%m")


@dataclass(frozen=True, order=True)
class ArchiveBar:
    """A validated 1d archive observation; ``volume`` is quote liquidity input."""

    symbol: str
    open_time: int
    quote_volume: float
    source_url: str
    source_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.upper())
        object.__setattr__(self, "open_time", normalize_open_time(self.open_time))
        if not self.symbol.endswith("USDT"):
            raise ValueError("only archive-observed USDT pairs are permitted")
        if self.quote_volume < 0:
            raise ValueError("quote_volume cannot be negative")

    def duplicate_identity(self) -> tuple[str, int, float, str]:
        """Fields that must agree for a duplicate archive key to be harmless."""
        return (self.symbol, self.open_time, self.quote_volume, self.source_hash)


@dataclass(frozen=True)
class ManifestEntry:
    symbol: str
    episode_id: str
    first_valid_bar_open_time: int
    last_valid_bar_open_time: int
    observed_months: tuple[str, ...]
    missing_months: tuple[str, ...]
    unexplained_gaps: tuple[tuple[int, int], ...]
    uncertainty_status: str
    source_urls: tuple[str, ...]
    source_hashes: tuple[str, ...]


@dataclass(frozen=True)
class UniverseManifest:
    schema_version: str
    entries: tuple[ManifestEntry, ...]
    sha256: str

    def canonical_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "entries": [asdict(entry) for entry in self.entries],
        }


def canonical_manifest_hash(schema_version: str, entries: Sequence[ManifestEntry]) -> str:
    """Hash sorted, whitespace-free JSON; ordering cannot affect the manifest."""
    payload = {
        "schema_version": schema_version,
        "entries": [asdict(entry) for entry in sorted(entries, key=lambda item: item.episode_id)],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return sha256(encoded).hexdigest()


def _daily_gaps(bars: Sequence[ArchiveBar]) -> tuple[tuple[int, int], ...]:
    gaps: list[tuple[int, int]] = []
    for previous, current in pairwise(bars):
        if current.open_time - previous.open_time > DAY_MS:
            gaps.append((previous.open_time + DAY_MS, current.open_time - DAY_MS))
    return tuple(gaps)


def build_manifest(
    bars: Iterable[ArchiveBar],
    *,
    expected_months: Mapping[str, Sequence[str]] | None = None,
    renamed_or_migrated: Iterable[str] = (),
    current_exchange_info: object | None = None,
) -> UniverseManifest:
    """Build a versioned manifest without a current exchange metadata dependency.

    ``current_exchange_info`` is an explicit fail-closed trap so callers cannot
    accidentally use today's listing catalogue to construct historical members.
    A rename/migration hint creates a separate, quarantined episode; it never
    silently links two archive identities.
    """
    if current_exchange_info is not None:
        raise ValueError("current exchangeInfo is forbidden in universe construction")
    migration_symbols = {symbol.upper() for symbol in renamed_or_migrated}
    grouped: dict[str, dict[int, ArchiveBar]] = {}
    conflicts: set[str] = set()
    for bar in bars:
        by_time = grouped.setdefault(bar.symbol, {})
        existing = by_time.get(bar.open_time)
        if existing is None:
            by_time[bar.open_time] = bar
        elif existing.duplicate_identity() != bar.duplicate_identity():
            conflicts.add(bar.symbol)
    entries: list[ManifestEntry] = []
    for symbol in sorted(grouped):
        observed = tuple(sorted(grouped[symbol].values(), key=lambda item: item.open_time))
        months = tuple(sorted({_month(bar.open_time) for bar in observed}))
        expected = tuple(sorted(set((expected_months or {}).get(symbol, ()))))
        missing = tuple(month for month in expected if month not in months)
        gaps = _daily_gaps(observed)
        status = "CLEAR"
        if symbol in conflicts or symbol in migration_symbols or missing or gaps:
            status = "QUARANTINED"
        # A symbol is one archive episode.  Migration evidence must not merge it
        # with another symbol, so the suffix makes that conservative decision plain.
        episode_id = f"{symbol}:1" if symbol not in migration_symbols else f"{symbol}:migration-1"
        entries.append(
            ManifestEntry(
                symbol=symbol,
                episode_id=episode_id,
                first_valid_bar_open_time=observed[0].open_time,
                last_valid_bar_open_time=observed[-1].open_time,
                observed_months=months,
                missing_months=missing,
                unexplained_gaps=gaps,
                uncertainty_status=status,
                source_urls=tuple(sorted({bar.source_url for bar in observed})),
                source_hashes=tuple(sorted({bar.source_hash for bar in observed})),
            )
        )
    ordered = tuple(sorted(entries, key=lambda item: item.episode_id))
    return UniverseManifest(
        MANIFEST_SCHEMA_VERSION,
        ordered,
        canonical_manifest_hash(MANIFEST_SCHEMA_VERSION, ordered),
    )


def _prefix(bars: Iterable[ArchiveBar], as_of_open_time: int) -> dict[str, list[ArchiveBar]]:
    """Observations available at signal time: completed bars strictly before it."""
    result: dict[str, list[ArchiveBar]] = {}
    for bar in bars:
        if bar.open_time < as_of_open_time:
            result.setdefault(bar.symbol, []).append(bar)
    for symbol in result:
        result[symbol].sort(key=lambda item: item.open_time)
    return result


def eligible_symbols(
    bars: Iterable[ArchiveBar],
    as_of_open_time: int,
    *,
    listing_buffer_completed_bars: int = 30,
    gap_recovery_completed_bars: int = 30,
) -> tuple[str, ...]:
    """Causal members eligible at a signal; gaps require a completed-bar recovery."""
    if listing_buffer_completed_bars < 1 or gap_recovery_completed_bars < 1:
        raise ValueError("buffers must be positive")
    eligible: list[str] = []
    for symbol, history in _prefix(bars, as_of_open_time).items():
        if history[-1].open_time != as_of_open_time - DAY_MS:
            continue
        trailing = 1
        for current, previous in pairwise(reversed(history)):
            if current.open_time - previous.open_time != DAY_MS:
                break
            trailing += 1
        if (
            len(history) >= listing_buffer_completed_bars
            and trailing >= gap_recovery_completed_bars
        ):
            eligible.append(symbol)
    return tuple(sorted(eligible))


def causal_liquidity_ranking(
    bars: Iterable[ArchiveBar],
    as_of_open_time: int,
    *,
    lookback_completed_bars: int = 30,
) -> tuple[tuple[str, float], ...]:
    """Rank only contiguous completed bars; future values never enter the score."""
    if lookback_completed_bars < 1:
        raise ValueError("lookback must be positive")
    ranked: list[tuple[str, float]] = []
    for symbol, history in _prefix(bars, as_of_open_time).items():
        if (
            len(history) < lookback_completed_bars
            or history[-1].open_time != as_of_open_time - DAY_MS
        ):
            continue
        window = history[-lookback_completed_bars:]
        if any(
            right.open_time - left.open_time != DAY_MS
            for left, right in pairwise(window)
        ):
            continue
        ranked.append((symbol, sum(bar.quote_volume for bar in window) / len(window)))
    return tuple(sorted(ranked, key=lambda item: (-item[1], item[0])))


def next_real_bar(
    bars: Iterable[ArchiveBar], signal_open_time: int, *, delay_bars: int = 0
) -> ArchiveBar | None:
    """Return the next observed bar after a signal, plus an optional bar delay.

    No synthetic calendar bar is created: absence of a later archive bar yields
    ``None`` and is a no-fill terminal exposure.
    """
    if delay_bars < 0:
        raise ValueError("delay_bars cannot be negative")
    observations = list(bars)
    if len({bar.symbol for bar in observations}) > 1:
        raise ValueError("execution alignment requires one symbol episode")
    future = sorted(
        (bar for bar in observations if bar.open_time > signal_open_time),
        key=lambda item: item.open_time,
    )
    return future[delay_bars] if len(future) > delay_bars else None
