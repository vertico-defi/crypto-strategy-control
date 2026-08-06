"""Immutable, boundary-bound session construction."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType


class SessionInvariantError(ValueError):
    """Input rows cannot define a causal session panel."""


@dataclass(frozen=True)
class Row:
    asset: str
    timestamp: datetime
    close: float
    available: datetime | None = None

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() != timedelta(0):
            raise SessionInvariantError("row timestamp must be UTC")
        if self.available is not None and (
            self.available.tzinfo is None or self.available.utcoffset() != timedelta(0)
        ):
            raise SessionInvariantError("availability timestamp must be UTC")


@dataclass(frozen=True)
class BoundaryRowIndex:
    """Exact row index for a strict half-open boundary; never mutates rows."""

    boundary: datetime
    rows: tuple[Row, ...]
    by_asset_time: Mapping[tuple[str, datetime], Row]

    @classmethod
    def build(cls, rows: Iterable[Row], *, boundary: datetime) -> BoundaryRowIndex:
        if boundary.tzinfo is None or boundary.utcoffset() != timedelta(0):
            raise SessionInvariantError("boundary must be UTC")
        retained = tuple(
            row
            for row in rows
            if row.timestamp < boundary
            and (row.available is None or row.available < boundary)
        )
        keys: set[tuple[str, datetime]] = set()
        previous: dict[str, datetime] = {}
        indexed: dict[tuple[str, datetime], Row] = {}
        for row in retained:
            key = (row.asset, row.timestamp)
            if key in keys:
                raise SessionInvariantError(f"duplicate row key: {key}")
            prior = previous.get(row.asset)
            if prior is not None and row.timestamp <= prior:
                raise SessionInvariantError(f"nonmonotonic rows for {row.asset}")
            keys.add(key)
            previous[row.asset] = row.timestamp
            indexed[key] = row
        return cls(boundary=boundary, rows=retained, by_asset_time=MappingProxyType(indexed))

    def exact(self, asset: str, timestamp: datetime) -> Row | None:
        return self.by_asset_time.get((asset, timestamp))


@dataclass(frozen=True)
class Session:
    timestamp: datetime
    rows: Mapping[str, Row]
    complete: bool
    eligible: bool


def build_sessions(
    index: BoundaryRowIndex,
    *,
    timestamps: Iterable[datetime],
    assets: tuple[str, ...],
    quarantine: frozenset[datetime] = frozenset(),
) -> tuple[Session, ...]:
    """Build sessions from exact indexed rows; absent assets never borrow a later row."""

    result: list[Session] = []
    for timestamp in timestamps:
        rows = {
            asset: row
            for asset in assets
            if (row := index.exact(asset, timestamp)) is not None
        }
        complete = len(rows) == len(assets)
        result.append(
            Session(
                timestamp=timestamp,
                rows=MappingProxyType(rows),
                complete=complete,
                eligible=complete and timestamp not in quarantine,
            )
        )
    return tuple(result)


def minute_grid(start: datetime, end: datetime) -> tuple[datetime, ...]:
    """Half-open UTC minute grid used by deterministic fixtures and production adapters."""

    if start.tzinfo != UTC or end.tzinfo != UTC or end < start:
        raise SessionInvariantError("invalid UTC grid boundary")
    count = int((end - start).total_seconds() // 60)
    return tuple(start + timedelta(minutes=i) for i in range(count))
