"""Causal exact target and execution-row oracles."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .session import BoundaryRowIndex, Row


class MissingExecutionRow(LookupError):
    """An exact requested execution row is absent; no forward scan is allowed."""


@dataclass(frozen=True)
class ExactExecutionOracle:
    index: BoundaryRowIndex

    def lookup(self, asset: str, timestamp: datetime) -> Row:
        row = self.index.exact(asset, timestamp)
        if row is None:
            raise MissingExecutionRow(
                f"missing exact execution row for {asset} at {timestamp.isoformat()}"
            )
        return row

    def lookup_synchronized(self, assets: tuple[str, ...], timestamp: datetime) -> tuple[Row, ...]:
        rows = tuple(self.lookup(asset, timestamp) for asset in assets)
        return rows
