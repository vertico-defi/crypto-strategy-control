"""Stage-oriented development evaluation shell.

The module deliberately refuses aggregate economics until an adapter supplies a
contract-bound trace and an independent reconciliation report.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, TypeVar

T = TypeVar("T")


class StrategyAdapter(Protocol):
    strategy_id: str

    def decisions(self, sessions: tuple[Any, ...]) -> tuple[Any, ...]: ...


@dataclass(frozen=True)
class StageResult:
    stage: str
    passed: bool
    evidence: dict[str, Any]


def strict_prefix(
    items: tuple[T, ...], *, timestamp_of: Any, boundary: datetime
) -> tuple[T, ...]:
    """Return only items strictly before a half-open fold boundary."""
    return tuple(item for item in items if timestamp_of(item) < boundary)


def delayed_execution_queue(decisions: tuple[T, ...]) -> tuple[tuple[T | None, T], ...]:
    """Pair each decision with the exact next execution slot; first slot is empty."""
    pairs: list[tuple[T | None, T]] = [(None, decisions[0])]
    pairs.extend((decisions[i - 1], decisions[i]) for i in range(1, len(decisions)))
    return tuple(pairs)


def require_independent_reconciliation(
    production_trace_hash: str,
    reference_trace_hash: str,
    *,
    reference_implementation_id: str,
) -> StageResult:
    if not reference_implementation_id or production_trace_hash != reference_trace_hash:
        return StageResult(
            "independent_reference_reconciled",
            False,
            {
                "production_trace_hash": production_trace_hash,
                "reference_trace_hash": reference_trace_hash,
            },
        )
    return StageResult(
        "independent_reference_reconciled",
        True,
        {
            "production_trace_hash": production_trace_hash,
            "reference_trace_hash": reference_trace_hash,
        },
    )
