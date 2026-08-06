"""Relative-value v4 adapter boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from strategy_control.relative_value_v2 import decision_for_scores


@dataclass(frozen=True)
class RelativeValueAdapter:
    strategy_id: str = "btc-eth-relative-value-rotation-v4-evaluation"
    contract_sha256: str = "ee1c3246d4941043900a89b968d748176050c564022bf14e7a27e6b38a3df7c9"

    def decisions(self, sessions: tuple[Any, ...]) -> tuple[Any, ...]:
        if not isinstance(sessions, tuple):
            raise TypeError("sessions must be an immutable tuple")
        return tuple(DecisionIntent(i, session) for i, session in enumerate(sessions))


@dataclass(frozen=True)
class DecisionIntent:
    session_index: int
    session: Any


@dataclass(frozen=True)
class RotationDecisionIntent:
    session_index: int
    actual_before: str
    desired: str
    score_identities: tuple[str, ...]


def decide_contract_target(
    trial: str,
    scores: dict[str, float | None],
    raw_returns: dict[str, list[float]],
    actual: str,
    *,
    session_index: int,
    score_identities: tuple[str, ...],
) -> RotationDecisionIntent:
    """Bind the frozen relative-value decision formula to an immutable identity."""
    desired = decision_for_scores(trial, scores, raw_returns, actual)
    return RotationDecisionIntent(session_index, actual, desired, score_identities)
