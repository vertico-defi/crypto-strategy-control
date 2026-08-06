"""Relative-value v4 adapter boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


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
