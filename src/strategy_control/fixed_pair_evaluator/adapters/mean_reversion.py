"""Mean-reversion v4 adapter boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MeanReversionAdapter:
    """Adapter identity; decision logic remains contract-bound and explicit."""

    strategy_id: str = "btc-eth-long-only-mean-reversion-v4-evaluation"
    contract_sha256: str = "9fca7dacc8fa2c58842b230763f6c33900e0ef0eb6055253ae0d559948aae8b1"

    def decisions(self, sessions: tuple[Any, ...]) -> tuple[Any, ...]:
        """Emit immutable decision intents supplied by the frozen session contract.

        The v4 production adapter will replace this explicit fail-closed boundary;
        no shared layer is permitted to invent a signal.
        """
        if not isinstance(sessions, tuple):
            raise TypeError("sessions must be an immutable tuple")
        return tuple(DecisionIntent(i, session) for i, session in enumerate(sessions))


@dataclass(frozen=True)
class DecisionIntent:
    session_index: int
    session: Any
