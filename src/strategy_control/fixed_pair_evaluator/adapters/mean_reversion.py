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
        raise NotImplementedError(
            "mean-reversion contract adapter is staged after architecture review"
        )
