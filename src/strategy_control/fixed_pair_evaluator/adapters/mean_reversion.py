"""Mean-reversion v4 adapter boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from strategy_control.mean_reversion_v2 import Clock, Decision, Target, Trial


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


@dataclass(frozen=True)
class MeanDecisionTrace:
    decisions: tuple[Decision, ...]
    targets: tuple[Target, ...]


def run_contract_clock(
    trial: Trial,
    rows: tuple[dict[str, Any], ...],
    *,
    delay: int = 0,
) -> MeanDecisionTrace:
    """Run the frozen per-asset clock without changing signal economics."""
    clock = Clock(trial, delay)
    decisions: list[Decision] = []
    targets: list[Target] = []
    for row in rows:
        decision, target = clock.decide(
            str(row["asset"]),
            row["session"],
            row["fill_time"],
            int(row["fill_index"]),
            row.get("signal"),
            delayed_fill_time=row.get("delayed_fill_time"),
            raw_daily_return=row.get("raw_daily_return"),
        )
        decisions.append(decision)
        if target is not None:
            targets.append(target)
        if row.get("fill_price") is not None:
            clock.apply_fill(row["fill_time"], float(row["fill_price"]), int(row["fill_index"]))
    return MeanDecisionTrace(tuple(decisions), tuple(targets))
