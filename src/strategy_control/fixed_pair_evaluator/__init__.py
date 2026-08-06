"""Contract-driven shared mechanics for the Phase 4 fixed-pair evaluator.

This package deliberately contains mechanics only.  Strategy economics remain in
the versioned adapters and frozen predecessor contracts.
"""

from .accounting import (
    CashLedger,
    Fill,
    PortfolioRebalance,
    apply_fill,
    rebalance,
    terminal_liquidation,
)
from .oracle import ExactExecutionOracle, MissingExecutionRow
from .session import BoundaryRowIndex, Session, build_sessions

__all__ = [
    "BoundaryRowIndex",
    "CashLedger",
    "ExactExecutionOracle",
    "Fill",
    "MissingExecutionRow",
    "PortfolioRebalance",
    "Session",
    "apply_fill",
    "build_sessions",
    "rebalance",
    "terminal_liquidation",
]
