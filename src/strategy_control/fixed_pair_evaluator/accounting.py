"""Small self-financing cash ledger used by both fixed-pair adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Fill:
    asset: str
    price: float
    target_weight: float
    cost_bps: float


@dataclass(frozen=True)
class CashLedger:
    cash: float
    units: Mapping[str, float]


def apply_fill(
    ledger: CashLedger,
    fill: Fill,
    *,
    equity_at_price: float,
) -> CashLedger:
    if equity_at_price < 0 or fill.price <= 0 or not 0 <= fill.target_weight <= 1:
        raise ValueError("invalid self-financing fill")
    gross = equity_at_price * fill.target_weight
    cost = gross * fill.cost_bps / 10_000
    units = dict(ledger.units)
    units[fill.asset] = gross / fill.price
    return CashLedger(cash=ledger.cash - gross - cost, units=units)
