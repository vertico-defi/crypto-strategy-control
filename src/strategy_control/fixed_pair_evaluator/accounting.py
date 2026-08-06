"""Small self-financing cash ledger used by both fixed-pair adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True)
class Fill:
    asset: str
    price: float
    target_weight: float
    cost_bps: float


@dataclass(frozen=True)
class PortfolioRebalance:
    prices: Mapping[str, float]
    target_weights: Mapping[str, float]
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
    return CashLedger(cash=ledger.cash - gross - cost, units=MappingProxyType(units))


def rebalance(
    ledger: CashLedger,
    plan: PortfolioRebalance,
    *,
    equity: float,
) -> CashLedger:
    """Apply one atomic target-weight transition with turnover costs."""
    if equity < 0 or plan.cost_bps < 0:
        raise ValueError("invalid rebalance")
    assets = set(ledger.units) | set(plan.prices) | set(plan.target_weights)
    current_weights = {
        asset: ledger.units.get(asset, 0.0) * plan.prices.get(asset, 0.0) / equity
        if equity
        else 0.0
        for asset in assets
    }
    turnover = sum(
        abs(plan.target_weights.get(asset, 0.0) - current_weights[asset])
        for asset in assets
    )
    post_cost_equity = equity - equity * turnover * plan.cost_bps / 10_000
    units = {
        asset: post_cost_equity * plan.target_weights.get(asset, 0.0) / plan.prices[asset]
        for asset in assets
        if plan.target_weights.get(asset, 0.0) > 0
    }
    return CashLedger(
        cash=post_cost_equity * (1.0 - sum(plan.target_weights.values())),
        units=MappingProxyType(units),
    )


def terminal_liquidation(
    ledger: CashLedger, prices: Mapping[str, float], cost_bps: float
) -> CashLedger:
    """Liquidate all units at exact terminal prices."""
    gross = ledger.cash + sum(ledger.units[asset] * prices[asset] for asset in ledger.units)
    risky = sum(ledger.units[asset] * prices[asset] for asset in ledger.units)
    return CashLedger(
        cash=gross - risky * cost_bps / 10_000,
        units=MappingProxyType({}),
    )
