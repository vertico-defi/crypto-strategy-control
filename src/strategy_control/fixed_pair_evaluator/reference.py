"""Intentionally independent standard-library reconciliation primitives."""

from __future__ import annotations

from collections.abc import Iterable, Mapping


def reference_turnover(
    current: Mapping[str, float], target: Mapping[str, float], *, include_cash: bool
) -> float:
    assets = set(current) | set(target)
    change = sum(abs(target.get(asset, 0.0) - current.get(asset, 0.0)) for asset in assets)
    if include_cash:
        current_cash = 1.0 - sum(current.values())
        target_cash = 1.0 - sum(target.values())
        return 0.5 * (change + abs(target_cash - current_cash))
    return change


def reference_compound(interval_returns: Iterable[float]) -> float:
    wealth = 1.0
    for value in interval_returns:
        if value <= -1.0:
            raise ValueError("invalid reference return")
        wealth *= 1.0 + value
    return wealth
