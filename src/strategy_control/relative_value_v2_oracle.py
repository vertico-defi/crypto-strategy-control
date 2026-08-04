"""Independent, standard-library synthetic oracle for relative-value v2.

It intentionally imports no production relative-value module.  Its algorithms are
separate implementations used only to reconcile deterministic fixture traces.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

ASSETS = ("BTCUSDT", "ETHUSDT")
CASH = "CASH"
COST = 0.0014


class OracleError(ValueError):
    pass


@dataclass(frozen=True)
class OracleRow:
    asset: str
    timestamp: datetime
    price: float
    row_id: str


@dataclass(frozen=True)
class OracleTrace:
    session_id: str
    target: str
    before: str
    after: str
    pending: str | None
    disposition: str
    wealth: float


def oracle_cutoff(records: Sequence[tuple[str, datetime, datetime, float]]) -> datetime:
    if not records or {r[0] for r in records} != set(ASSETS):
        raise OracleError("incomplete lookback")
    if any(not math.isfinite(r[3]) for r in records):
        raise OracleError("nonfinite observation")
    return max(max(_utc(r[1]), _utc(r[2])) for r in records)


def oracle_vector_after(
    cutoff: datetime, rows: Sequence[OracleRow], end: datetime
) -> tuple[OracleRow, OracleRow]:
    prior: dict[str, datetime] = {}
    grouped: dict[datetime, dict[str, OracleRow]] = {}
    boundary = _utc(end)
    for row in rows:
        stamp = _utc(row.timestamp)
        if (
            row.asset not in ASSETS
            or not row.row_id
            or not math.isfinite(row.price)
            or row.price <= 0
        ):
            raise OracleError("bad row")
        if row.asset in prior and stamp <= prior[row.asset]:
            raise OracleError("unordered row")
        prior[row.asset] = stamp
        if stamp < boundary:
            if row.asset in grouped.setdefault(stamp, {}):
                raise OracleError("duplicate")
            grouped[stamp][row.asset] = row
    exact = [
        stamp
        for stamp, pair in grouped.items()
        if stamp > _utc(cutoff) and set(pair) == set(ASSETS)
    ]
    if not exact:
        raise OracleError("no vector")
    pair = grouped[min(exact)]
    return pair[ASSETS[0]], pair[ASSETS[1]]


def oracle_clock(
    decisions: Sequence[tuple[str, datetime]], *, delayed: bool
) -> tuple[OracleTrace, ...]:
    if not decisions:
        raise OracleError("empty clock")
    actual = CASH
    pending = None
    wealth = 1.0
    result = []
    for i, (session, _stamp) in enumerate(decisions):
        target = session
        if target not in {CASH, *ASSETS}:
            raise OracleError("target")
        before = actual
        terminal = i == len(decisions) - 1
        if terminal:
            actual = CASH
            pending = None
            disp = "terminal_cash"
        elif delayed:
            if pending is not None:
                actual = pending
            pending = target
            disp = "queued"
        else:
            actual = target
            disp = "executed"
        wealth *= 1 - (COST if before != actual else 0.0)
        result.append(OracleTrace(str(i), target, before, actual, pending, disp, wealth))
    return tuple(result)


def oracle_target(
    btc_score: float,
    eth_score: float,
    btc_raw: Sequence[float],
    eth_raw: Sequence[float],
    actual: str,
    *,
    gap: float = 0.25,
    cash_filter: bool = True,
) -> str:
    """Independent fixed state/tie/cash-filter decision for representative traces."""
    if actual not in {CASH, *ASSETS} or not all(
        math.isfinite(value) for value in (btc_score, eth_score, *btc_raw, *eth_raw)
    ):
        raise OracleError("decision input")
    if btc_score == eth_score:
        return actual
    winner = ASSETS[0] if btc_score > eth_score else ASSETS[1]
    winner_raw = btc_raw if winner == ASSETS[0] else eth_raw
    if cash_filter and sorted(winner_raw)[len(winner_raw) // 2] <= 0:
        return CASH if actual == CASH else actual
    winner_score = btc_score if winner == ASSETS[0] else eth_score
    loser_score = eth_score if winner == ASSETS[0] else btc_score
    if actual == CASH:
        return winner if winner_score - loser_score >= gap else CASH
    if actual == winner:
        return winner
    actual_score = btc_score if actual == ASSETS[0] else eth_score
    return winner if winner_score - actual_score >= gap else actual


def oracle_rebalance(
    wealth: float,
    actual: str,
    target: str,
    returns: Mapping[str, float],
    cost_rate: float = COST,
) -> tuple[float, float, float, dict[str, float]]:
    """Independent self-financing gross, turnover, cost, and attribution identity."""
    if (
        actual not in {CASH, *ASSETS}
        or target not in {CASH, *ASSETS}
        or set(returns) != set(ASSETS)
    ):
        raise OracleError("accounting identity")
    if not all(math.isfinite(x) for x in (wealth, cost_rate, *returns.values())) or wealth <= 0:
        raise OracleError("nonfinite accounting")
    old = {asset: float(actual == asset) for asset in ASSETS}
    gross = 1 + sum(old[asset] * returns[asset] for asset in ASSETS)
    if gross <= 0:
        raise OracleError("unpriceable exposure")
    drift = {asset: old[asset] * (1 + returns[asset]) / gross for asset in ASSETS}
    cash_drift = 1 - sum(drift.values())
    new = {asset: float(target == asset) for asset in ASSETS}
    turnover = 0.5 * (
        sum(abs(new[asset] - drift[asset]) for asset in ASSETS)
        + abs((1 - sum(new.values())) - cash_drift)
    )
    cost = wealth * gross * turnover * cost_rate
    attribution = {asset: wealth * drift[asset] * returns[asset] for asset in ASSETS}
    return wealth * gross - cost, turnover, cost, attribution


def oracle_gate(value: float, threshold: float, relation: str) -> bool:
    if not math.isfinite(value) or not math.isfinite(threshold):
        return False
    return (
        value > threshold
        if relation == ">"
        else value >= threshold
        if relation == ">="
        else value <= threshold
        if relation == "<="
        else False
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise OracleError("naive time")
    return value.astimezone(UTC)
