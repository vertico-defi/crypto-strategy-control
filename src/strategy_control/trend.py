"""Pure, fail-closed primitives for the frozen BTC/ETH trend experiment.

This module deliberately accepts already-authorized in-memory observations only.
It has no filesystem, network, or order-routing capability and therefore cannot
open the final holdout by accident.
"""

from __future__ import annotations

import hashlib
import importlib
import itertools
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from statistics import NormalDist, median
from typing import Any


class TrendError(ValueError):
    """Raised when an input cannot satisfy the frozen causal contract."""


@dataclass(frozen=True)
class MinuteBar:
    event_end: datetime
    available_at: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class DailyBar:
    session: datetime
    available_at: datetime
    open: float
    high: float
    low: float
    close: float
    complete: bool


@dataclass(frozen=True)
class Fill:
    timestamp: datetime
    prices: Mapping[str, float]
    targets: Mapping[str, float]


@dataclass(frozen=True)
class IntervalResult:
    start: datetime
    end: datetime
    net_return: float
    equity: float
    turnover: float
    cost: float


def first_strictly_causal_fill(
    day: DailyBar, candidate_bars: Sequence[MinuteBar]
) -> MinuteBar | None:
    """Return the first bar whose open is strictly after all session information."""

    session_end = _utc(day.session) + timedelta(days=1)
    cutoff = max(session_end, _utc(day.available_at))
    for bar in candidate_bars:
        end = _utc(bar.event_end)
        open_timestamp = end - timedelta(minutes=1)
        if open_timestamp > cutoff and _utc(bar.available_at) >= end:
            return bar
    return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise TrendError("timestamps must be timezone-aware UTC")
    normalized = value.astimezone(UTC)
    if normalized.utcoffset() != timedelta(0):  # defensive; astimezone always makes UTC
        raise TrendError("timestamps must be UTC")
    return normalized


def _finite_positive(value: float, label: str) -> None:
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise TrendError(f"{label} must be finite and positive")


def aggregate_daily_sessions(bars: Sequence[MinuteBar]) -> list[DailyBar]:
    """Aggregate end-stamped minute bars into complete UTC sessions.

    A session labelled D comprises ends D 00:01 through D+1 00:00, exactly as
    frozen.  Any duplicate, non-monotonic, malformed or incomplete session is
    represented as an incomplete DailyBar rather than silently filled.
    """

    grouped: dict[datetime, list[MinuteBar]] = {}
    previous: datetime | None = None
    for bar in bars:
        end = _utc(bar.event_end)
        available = _utc(bar.available_at)
        if available < end:
            raise TrendError("available timestamp precedes event end")
        if previous is not None and end <= previous:
            raise TrendError("duplicate or non-monotonic minute timestamp")
        previous = end
        for value, name in (
            (bar.open, "open"),
            (bar.high, "high"),
            (bar.low, "low"),
            (bar.close, "close"),
        ):
            _finite_positive(value, name)
        if bar.low > min(bar.open, bar.close) or bar.high < max(bar.open, bar.close):
            raise TrendError("invalid OHLC range")
        # minus one nanosecond is equivalent to assigning a midnight end to prior day.
        session = (end - timedelta(microseconds=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        grouped.setdefault(session, []).append(bar)
    output: list[DailyBar] = []
    for session in sorted(grouped):
        rows = grouped[session]
        expected = [session + timedelta(minutes=index) for index in range(1, 1441)]
        complete = len(rows) == 1440 and [row.event_end for row in rows] == expected
        first, last = rows[0], rows[-1]
        output.append(
            DailyBar(
                session,
                max(row.available_at for row in rows),
                first.open,
                max(row.high for row in rows),
                min(row.low for row in rows),
                last.close,
                complete,
            )
        )
    return output


def post_information_eligible(days: Sequence[DailyBar], recovery_sessions: int = 150) -> list[bool]:
    """Return strict eligibility after incomplete sessions reset all state."""

    if recovery_sessions < 1:
        raise TrendError("recovery_sessions must be positive")
    result: list[bool] = []
    run = 0
    previous: datetime | None = None
    for day in days:
        session = _utc(day.session)
        if previous is not None and session != previous + timedelta(days=1):
            run = 0
        previous = session
        if not day.complete:
            run = 0
            result.append(False)
            continue
        run += 1
        result.append(run >= recovery_sessions)
    return result


def _window(values: Sequence[float], end: int, length: int) -> Sequence[float]:
    if length < 1 or end < length:
        return ()
    return values[end - length : end]


def donchian_ensemble(
    days: Sequence[DailyBar], lookbacks: Sequence[int] = (20, 60, 120)
) -> list[float | None]:
    """Long/cash state averaged across prior-range Donchian rules."""

    if not lookbacks or any(length < 1 for length in lookbacks):
        raise TrendError("Donchian lookbacks must be positive")
    highs, lows = [day.high for day in days], [day.low for day in days]
    closes = [day.close for day in days]
    states = [0.0] * len(lookbacks)
    output: list[float | None] = []
    for index, close in enumerate(closes):
        valid = True
        for rule, length in enumerate(lookbacks):
            prior_high, prior_low = _window(highs, index, length), _window(lows, index, length)
            if not prior_high:
                valid = False
                continue
            if close > max(prior_high):
                states[rule] = 1.0
            elif close < min(prior_low):
                states[rule] = 0.0
        output.append(sum(states) / len(states) if valid else None)
    return output


def time_series_momentum(
    days: Sequence[DailyBar], lookbacks: Sequence[int] = (20, 60, 120)
) -> list[float | None]:
    if not lookbacks or any(length < 1 for length in lookbacks):
        raise TrendError("momentum lookbacks must be positive")
    closes = [day.close for day in days]
    output: list[float | None] = []
    for index, close in enumerate(closes):
        signals: list[float] = []
        for length in lookbacks:
            if index < length:
                continue
            signals.append(1.0 if close / closes[index - length] - 1.0 > 0 else 0.0)
        output.append(sum(signals) / len(lookbacks) if len(signals) == len(lookbacks) else None)
    return output


def realized_volatility(
    days: Sequence[DailyBar], lookback: int = 20, annualization: int = 365
) -> list[float | None]:
    if lookback < 2 or annualization < 1:
        raise TrendError("invalid volatility settings")
    closes = [day.close for day in days]
    output: list[float | None] = []
    for index in range(len(closes)):
        if index < lookback:
            output.append(None)
            continue
        returns = [
            closes[pos] / closes[pos - 1] - 1.0 for pos in range(index - lookback + 1, index + 1)
        ]
        mean = sum(returns) / lookback
        variance = sum((value - mean) ** 2 for value in returns) / (lookback - 1)
        output.append(math.sqrt(variance) * math.sqrt(annualization))
    return output


def _complete_segments(days: Sequence[DailyBar]) -> list[tuple[int, int]]:
    segments: list[tuple[int, int]] = []
    start: int | None = None
    previous: datetime | None = None
    for index, day in enumerate(days):
        session = _utc(day.session)
        contiguous = previous is None or session == previous + timedelta(days=1)
        if not day.complete or not contiguous:
            if start is not None:
                segments.append((start, index))
            start = index if day.complete else None
        elif start is None:
            start = index
        previous = session
    if start is not None:
        segments.append((start, len(days)))
    return segments


def primary_exposure(
    days: Sequence[DailyBar],
    asset_weight: float = 0.5,
    target_vol: float = 0.15,
    lookbacks: Sequence[int] = (20, 60, 120),
    recovery_sessions: int = 150,
    mode: str = "combined",
) -> list[float | None]:
    """Frozen causal exposure with complete-session state reset and recovery."""

    if (
        not 0 <= asset_weight <= 1
        or not 0 < target_vol <= 1
        or recovery_sessions < max(lookbacks)
        or mode not in {"combined", "donchian", "momentum"}
    ):
        raise TrendError("invalid target exposure or volatility")
    output: list[float | None] = [None] * len(days)
    for start, end in _complete_segments(days):
        segment = days[start:end]
        donchian = donchian_ensemble(segment, lookbacks)
        momentum = time_series_momentum(segment, lookbacks)
        volatility = realized_volatility(segment)
        for offset in range(len(segment)):
            if offset + 1 < recovery_sessions:
                continue
            donchian_value = donchian[offset]
            momentum_value = momentum[offset]
            observed_volatility = volatility[offset]
            if donchian_value is None or momentum_value is None:
                continue
            if observed_volatility is None or not math.isfinite(observed_volatility):
                continue
            if observed_volatility <= 0:
                output[start + offset] = 0.0
                continue
            raw_signal = {
                "combined": (donchian_value + momentum_value) / 2.0,
                "donchian": donchian_value,
                "momentum": momentum_value,
            }[mode]
            scalar = min(1.0, target_vol / observed_volatility)
            output[start + offset] = asset_weight * raw_signal * scalar
    return output


def self_financing(fills: Sequence[Fill], one_way_cost_bps: float = 14.0) -> list[IntervalResult]:
    """Mark, deduct target-vs-drift turnover, allocate, then terminal-liquidate."""

    if one_way_cost_bps < 0:
        raise TrendError("cost must be non-negative")
    if len(fills) < 2:
        return []
    cost_rate = one_way_cost_bps / 10_000.0
    assets = tuple(sorted(fills[0].prices))
    if not assets:
        raise TrendError("fills need at least one asset")
    if any(abs(float(target)) > 1e-15 for target in fills[-1].targets.values()):
        raise TrendError("final fill targets must be cash for terminal liquidation")
    equity, holdings, previous_prices = 1.0, {asset: 0.0 for asset in assets}, dict(fills[0].prices)
    previous_time: datetime | None = None
    results: list[IntervalResult] = []
    for index, fill in enumerate(fills):
        timestamp = _utc(fill.timestamp)
        if previous_time is not None and timestamp <= previous_time:
            raise TrendError("fills must be strictly chronological")
        if tuple(sorted(fill.prices)) != assets or set(fill.targets) != set(assets):
            raise TrendError("inconsistent fill assets")
        for asset in assets:
            _finite_positive(fill.prices[asset], f"price {asset}")
            target = fill.targets[asset]
            if (
                not isinstance(target, (int, float))
                or not math.isfinite(target)
                or target < 0
                or target > 1
            ):
                raise TrendError("target must be a finite long-only weight")
        if sum(fill.targets.values()) > 1 + 1e-12:
            raise TrendError("gross exposure exceeds one")
        if index:
            equity = sum(holdings[a] * fill.prices[a] for a in assets) + (
                equity - sum(holdings[a] * previous_prices[a] for a in assets)
            )
        risky_value = {a: holdings[a] * fill.prices[a] for a in assets}
        pre_weights = {a: risky_value[a] / equity for a in assets}
        turnover = sum(abs(fill.targets[a] - pre_weights[a]) for a in assets)
        cost = equity * cost_rate * turnover
        equity -= cost
        if equity <= 0:
            raise TrendError("cost depleted equity")
        for asset in assets:
            holdings[asset] = equity * fill.targets[asset] / fill.prices[asset]
        previous_prices = dict(fill.prices)
        if index:
            assert previous_time is not None
            # Interval return includes the mark to this causal fill and this fill's rebalance cost.
            results.append(
                IntervalResult(
                    previous_time,
                    timestamp,
                    equity / results[-1].equity - 1.0 if results else equity - 1.0,
                    equity,
                    turnover,
                    cost,
                )
            )
        previous_time = timestamp
    return results


def fold_intervals(
    intervals: Sequence[IntervalResult], start: datetime, end: datetime
) -> list[IntervalResult]:
    start, end = _utc(start), _utc(end)
    if end <= start:
        raise TrendError("invalid fold boundary")
    return [item for item in intervals if start <= item.start < end and item.end < end]


def delayed_fills(fills: Sequence[Fill], sessions: int = 1) -> list[Fill]:
    if sessions < 1:
        raise TrendError("delay must be positive")
    delayed = [
        Fill(
            fill.timestamp,
            fill.prices,
            fills[index - sessions].targets
            if index >= sessions
            else {a: 0.0 for a in fill.targets},
        )
        for index, fill in enumerate(fills)
    ]
    if delayed:
        last = delayed[-1]
        delayed[-1] = Fill(last.timestamp, last.prices, {asset: 0.0 for asset in last.targets})
    return delayed


def buy_and_hold(
    fills: Sequence[Fill], initial_weights: Mapping[str, float], one_way_cost_bps: float = 14.0
) -> list[IntervalResult]:
    """Buy once at the first causal fill, drift without rebalancing, and liquidate."""

    if len(fills) < 2 or one_way_cost_bps < 0:
        raise TrendError("invalid buy-and-hold inputs")
    assets = tuple(sorted(fills[0].prices))
    if set(initial_weights) != set(assets) or sum(initial_weights.values()) > 1 + 1e-12:
        raise TrendError("invalid buy-and-hold weights")
    if any(weight < 0 or not math.isfinite(weight) for weight in initial_weights.values()):
        raise TrendError("invalid buy-and-hold weights")
    timestamps = [_utc(fill.timestamp) for fill in fills]
    if any(timestamps[index] <= timestamps[index - 1] for index in range(1, len(timestamps))):
        raise TrendError("fills must be strictly chronological")
    for fill in fills:
        if tuple(sorted(fill.prices)) != assets:
            raise TrendError("inconsistent fill assets")
        for asset in assets:
            _finite_positive(fill.prices[asset], f"price {asset}")
    rate = one_way_cost_bps / 10_000.0
    initial_turnover = sum(initial_weights.values())
    equity = 1.0 - rate * initial_turnover
    if equity <= 0:
        raise TrendError("cost depleted equity")
    holdings = {
        asset: equity * initial_weights[asset] / fills[0].prices[asset] for asset in assets
    }
    cash = equity * (1 - sum(initial_weights.values()))
    results: list[IntervalResult] = []
    previous_equity = 1.0
    for index in range(1, len(fills)):
        marked = cash + sum(holdings[asset] * fills[index].prices[asset] for asset in assets)
        turnover = initial_turnover if index == 1 else 0.0
        cost = rate * initial_turnover if index == 1 else 0.0
        if index == len(fills) - 1:
            terminal_turnover = sum(
                holdings[asset] * fills[index].prices[asset] / marked for asset in assets
            )
            terminal_cost = marked * rate * terminal_turnover
            marked -= terminal_cost
            turnover += terminal_turnover
            cost += terminal_cost
        results.append(
            IntervalResult(
                timestamps[index - 1],
                timestamps[index],
                marked / previous_equity - 1,
                marked,
                turnover,
                cost,
            )
        )
        previous_equity = marked
    return results


def _mean(values: Sequence[float]) -> float:
    if not values:
        raise TrendError("empty observations")
    return sum(values) / len(values)


def daily_sharpe(values: Sequence[float]) -> float:
    if not values:
        raise TrendError("empty observations")
    if len(values) == 1:
        return math.inf if values[0] > 0 else -math.inf
    mean = _mean(values)
    sd = math.sqrt(sum((x - mean) ** 2 for x in values) / (len(values) - 1))
    if sd == 0:
        return math.inf if mean > 0 else -math.inf
    return mean / sd


def maximum_drawdown(returns: Sequence[float]) -> float:
    equity, peak, maximum = 1.0, 1.0, 0.0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        maximum = max(maximum, 1.0 - equity / peak)
    return maximum


def stationary_bootstrap(
    values: Sequence[float],
    resamples: int = 2000,
    block_length: int = 20,
    experiment_id: str = "btc-eth-vol-targeted-trend-v1",
    rng: Any | None = None,
) -> dict[str, Any]:
    if not values or resamples < 1 or block_length < 1:
        raise TrendError("invalid bootstrap inputs")
    if any(not math.isfinite(value) for value in values):
        raise TrendError("bootstrap observations must be finite")
    seed = int.from_bytes(hashlib.sha256(experiment_id.encode()).digest()[:8], "big")
    if rng is None:
        numpy = importlib.import_module("numpy")
        rng = numpy.random.Generator(numpy.random.PCG64(seed))
    probability = 1.0 / block_length
    means: list[float] = []
    count = len(values)
    for _ in range(resamples):
        index = int(rng.integers(count))
        sample: list[float] = []
        for _ in range(count):
            sample.append(values[index])
            index = (
                int(rng.integers(count))
                if float(rng.random()) < probability
                else (index + 1) % count
            )
        means.append(_mean(sample))
    means.sort()

    def quantile(q: float) -> float:
        position = (len(means) - 1) * q
        low, high = math.floor(position), math.ceil(position)
        return (
            means[low]
            if low == high
            else means[low] + (means[high] - means[low]) * (position - low)
        )

    return {
        "seed": seed,
        "mean": _mean(values),
        "lower_95": quantile(0.025),
        "upper_95": quantile(0.975),
        "resamples": resamples,
    }


def deflated_sharpe_probability(
    primary: Sequence[float], alternatives: Sequence[Sequence[float]]
) -> float:
    if (
        len(alternatives) != 7
        or len(primary) < 4
        or any(len(v) != len(primary) for v in alternatives)
    ):
        raise TrendError("DSR requires seven aligned alternatives and four observations")
    sharpes = [daily_sharpe(values) for values in alternatives]
    if any(not math.isfinite(value) for value in sharpes):
        return 0.0
    mean_s = _mean(sharpes)
    sd_s = math.sqrt(sum((x - mean_s) ** 2 for x in sharpes) / 6)
    normal = NormalDist()
    gamma = 0.5772156649015329
    sr0 = sd_s * (
        (1 - gamma) * normal.inv_cdf(1 - 1 / 7) + gamma * normal.inv_cdf(1 - 1 / (7 * math.e))
    )
    observed = daily_sharpe(primary)
    if not math.isfinite(observed):
        return 1.0 if observed > 0 else 0.0
    n, mean = len(primary), _mean(primary)
    sd = math.sqrt(sum((x - mean) ** 2 for x in primary) / (n - 1))
    skew = n / ((n - 1) * (n - 2)) * sum(((x - mean) / sd) ** 3 for x in primary)
    excess_kurtosis = (n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))) * sum(
        ((x - mean) / sd) ** 4 for x in primary
    ) - 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))
    nonexcess_kurtosis = excess_kurtosis + 3
    denominator = 1 - skew * observed + (nonexcess_kurtosis - 1) * observed * observed / 4
    if denominator <= 0:
        return 0.0
    return NormalDist().cdf((observed - sr0) * math.sqrt(n - 1) / math.sqrt(denominator))


def cscv_pbo(alternatives: Sequence[Sequence[float]]) -> float:
    """Exact 8-block/70-split CSCV with frozen ordered tie handling."""
    if (
        len(alternatives) != 7
        or not alternatives
        or len(alternatives[0]) < 8
        or any(len(x) != len(alternatives[0]) for x in alternatives)
    ):
        raise TrendError("PBO requires seven aligned series with at least eight observations")
    length = len(alternatives[0])
    q, r = divmod(length, 8)
    cuts, cursor = [], 0
    for index in range(8):
        size = q + (1 if index < r else 0)
        cuts.append((cursor, cursor + size))
        cursor += size
    events = 0
    for train_blocks in itertools.combinations(range(8), 4):
        train = set(train_blocks)
        test = [i for i in range(8) if i not in train]
        train_values = [
            [x for block in train for x in series[cuts[block][0] : cuts[block][1]]]
            for series in alternatives
        ]
        selected = max(range(7), key=lambda i: (daily_sharpe(train_values[i]), -i))
        test_sharpes = [
            daily_sharpe([x for block in test for x in series[cuts[block][0] : cuts[block][1]]])
            for series in alternatives
        ]
        score = test_sharpes[selected]
        rank = (
            sum(value < score for value in test_sharpes)
            + sum(value <= score for value in test_sharpes)
            + 1
        ) / 2
        relative_rank = rank / 8.0
        if math.log(relative_rank / (1 - relative_rank)) <= 0:
            events += 1
    return events / 70.0


def regime_labels(btc_days: Sequence[DailyBar]) -> list[str | None]:
    closes = [d.close for d in btc_days]
    vols = realized_volatility(btc_days, 60)
    prior_vols: list[float] = []
    labels: list[str | None] = []
    for i, vol in enumerate(vols):
        if i < 120 or vol is None or len(prior_vols) < 120:
            labels.append(None)
        else:
            trend = "up" if closes[i] / closes[i - 120] - 1 > 0 else "down"
            prior_median = median(prior_vols)
            labels.append(f"{trend}_{'high' if vol > prior_median else 'low'}")
        if vol is not None:
            prior_vols.append(vol)
    return labels


def aggregate_return(returns: Sequence[float]) -> float:
    equity = 1.0
    for value in returns:
        if not math.isfinite(value) or value <= -1:
            raise TrendError("invalid return observation")
        equity *= 1 + value
    return equity - 1


def exceptional_trade_concentration(intervals: Sequence[IntervalResult]) -> dict[str, Any]:
    if not intervals:
        raise TrendError("empty intervals")
    previous_equity = 1.0
    pnl: list[float] = []
    for interval in intervals:
        contribution = interval.equity - previous_equity
        pnl.append(contribution)
        previous_equity = interval.equity
    total = previous_equity - 1.0
    positives = sorted((value for value in pnl if value > 0), reverse=True)
    largest_fraction = positives[0] / total if total > 0 and positives else math.inf
    top_five_fraction = sum(positives[:5]) / total if total > 0 and positives else math.inf
    return {
        "largest_positive_day_fraction_of_positive_total_pnl": largest_fraction,
        "top_five_positive_days_fraction_of_positive_total_pnl": top_five_fraction,
        "pass": largest_fraction <= 0.5 and top_five_fraction <= 0.75,
    }


def evaluate_gates(metrics: Mapping[str, Any], gates: Mapping[str, Any]) -> dict[str, bool]:
    """Evaluate frozen simple gates; unrecognized or absent evidence fails closed."""
    checks: dict[str, bool] = {}
    for name, requirement in gates.items():
        value = metrics.get(name)
        if name.endswith("_gt"):
            checks[name] = isinstance(value, (int, float)) and value > requirement
        elif name.endswith("_gte"):
            checks[name] = isinstance(value, (int, float)) and value >= requirement
        elif name.endswith("_lte"):
            checks[name] = isinstance(value, (int, float)) and value <= requirement
        elif name in {
            "no_material_leakage",
            "exceptional_trade_gate",
            "regime_gate",
            "baseline_superiority",
        }:
            checks[name] = (
                value is True
                or (requirement == "pass" and value == "pass")
                or (name == "baseline_superiority" and value is True)
            )
        elif name in {
            "fold_count",
            "parameter_neighbor_count",
            "positive_folds_minimum",
            "positive_parameter_neighbors_minimum",
        }:
            checks[name] = isinstance(value, int) and value >= requirement
        else:
            checks[name] = False
    return checks
