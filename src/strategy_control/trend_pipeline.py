"""Development-only loader and evaluator for the frozen BTC/ETH trend experiment."""

from __future__ import annotations

import importlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from strategy_control.trend import (
    DailyBar,
    Fill,
    IntervalResult,
    TrendError,
    aggregate_return,
    buy_and_hold,
    cscv_pbo,
    daily_sharpe,
    deflated_sharpe_probability,
    delayed_fills,
    evaluate_gates,
    exceptional_trade_concentration,
    maximum_drawdown,
    primary_exposure,
    regime_labels,
    self_financing,
    stationary_bootstrap,
)

SYMBOLS = ("BTCUSDT", "ETHUSDT")
DEVELOPMENT_END = datetime(2026, 1, 1, tzinfo=UTC)
DEVELOPMENT_FOLDS = (
    (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), DEVELOPMENT_END),
)
AGGREGATE_START = datetime(2025, 1, 1, tzinfo=UTC)
BASE_COST_BPS = 14.0
DOUBLED_COST_BPS = 28.0


@dataclass(frozen=True)
class DevelopmentMarket:
    days: Mapping[str, Sequence[DailyBar]]
    causal_fills: Mapping[str, Mapping[datetime, tuple[datetime, float]]]
    source_partition_count: int
    holdout_values_read: bool = False


def _development_partitions(data_contract: Mapping[str, Any]) -> list[str]:
    partitions = data_contract.get("partitions")
    if not isinstance(partitions, list):
        raise TrendError("data-contract partitions missing")
    selected: list[str] = []
    for item in partitions:
        if not isinstance(item, dict):
            raise TrendError("malformed data-contract partition")
        scope = item.get("verification_scope")
        relative = item.get("relative_path")
        if scope == "BYTE_HASH_ONLY_NO_PARQUET_PARSE":
            continue
        if scope != "HASH_AND_SCHEMA_METADATA_ONLY" or not isinstance(relative, str):
            raise TrendError("unexpected data-contract partition scope")
        if "year=2026" in relative:
            raise TrendError("development loader refused a holdout partition")
        selected.append(relative)
    if len(selected) != 36:
        raise TrendError(f"expected 36 development partitions, observed {len(selected)}")
    return selected


def load_development_market(
    source_repository: Path, data_contract: Mapping[str, Any]
) -> DevelopmentMarket:
    """Load only allowlisted 2024-2025 columns; never open a 2026 Parquet file."""

    pandas = importlib.import_module("pandas")
    numpy = importlib.import_module("numpy")
    parquet = importlib.import_module("pyarrow.parquet")
    relative_paths = _development_partitions(data_contract)
    days_by_symbol: dict[str, list[DailyBar]] = {}
    fills_by_symbol: dict[str, dict[datetime, tuple[datetime, float]]] = {}
    dataset_root = source_repository / "data/real/historical-v2-pathc-20260723T175155Z"
    for symbol in SYMBOLS:
        symbol_paths = [relative for relative in relative_paths if f"symbol={symbol}/" in relative]
        if len(symbol_paths) != 18:
            raise TrendError(f"expected 18 development partitions for {symbol}")
        frames: list[Any] = []
        for relative in sorted(symbol_paths):
            if "year=2026" in relative:
                raise TrendError("holdout path reached development reader")
            path = dataset_root / relative
            table = parquet.ParquetFile(path).read(
                columns=(
                    "event_timestamp",
                    "available_timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                )
            )
            frames.append(table.to_pandas())
        frame = pandas.concat(frames, ignore_index=True).sort_values("event_timestamp")
        frame["event_timestamp"] = pandas.to_datetime(frame["event_timestamp"], utc=True)
        frame["available_timestamp"] = pandas.to_datetime(frame["available_timestamp"], utc=True)
        if frame["event_timestamp"].duplicated().any():
            raise TrendError(f"duplicate development timestamp: {symbol}")
        if frame["event_timestamp"].max().to_pydatetime() > DEVELOPMENT_END:
            raise TrendError("development value boundary crossed")
        if (frame["available_timestamp"] < frame["event_timestamp"]).any():
            raise TrendError(f"availability precedes event timestamp: {symbol}")
        numeric_columns = ("open", "high", "low", "close")
        if not bool(numpy.isfinite(frame[list(numeric_columns)].to_numpy()).all()):
            raise TrendError(f"non-finite development OHLC: {symbol}")
        if bool(
            (frame["low"] > frame[["open", "close"]].min(axis=1)).any()
            or (frame["high"] < frame[["open", "close"]].max(axis=1)).any()
        ):
            raise TrendError(f"invalid development OHLC: {symbol}")
        frame["session"] = (frame["event_timestamp"] - pandas.Timedelta(nanoseconds=1)).dt.floor(
            "D"
        )
        daily: list[DailyBar] = []
        for session_value, group in frame.groupby("session", sort=True):
            session = session_value.to_pydatetime()
            first_end = group["event_timestamp"].iloc[0].to_pydatetime()
            last_end = group["event_timestamp"].iloc[-1].to_pydatetime()
            complete = bool(
                len(group) == 1440
                and group["event_timestamp"].nunique() == 1440
                and first_end == session + timedelta(minutes=1)
                and last_end == session + timedelta(days=1)
                and group["event_timestamp"]
                .diff()
                .dropna()
                .eq(pandas.Timedelta(minutes=1))
                .all()
            )
            daily.append(
                DailyBar(
                    session=session,
                    available_at=group["available_timestamp"].max().to_pydatetime(),
                    open=float(group["open"].iloc[0]),
                    high=float(group["high"].max()),
                    low=float(group["low"].min()),
                    close=float(group["close"].iloc[-1]),
                    complete=complete,
                )
            )
        event_index = pandas.DatetimeIndex(frame["event_timestamp"])
        causal: dict[datetime, tuple[datetime, float]] = {}
        for day in daily:
            cutoff = max(day.session + timedelta(days=1), day.available_at)
            candidate = int(event_index.searchsorted(cutoff + timedelta(minutes=1), side="right"))
            while candidate < len(frame):
                event_end = frame["event_timestamp"].iloc[candidate].to_pydatetime()
                available = frame["available_timestamp"].iloc[candidate].to_pydatetime()
                open_time = event_end - timedelta(minutes=1)
                if open_time > cutoff and available >= event_end:
                    causal[day.session] = (open_time, float(frame["open"].iloc[candidate]))
                    break
                candidate += 1
        days_by_symbol[symbol] = daily
        fills_by_symbol[symbol] = causal
    if [day.session for day in days_by_symbol[SYMBOLS[0]]] != [
        day.session for day in days_by_symbol[SYMBOLS[1]]
    ]:
        raise TrendError("BTC and ETH development sessions are not aligned")
    return DevelopmentMarket(
        days=days_by_symbol,
        causal_fills=fills_by_symbol,
        source_partition_count=len(relative_paths),
        holdout_values_read=False,
    )


def build_period_fills(
    market: DevelopmentMarket,
    targets: Mapping[str, Sequence[float | None]],
    start: datetime,
    end: datetime,
) -> list[Fill]:
    """Build aligned fills inside a half-open period and force terminal cash."""

    if market.holdout_values_read or end > DEVELOPMENT_END or end <= start:
        raise TrendError("development fill boundary violation")
    symbols = tuple(sorted(targets))
    if not symbols or any(symbol not in market.days for symbol in symbols):
        raise TrendError("invalid target symbols")
    sessions = [day.session for day in market.days[symbols[0]]]
    if any(len(targets[symbol]) != len(sessions) for symbol in symbols):
        raise TrendError("target length mismatch")
    fills: list[Fill] = []
    started = False
    for index, session in enumerate(sessions):
        entries = [market.causal_fills[symbol].get(session) for symbol in symbols]
        values = [targets[symbol][index] for symbol in symbols]
        if any(entry is None for entry in entries) or any(value is None for value in values):
            if started and start <= session < end:
                later_eligible = False
                for later in range(index + 1, len(sessions)):
                    later_entries = [
                        market.causal_fills[symbol].get(sessions[later]) for symbol in symbols
                    ]
                    later_values = [targets[symbol][later] for symbol in symbols]
                    if any(entry is None for entry in later_entries) or any(
                        value is None for value in later_values
                    ):
                        continue
                    resolved_later = [entry for entry in later_entries if entry is not None]
                    if resolved_later and start <= resolved_later[0][0] < end:
                        later_eligible = True
                        break
                if later_eligible:
                    raise TrendError(
                        "missing or quarantined fill inside an active evaluation period"
                    )
                break
            continue
        resolved = [entry for entry in entries if entry is not None]
        timestamps = {entry[0] for entry in resolved}
        if len(timestamps) != 1:
            raise TrendError("unaligned causal fill timestamps")
        timestamp = resolved[0][0]
        if timestamp < start or timestamp >= end:
            continue
        started = True
        target_values: dict[str, float] = {}
        for position, symbol in enumerate(symbols):
            value = values[position]
            if value is None:
                raise TrendError("target unexpectedly absent after eligibility check")
            target_values[symbol] = float(value)
        fills.append(
            Fill(
                timestamp,
                {symbol: resolved[position][1] for position, symbol in enumerate(symbols)},
                target_values,
            )
        )
    if len(fills) < 2:
        raise TrendError("insufficient causal fills for evaluation")
    last = fills[-1]
    fills[-1] = Fill(last.timestamp, last.prices, {symbol: 0.0 for symbol in symbols})
    return fills


def _returns(intervals: Sequence[IntervalResult]) -> list[float]:
    return [interval.net_return for interval in intervals]


def _period_summary(intervals: Sequence[IntervalResult]) -> dict[str, Any]:
    values = _returns(intervals)
    if not values:
        raise TrendError("empty period result")
    sharpe = daily_sharpe(values)
    return {
        "intervals": len(values),
        "net_return": aggregate_return(values),
        "annualized_sharpe": sharpe * math.sqrt(365) if math.isfinite(sharpe) else None,
        "maximum_drawdown": maximum_drawdown(values),
        "turnover": sum(interval.turnover for interval in intervals),
        "cost": sum(interval.cost for interval in intervals),
    }


def _target_set(
    market: DevelopmentMarket,
    *,
    lookbacks: Sequence[int] = (20, 60, 120),
    target_vol: float = 0.15,
    mode: str = "combined",
    asset_weight: float = 0.5,
) -> dict[str, list[float | None]]:
    return {
        symbol: primary_exposure(
            market.days[symbol],
            asset_weight=asset_weight,
            target_vol=target_vol,
            lookbacks=lookbacks,
            mode=mode,
        )
        for symbol in SYMBOLS
    }


def evaluate_development(
    market: DevelopmentMarket,
    preregistration: Mapping[str, Any],
    *,
    bootstrap_rng: Any | None = None,
) -> dict[str, Any]:
    """Evaluate only the four frozen 2025 development folds and aggregate period."""

    if market.holdout_values_read:
        raise TrendError("holdout values were read")
    variants = {
        "primary_combined": _target_set(market),
        "donchian_only": _target_set(market, mode="donchian"),
        "time_series_momentum_only": _target_set(market, mode="momentum"),
        "shorter_horizons": _target_set(market, lookbacks=(15, 45, 90)),
        "longer_horizons": _target_set(market, lookbacks=(25, 75, 150)),
        "lower_volatility_target": _target_set(market, target_vol=0.10),
        "higher_volatility_target": _target_set(market, target_vol=0.20),
    }
    fills_by_variant = {
        name: build_period_fills(market, targets, AGGREGATE_START, DEVELOPMENT_END)
        for name, targets in variants.items()
    }
    intervals_by_variant = {
        name: self_financing(fills, BASE_COST_BPS) for name, fills in fills_by_variant.items()
    }
    variant_summaries = {
        name: _period_summary(intervals) for name, intervals in intervals_by_variant.items()
    }
    primary_fills = fills_by_variant["primary_combined"]
    primary_intervals = intervals_by_variant["primary_combined"]
    primary = variant_summaries["primary_combined"]
    doubled = _period_summary(self_financing(primary_fills, DOUBLED_COST_BPS))
    delayed = _period_summary(self_financing(delayed_fills(primary_fills), BASE_COST_BPS))

    fold_reports: list[dict[str, Any]] = []
    for start, end in DEVELOPMENT_FOLDS:
        fold_fills = build_period_fills(market, variants["primary_combined"], start, end)
        summary = _period_summary(self_financing(fold_fills, BASE_COST_BPS))
        summary.update({"start_utc": start.isoformat(), "end_exclusive_utc": end.isoformat()})
        fold_reports.append(summary)
    eligible_folds = [fold for fold in fold_reports if fold["intervals"] >= 60]
    positive_folds = sum(fold["net_return"] > 0 for fold in eligible_folds)

    standalone: dict[str, dict[str, Any]] = {}
    for symbol in SYMBOLS:
        target = {
            symbol: primary_exposure(
                market.days[symbol], asset_weight=1.0, target_vol=0.15
            )
        }
        fills = build_period_fills(market, target, AGGREGATE_START, DEVELOPMENT_END)
        standalone[symbol] = _period_summary(self_financing(fills, BASE_COST_BPS))

    benchmark_fills = primary_fills
    benchmarks = {
        "cash_zero_return": {
            "intervals": len(primary_intervals),
            "net_return": 0.0,
            "annualized_sharpe": None,
            "maximum_drawdown": 0.0,
        },
        "BTCUSDT_buy_and_hold": _period_summary(
            buy_and_hold(benchmark_fills, {"BTCUSDT": 1.0, "ETHUSDT": 0.0})
        ),
        "ETHUSDT_buy_and_hold": _period_summary(
            buy_and_hold(benchmark_fills, {"BTCUSDT": 0.0, "ETHUSDT": 1.0})
        ),
        "equal_weight_BTC_ETH_buy_and_hold": _period_summary(
            buy_and_hold(benchmark_fills, {"BTCUSDT": 0.5, "ETHUSDT": 0.5})
        ),
    }
    equal_weight = benchmarks["equal_weight_BTC_ETH_buy_and_hold"]
    baseline_superiority = bool(
        primary["annualized_sharpe"] is not None
        and equal_weight["annualized_sharpe"] is not None
        and primary["annualized_sharpe"] > equal_weight["annualized_sharpe"]
        and primary["maximum_drawdown"] < equal_weight["maximum_drawdown"]
    )

    alternative_returns = [_returns(intervals_by_variant[name]) for name in variants]
    if len({len(values) for values in alternative_returns}) != 1:
        raise TrendError("multiplicity alternatives are not aligned")
    bootstrap = stationary_bootstrap(_returns(primary_intervals), rng=bootstrap_rng)
    dsr = deflated_sharpe_probability(_returns(primary_intervals), alternative_returns)
    pbo = cscv_pbo(alternative_returns)
    exceptional = exceptional_trade_concentration(primary_intervals)

    btc_regimes = regime_labels(market.days["BTCUSDT"])
    regime_by_fill: dict[datetime, str] = {}
    for index, day in enumerate(market.days["BTCUSDT"]):
        entry = market.causal_fills["BTCUSDT"].get(day.session)
        label = btc_regimes[index]
        if entry is not None and label is not None:
            regime_by_fill[entry[0]] = label
    regime_returns: dict[str, list[float]] = {}
    for interval in primary_intervals:
        label = regime_by_fill.get(interval.start)
        if label is not None:
            regime_returns.setdefault(label, []).append(interval.net_return)
    regime_report = {
        label: {"intervals": len(values), "net_return": aggregate_return(values)}
        for label, values in sorted(regime_returns.items())
    }
    eligible_regimes = [item for item in regime_report.values() if item["intervals"] >= 45]
    regime_pass = len(eligible_regimes) >= 3 and all(
        item["net_return"] > 0 and item["net_return"] >= -0.05 for item in eligible_regimes
    )

    neighbor_names = (
        "shorter_horizons",
        "longer_horizons",
        "lower_volatility_target",
        "higher_volatility_target",
    )
    metrics = {
        "aggregate_net_return_gt": primary["net_return"],
        "positive_folds_minimum": positive_folds,
        "fold_count": len(eligible_folds),
        "annualized_sharpe_gte": primary["annualized_sharpe"],
        "maximum_drawdown_lte": primary["maximum_drawdown"],
        "doubled_cost_aggregate_net_return_gt": doubled["net_return"],
        "additional_delay_aggregate_net_return_gt": delayed["net_return"],
        "asset_standalone_net_return_each_gt": min(
            standalone[symbol]["net_return"] for symbol in SYMBOLS
        ),
        "positive_parameter_neighbors_minimum": sum(
            variant_summaries[name]["net_return"] > 0 for name in neighbor_names
        ),
        "parameter_neighbor_count": len(neighbor_names),
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": bootstrap["lower_95"],
        "deflated_sharpe_probability_gte": dsr,
        "probability_of_backtest_overfitting_lte": pbo,
        "baseline_superiority": baseline_superiority,
        "regime_gate": "pass" if regime_pass else "fail",
        "exceptional_trade_gate": "pass" if exceptional["pass"] else "fail",
        "no_material_leakage": True,
    }
    gates = preregistration.get("development_gates_all_required")
    if not isinstance(gates, dict):
        raise TrendError("development gates missing from preregistration")
    checks = evaluate_gates(metrics, gates)
    all_pass = bool(checks) and all(checks.values())
    return {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-vol-targeted-trend-v1",
        "stage": "DEVELOPMENT",
        "classification": "DEVELOPMENT_GO" if all_pass else "HISTORICAL_NO_GO",
        "all_development_gates_pass": all_pass,
        "metrics": metrics,
        "gate_checks": checks,
        "folds": fold_reports,
        "variants": variant_summaries,
        "benchmarks": benchmarks,
        "doubled_cost": doubled,
        "additional_delay": delayed,
        "standalone_assets": standalone,
        "bootstrap": bootstrap,
        "deflated_sharpe_probability": dsr,
        "probability_of_backtest_overfitting": pbo,
        "regimes": regime_report,
        "exceptional_trade_concentration": exceptional,
        "source_partition_count": market.source_partition_count,
        "holdout_values_read": False,
        "holdout_opened": False,
        "capital_permitted": 0,
    }
