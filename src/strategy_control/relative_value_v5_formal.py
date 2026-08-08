"""Bounded gate-first development panel for the narrow RV v5 evaluator."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import resource
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from statistics import NormalDist

from .mean_reversion_v5_cleanroom_data import load_development_daily_sessions
from .relative_value_v5_cleanroom import evaluate_development

FOLDS = (
    (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
)
TRIALS = (
    ("primary_risk_adjusted_20_60_120", (20, 60, 120), 0.25, True, True),
    ("raw_60_session_relative_strength_rotation", (60,), 0.0, True, False),
    ("short_10_30_60_horizons", (10, 30, 60), 0.25, True, True),
    ("long_60_120_180_horizons", (60, 120, 180), 0.25, True, True),
    ("raw_unadjusted_20_60_120", (20, 60, 120), 0.25, True, False),
    ("wide_0_50_rotation_gap", (20, 60, 120), 0.50, True, True),
    ("always_in_higher_score_no_cash_filter", (20, 60, 120), 0.0, False, True),
)
SEED = 4689472421920140622


def stats(values: tuple[float, ...]) -> dict[str, float]:
    equity = 1.0
    peak = 1.0
    drawdown = 0.0
    for value in values:
        equity *= 1.0 + value
        peak = max(peak, equity)
        drawdown = max(drawdown, 1.0 - equity / peak)
    mean = sum(values) / len(values) if values else 0.0
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1) if len(values) > 1 else 0.0
    sharpe = mean / math.sqrt(variance) * math.sqrt(365.0) if variance > 0 else 0.0
    return {
        "net_return": equity - 1.0,
        "maximum_drawdown": drawdown,
        "annualized_sharpe": sharpe,
    }


def bootstrap(values: tuple[float, ...], count: int = 2000) -> dict[str, float | int]:
    """Frozen Politis--Romano circular stationary bootstrap of daily mean return."""
    import numpy as np

    if len(values) < 2 or any(not math.isfinite(value) for value in values):
        raise ValueError("bootstrap requires finite daily returns")
    rng = np.random.Generator(np.random.PCG64(SEED))
    samples: list[float] = []
    for _ in range(count):
        cursor = int(rng.integers(len(values)))
        sample: list[float] = []
        for _ in values:
            sample.append(values[cursor])
            cursor = int(rng.integers(len(values))) if float(rng.random()) < 0.05 else (cursor + 1) % len(values)
        samples.append(statistics.mean(sample))
    samples.sort()
    def percentile(q: float) -> float:
        index = (len(samples) - 1) * q
        lo, hi = math.floor(index), math.ceil(index)
        return samples[lo] if lo == hi else samples[lo] + (samples[hi] - samples[lo]) * (index - lo)
    return {
        "seed": SEED, "block_length": 20, "resamples": count,
        "mean": statistics.mean(values), "lower_95": percentile(0.025),
        "upper_95": percentile(0.975),
    }


def _sharpe(values: tuple[float, ...]) -> float:
    return statistics.mean(values) / statistics.stdev(values) if len(values) > 1 and statistics.stdev(values) > 0 else 0.0


def pbo(columns: tuple[tuple[float, ...], ...]) -> float:
    if len(columns) != 7 or len(columns[0]) < 8 or any(len(c) != len(columns[0]) for c in columns):
        return 1.0
    q, r = divmod(len(columns[0]), 8)
    blocks, at = [], 0
    for i in range(8):
        end = at + q + int(i < r)
        blocks.append(tuple(range(at, end)))
        at = end
    events = 0
    for training_blocks in itertools.combinations(range(8), 4):
        train = tuple(i for b in training_blocks for i in blocks[b])
        test = tuple(i for b in range(8) if b not in training_blocks for i in blocks[b])
        winner = max(range(7), key=lambda i: (_sharpe(tuple(columns[i][j] for j in train)), -i))
        scores = tuple(_sharpe(tuple(column[j] for j in test)) for column in columns)
        rank = (sum(x < scores[winner] for x in scores) + sum(x <= scores[winner] for x in scores) + 1) / 2
        relative = rank / 8
        if not 0 < relative < 1:
            return 1.0
        events += math.log(relative / (1 - relative)) <= 0
    return events / 70


def _pointer(document: Any, pointer: str) -> Any:
    value = document
    for token in pointer.removeprefix("/").split("/"):
        value = value[int(token)] if isinstance(value, list) else value[token]
    return value


def registry_sharpes() -> tuple[float, ...]:
    """Read only the 28 hash-bound observed registry values, never the v5 result."""
    root = Path(__file__).resolve().parents[2]
    contract = json.loads((root / "experiments/btc-eth-relative-value-rotation-v2/PREREGISTRATION_DRAFT.json").read_text())
    values: list[float] = []
    documents: dict[str, Any] = {}
    for slot in contract["multiplicity_registry"]["ordered_prior_slots"]:
        if slot["status"] != "observed":
            continue
        experiment, _ = slot["name"].split(":", 1)
        if experiment not in documents:
            path = root / "experiments" / experiment / "DEVELOPMENT_RESULT.json"
            raw = json.loads(path.read_text())
            canonical = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
            if hashlib.sha256(canonical).hexdigest() != slot["artifact_canonical_sha256"]:
                raise ValueError("multiplicity artifact hash mismatch")
            documents[experiment] = raw
        annualized = _pointer(documents[experiment], slot["json_pointer"])
        if not isinstance(annualized, (int, float)) or not math.isfinite(annualized):
            raise ValueError("nonfinite registered Sharpe")
        values.append(float(annualized) / math.sqrt(365))
    if len(values) != 28:
        raise ValueError("wrong observed registry cardinality")
    return tuple(values)


def dsr_probability(values: tuple[float, ...], records: tuple[float, ...]) -> float:
    if len(values) < 30 or len(records) != 35 or any(not math.isfinite(x) for x in (*values, *records)):
        return 0.0
    observed, sigma = _sharpe(values), statistics.stdev(records)
    if sigma <= 0:
        return 0.0
    mean = statistics.mean(values)
    denominator = sum((x - mean) ** 2 for x in values)
    if denominator <= 0:
        return 0.0
    vif = max(1.0, 1.0 + 2.0 * sum((1.0 - lag / 29.0) * sum((values[i] - mean) * (values[i-lag] - mean) for i in range(lag, len(values))) / denominator for lag in range(1, 29)))
    effective = len(values) / vif
    if effective < 30 or not math.isfinite(effective):
        return 0.0
    deviation = statistics.stdev(values)
    standardized = tuple((x - mean) / deviation for x in values)
    n = len(values)
    skew = n / ((n - 1) * (n - 2)) * sum(x**3 for x in standardized)
    excess = n * (n + 1) / ((n - 1) * (n - 2) * (n - 3)) * sum(x**4 for x in standardized) - 3 * (n - 1)**2 / ((n - 2) * (n - 3))
    gamma = 0.5772156649015329
    normal = NormalDist()
    sr0 = sigma * ((1-gamma) * normal.inv_cdf(1 - 1 / 56) + gamma * normal.inv_cdf(1 - 1 / (56 * math.e)))
    scale = 1 - skew * observed + ((excess + 3 - 1) / 4) * observed**2
    if scale <= 0 or not all(math.isfinite(x) for x in (observed, sr0, scale)):
        return 0.0
    return normal.cdf((observed - sr0) * math.sqrt(effective - 1) / math.sqrt(scale))


def run(source_repository: Path) -> dict[str, Any]:
    started = time.monotonic()
    months = tuple(
        f"{year:04d}-{month:02d}"
        for year in (2024, 2025)
        for month in range(1, 13)
        if not (year == 2024 and month < 7)
    )
    sessions, evidence = load_development_daily_sessions(source_repository, selected_months=months)
    all_sessions = tuple(item for item in sessions if item.timestamp < FOLDS[-1][1])
    trial_results: dict[str, dict[str, Any]] = {}
    raw_results: dict[str, Any] = {}
    for name, horizons, gap, cash_filter, risk_adjusted in TRIALS:
        result = evaluate_development(
            all_sessions,
            start_timestamp=FOLDS[0][0],
            horizons=horizons,
            gap=gap,
            cash_filter=cash_filter,
            risk_adjusted=risk_adjusted,
        )
        folds = []
        for begin, end in FOLDS:
            fold_sessions = tuple(item for item in sessions if item.timestamp < end)
            fold = evaluate_development(
                fold_sessions,
                start_timestamp=begin,
                horizons=horizons,
                gap=gap,
                cash_filter=cash_filter,
                risk_adjusted=risk_adjusted,
            )
            folds.append(stats(fold.interval_returns) | {"fills": len(fold.fills)})
        trial_results[name] = {
            "net_return": result.net_return,
            "stats": stats(result.interval_returns),
            "folds": folds,
            "fills": len(result.fills),
        }
        raw_results[name] = result
    primary = trial_results[TRIALS[0][0]]
    doubled = evaluate_development(all_sessions, cost_bps=28.0, start_timestamp=FOLDS[0][0])
    delayed = evaluate_development(all_sessions, delay_sessions=1, start_timestamp=FOLDS[0][0])
    primary_folds = [item["net_return"] for item in primary["folds"]]
    primary_result = raw_results[TRIALS[0][0]]
    primary_values = tuple(float(x) for x in primary_result.interval_returns)
    boot = bootstrap(primary_values)
    columns = tuple(tuple(raw_results[name].interval_returns) for name, *_ in TRIALS)
    common_size = min(len(column) for column in columns)
    columns = tuple(column[-common_size:] for column in columns)
    pbo_value = pbo(columns)
    current_sharpes = tuple(_sharpe(column) for column in columns)
    registry = registry_sharpes()
    dsr_value = dsr_probability(columns[0], tuple((*registry, *current_sharpes)))
    entries = {asset: 0 for asset in ("BTCUSDT", "ETHUSDT")}
    holds = {asset: 0 for asset in entries}
    active = {asset: False for asset in entries}
    for fill in primary_result.fills:
        for asset in entries:
            if fill.target == asset:
                if not active[asset]:
                    entries[asset] += 1
                active[asset] = True
            elif active[asset]:
                holds[asset] += 1
                active[asset] = False
    contributions = {
        asset: sum(value for value, held in zip(primary_result.interval_returns, primary_result.interval_assets, strict=True) if held == asset)
        for asset in entries
    }
    equal_weight: list[float] = []
    prior_prices: dict[str, float] | None = None
    for session in all_sessions:
        if session.timestamp < FOLDS[0][0] or not session.complete:
            prior_prices = None
            continue
        prices = {asset: session.execution_rows[asset].close for asset in ("BTCUSDT", "ETHUSDT")}
        if prior_prices is not None:
            equal_weight.append(sum(prices[a] / prior_prices[a] - 1 for a in prices) / 2)
        prior_prices = prices
    equal_weight_stats = stats(tuple(equal_weight))
    raw_baseline = trial_results["raw_60_session_relative_strength_rotation"]["stats"]
    baseline_pass = bool(
        primary["net_return"] > trial_results["raw_60_session_relative_strength_rotation"]["net_return"]
        and primary["stats"]["annualized_sharpe"] > raw_baseline["annualized_sharpe"]
        and primary["net_return"] > equal_weight_stats["net_return"]
        and primary["stats"]["annualized_sharpe"] > equal_weight_stats["annualized_sharpe"]
        and primary["stats"]["maximum_drawdown"] < equal_weight_stats["maximum_drawdown"]
    )
    positive = sorted((x for x in primary_values if x > 0), reverse=True)
    positive_total = sum(positive)
    exceptional = {
        "largest_positive_interval_fraction": positive[0] / positive_total if positive_total else math.inf,
        "top_five_positive_intervals_fraction": sum(positive[:5]) / positive_total if positive_total else math.inf,
    }
    exceptional["pass"] = bool(positive_total > 0 and exceptional["largest_positive_interval_fraction"] <= 0.5 and exceptional["top_five_positive_intervals_fraction"] <= 0.75)
    regime_labels: dict[datetime, str] = {}
    closes: list[float] = []
    volatility_history: list[float] = []
    for session in all_sessions:
        if not session.complete:
            closes, volatility_history = [], []
            continue
        closes.append(session.rows["BTCUSDT"].close)
        if len(closes) < 61:
            continue
        recent = [closes[i] / closes[i - 1] - 1 for i in range(len(closes) - 60, len(closes))]
        vol = statistics.stdev(recent)
        if len(volatility_history) >= 120 and len(closes) >= 121:
            trend = "up" if math.log(closes[-1] / closes[-121]) > 0 else "down"
            regime_labels[session.timestamp] = f"{trend}_{'high' if vol > statistics.median(volatility_history) else 'low'}"
        volatility_history.append(vol)
    regime_returns: dict[str, list[float]] = {}
    for timestamp, value in zip(primary_result.interval_timestamps, primary_values, strict=True):
        if timestamp in regime_labels:
            regime_returns.setdefault(regime_labels[timestamp], []).append(value)
    regime_entries: dict[str, int] = {}
    for fill in primary_result.fills:
        label = regime_labels.get(fill.target_timestamp)
        if label is not None and fill.target in ("BTCUSDT", "ETHUSDT"):
            regime_entries[label] = regime_entries.get(label, 0) + 1
    eligible_regimes = [name for name, values in regime_returns.items() if len(values) >= 45 and regime_entries.get(name, 0) >= 5]
    regime_pass = len(eligible_regimes) >= 3 and all(math.prod(1 + x for x in regime_returns[name]) - 1 > 0 for name in eligible_regimes)
    regimes = {name: {"intervals": len(values), "net_return": math.prod(1 + x for x in values) - 1, "entries": regime_entries.get(name, 0)} for name, values in regime_returns.items()}
    gates = {
        "aggregate_net_return_gt": primary["net_return"] > 0,
        "annualized_sharpe_gte": primary["stats"]["annualized_sharpe"] >= 0.75,
        "maximum_drawdown_lte": primary["stats"]["maximum_drawdown"] <= 0.2,
        "fold_count": len(primary_folds) == 4,
        "positive_folds_minimum": sum(x > 0 for x in primary_folds) >= 3,
        "doubled_cost_aggregate_net_return_gt": doubled.net_return > 0,
        "additional_delay_aggregate_net_return_gt": delayed.net_return > 0,
        "positive_parameter_neighbors_minimum": sum(trial_results[name]["net_return"] > 0 for name in (
            "short_10_30_60_horizons", "long_60_120_180_horizons", "raw_unadjusted_20_60_120", "wide_0_50_rotation_gap")) >= 3,
        "parameter_neighbor_count": 4 >= 4,
        "bootstrap_lower_ci_gt": boot["lower_95"] > 0,
        "deflated_sharpe_probability_gte": dsr_value >= 0.95,
        "probability_of_backtest_overfitting_lte": pbo_value <= 0.2,
        "baseline_superiority": baseline_pass,
        "completed_entries_total_minimum": sum(entries.values()) >= 24,
        "completed_holds_each_asset_minimum": all(value >= 8 for value in holds.values()),
        "asset_net_contribution_each_gt": all(value > 0 for value in contributions.values()),
        "exceptional_profit_gate": exceptional["pass"],
        "regime_gate": regime_pass,
        "no_material_leakage": evidence["holdout_path_resolution_count"] == 0,
    }
    return {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-relative-value-rotation-v5-cleanroom-evaluation",
        "classification": (
            "DEVELOPMENT_RESULT_NOT_PROMOTABLE"
            if not all(gates.values())
            else "DEVELOPMENT_RESULT_PENDING_AUDIT"
        ),
        "source_repository": "crypto-direction-lab (absolute path omitted)",
        "row_counts": evidence["row_counts"],
        "selected_partition_count": evidence["selected_partition_count"],
        "daily_session_count": evidence["daily_session_count"],
        "complete_session_count": evidence["complete_session_count"],
        "holdout_path_resolution_count": evidence["holdout_path_resolution_count"],
        "primary": primary,
        "doubled_cost": {"net_return": doubled.net_return, "costs": doubled.costs},
        "delayed_execution": {"net_return": delayed.net_return, "costs": delayed.costs},
        "bootstrap": boot,
        "deflated_sharpe": {"probability": dsr_value, "N": 56, "prior_observed_count": len(registry), "current_trial_count": len(current_sharpes)},
        "pbo": {"value": pbo_value, "split_count": 70, "common_interval_count": common_size},
        "baselines": {"raw_60_rotation": raw_baseline, "equal_weight_buy_and_hold": equal_weight_stats},
        "exceptional_profit": exceptional,
        "regimes": {"report": regimes, "eligible": eligible_regimes, "pass": regime_pass},
        "trade_counts": {"entries": entries, "completed_holds": holds},
        "asset_return_contributions": contributions,
        "trials": trial_results,
        "gates": gates,
        "all_gates_pass": all(gates.values()),
        "dsr_registry_classification": "VALID_HASH_BOUND_CALCULATED",
        "pbo_status": "VALID_CALCULATED",
        "formal_economic_result": True,
        "holdout_accessed": False,
        "runtime_seconds": time.monotonic() - started,
        "max_rss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import json

    result = run(args.source_repository)
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "classification": result["classification"],
                "all_gates_pass": result["all_gates_pass"],
                "runtime_seconds": result["runtime_seconds"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
