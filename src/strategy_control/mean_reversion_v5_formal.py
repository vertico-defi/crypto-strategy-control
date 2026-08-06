"""Bounded full development run for the v5 clean-room evaluator."""
# The result serializer contains deliberately explicit long field mappings.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import random
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .mean_reversion_v5_cleanroom import ASSETS, TRIALS, CleanResult, evaluate
from .mean_reversion_v5_cleanroom_data import load_development_daily_sessions

FOLDS = (
    (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
)
BOOTSTRAP_SEED = 4480959964820476661


def _stats(values: tuple[float, ...]) -> dict[str, float]:
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    equity = 1.0
    peak = 1.0
    drawdown = 0.0
    for value in values:
        equity *= 1.0 + value
        peak = max(peak, equity)
        drawdown = max(drawdown, 1.0 - equity / peak)
    return {
        "net_return": equity - 1.0,
        "annualized_sharpe": mean / math.sqrt(variance) * math.sqrt(365.0)
        if variance > 0
        else 0.0,
        "maximum_drawdown": drawdown,
        "mean_interval_return": mean,
    }


def _bootstrap(values: tuple[float, ...], count: int = 2000) -> dict[str, float]:
    rng = random.Random(BOOTSTRAP_SEED)
    samples: list[float] = []
    for _ in range(count):
        equity = 1.0
        for _ in values:
            equity *= 1.0 + rng.choice(values)
        samples.append(equity - 1.0)
    samples.sort()
    return {
        "lower_95": samples[int(0.025 * (count - 1))],
        "median": samples[int(0.5 * (count - 1))],
        "upper_95": samples[int(0.975 * (count - 1))],
        "sample_count": float(count),
    }


def _pbo(trial_returns: dict[str, tuple[float, ...]]) -> float:
    names = tuple(trial_returns)
    length = len(next(iter(trial_returns.values())))
    blocks = [
        tuple(range(start, min(length, start + math.ceil(length / 8))))
        for start in range(0, length, math.ceil(length / 8))
    ][:8]
    if len(blocks) != 8:
        return 1.0
    overfit = 0
    total = 0
    from itertools import combinations

    for train_blocks in combinations(range(8), 4):
        train = set(train_blocks)
        train_indices = [index for block, values in enumerate(blocks) if block in train for index in values]
        test_indices = [index for block, values in enumerate(blocks) if block not in train for index in values]
        def sr(name: str, indices: list[int]) -> float:
            values = [trial_returns[name][index] for index in indices]
            mean = sum(values) / len(values)
            variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
            return mean / math.sqrt(variance) if variance > 0 else (math.inf if mean > 0 else -math.inf)
        selected = max(names, key=lambda name: sr(name, train_indices))
        ranks = sorted(sr(name, test_indices) for name in names)
        rank = sum(value <= sr(selected, test_indices) for value in ranks)
        overfit += int(rank <= len(names) // 2)
        total += 1
    return overfit / total if total else 1.0


def _trace_hash(result: CleanResult) -> str:
    payload = {
        "decisions": [item.__dict__ for item in result.decisions],
        "fills": [item.__dict__ for item in result.fills],
        "interval_returns": result.interval_returns,
        "costs": result.costs,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()


def run(source_repository: Path) -> dict[str, Any]:
    months = tuple(
        f"{year:04d}-{month:02d}"
        for year in (2024, 2025)
        for month in range(1, 13)
        if not (year == 2024 and month < 7)
    )
    started = time.monotonic()
    sessions, evidence = load_development_daily_sessions(
        source_repository, selected_months=months
    )
    results: dict[str, CleanResult] = {}
    fold_results: dict[str, list[dict[str, Any]]] = {}
    for trial in TRIALS:
        full = evaluate(
            tuple(item for item in sessions if item.timestamp < FOLDS[-1][1]),
            trial=trial,
            start_timestamp=FOLDS[0][0],
        )
        results[trial.name] = full
        fold_results[trial.name] = []
        for begin, end in FOLDS:
            fold = evaluate(
                tuple(item for item in sessions if item.timestamp < end),
                trial=trial,
                start_timestamp=begin,
            )
            fold_results[trial.name].append(_stats(fold.interval_returns) | {"fills": len(fold.fills)})
    primary = results[TRIALS[0].name]
    primary_stats = _stats(primary.interval_returns)
    double = evaluate(
        tuple(item for item in sessions if item.timestamp < FOLDS[-1][1]),
        cost_bps=28.0,
        start_timestamp=FOLDS[0][0],
    )
    delayed = evaluate(
        tuple(item for item in sessions if item.timestamp < FOLDS[-1][1]),
        delay_sessions=1,
        start_timestamp=FOLDS[0][0],
    )
    gross = evaluate(
        tuple(item for item in sessions if item.timestamp < FOLDS[-1][1]),
        cost_bps=0.0,
        start_timestamp=FOLDS[0][0],
    )
    standalone = {
        asset: evaluate(
            tuple(item for item in sessions if item.timestamp < FOLDS[-1][1]),
            start_timestamp=FOLDS[0][0],
            active_assets=(asset,),
        )
        for asset in ASSETS
    }
    trial_net = {name: result.net_return for name, result in results.items()}
    fold_net = [item["net_return"] for item in fold_results[TRIALS[0].name]]
    equal_weight_values = tuple(
        0.5 * (session.rows[ASSETS[0]].close / previous.rows[ASSETS[0]].close - 1.0)
        + 0.5 * (session.rows[ASSETS[1]].close / previous.rows[ASSETS[1]].close - 1.0)
        for previous, session in itertools.pairwise(sessions)
        if previous.complete and session.complete and session.timestamp >= FOLDS[0][0]
    )
    equal_weight = _stats(equal_weight_values)
    bootstrap = _bootstrap(primary.interval_returns)
    # The exact 49-slot historical registry is not present in this successor's
    # immutable inputs; the frozen degeneracy rule therefore returns zero.
    dsr_probability = 0.0
    pbo_probability = _pbo({name: result.interval_returns for name, result in results.items()})
    gates = {
        "aggregate_net_return_gt": primary.net_return > 0,
        "annualized_sharpe_gte": primary_stats["annualized_sharpe"] >= 0.75,
        "positive_folds_minimum": sum(value > 0 for value in fold_net) >= 3,
        "fold_count": len(fold_net) == 4,
        "maximum_drawdown_lte": primary_stats["maximum_drawdown"] <= 0.20,
        "doubled_cost_aggregate_net_return_gt": double.net_return > 0,
        "additional_delay_aggregate_net_return_gt": delayed.net_return > 0,
        "positive_parameter_neighbors_minimum": sum(value > 0 for value in trial_net.values()) - 1 >= 3,
        "parameter_neighbor_count": len(TRIALS) - 1 == 6,
        "asset_standalone_net_return_each_gt": all(item.net_return > 0 for item in standalone.values()),
        "completed_entries_total_minimum": len(primary.fills) // 2 >= 24,
        "completed_entries_each_asset_minimum": all(
            sum(fill.asset == asset and fill.target_weight == 0 for fill in primary.fills) >= 10
            for asset in ASSETS
        ),
        "bootstrap_mean_daily_net_return_lower_95_ci_gt": bootstrap["lower_95"] > 0,
        "deflated_sharpe_probability_gte": dsr_probability >= 0.95,
        "probability_of_backtest_overfitting_lte": pbo_probability <= 0.20,
        "regime_gate": all(value > 0 for value in fold_net[:2]) or all(value > 0 for value in fold_net[2:]),
        "exceptional_trade_gate": max(primary.interval_returns) < 0.25,
        "baseline_superiority": primary_stats["annualized_sharpe"] > equal_weight["annualized_sharpe"]
        and primary_stats["maximum_drawdown"] < equal_weight["maximum_drawdown"],
        "no_material_leakage": evidence["holdout_path_resolution_count"] == 0,
    }
    return {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-long-only-mean-reversion-v5-cleanroom-evaluation",
        "classification": "DEVELOPMENT_RESULT_PENDING_INDEPENDENT_AUDIT",
        "source_repository": "crypto-direction-lab (absolute local path omitted)",
        "dataset_id": evidence["dataset_id"],
        "development_allowlist_count": evidence["development_allowlist_count"],
        "selected_partition_count": evidence["selected_partition_count"],
        "row_counts": evidence["row_counts"],
        "daily_session_count": evidence["daily_session_count"],
        "complete_session_count": evidence["complete_session_count"],
        "incomplete_session_count": evidence["incomplete_session_count"],
        "holdout_path_resolution_count": evidence["holdout_path_resolution_count"],
        "primary": {
            "gross": _stats(gross.interval_returns),
            "net": primary_stats,
            "costs": primary.costs,
            "fills": len(primary.fills),
            "trace_hash": _trace_hash(primary),
        },
        "folds": fold_results[TRIALS[0].name],
        "trials": {name: {"net_return": result.net_return, "fills": len(result.fills)} for name, result in results.items()},
        "stresses": {"doubled_cost": _stats(double.interval_returns) | {"net_return_terminal": double.net_return}, "additional_delay": _stats(delayed.interval_returns) | {"net_return_terminal": delayed.net_return}},
        "standalone": {asset: _stats(result.interval_returns) | {"net_return_terminal": result.net_return} for asset, result in standalone.items()},
        "baseline_equal_weight": equal_weight,
        "bootstrap": bootstrap,
        "dsr_probability": dsr_probability,
        "pbo_probability": pbo_probability,
        "gates": gates,
        "all_development_gates_pass": all(gates.values()),
        "formal_result_exists": True,
        "holdout_accessed": False,
        "capital_permitted": 0,
        "gpu_seconds_used": 0,
        "runtime_seconds": time.monotonic() - started,
        "max_rss_kb": __import__("resource").getrusage(__import__("resource").RUSAGE_SELF).ru_maxrss,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    payload = run(arguments.source_repository)
    arguments.output.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"classification": payload["classification"], "net_return": payload["primary"]["net"]["net_return"], "gates_pass": payload["all_development_gates_pass"], "runtime_seconds": payload["runtime_seconds"], "max_rss_kb": payload["max_rss_kb"]}, sort_keys=True))


if __name__ == "__main__":
    main()
