"""Bounded gate-first development panel for the narrow RV v5 evaluator."""

from __future__ import annotations

import argparse
import math
import random
import resource
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .mean_reversion_v5_cleanroom_data import load_development_daily_sessions
from .relative_value_v5_cleanroom import evaluate_development

FOLDS = (
    (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC)),
    (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
    (datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC)),
    (datetime(2025, 10, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
)
TRIALS = (
    ("primary_risk_adjusted_20_60_120", (20, 60, 120), 0.25, True),
    ("raw_60_session_relative_strength_rotation", (60,), 0.0, False),
    ("short_10_30_60_horizons", (10, 30, 60), 0.25, True),
    ("long_60_120_180_horizons", (60, 120, 180), 0.25, True),
    ("raw_unadjusted_20_60_120", (20, 60, 120), 0.25, False),
    ("wide_0_50_rotation_gap", (20, 60, 120), 0.50, True),
    ("always_in_higher_score_no_cash_filter", (20, 60, 120), 0.25, False),
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


def bootstrap(values: tuple[float, ...], count: int = 2000) -> dict[str, float]:
    rng = random.Random(SEED)
    samples = []
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
        "count": float(count),
    }


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
    for name, horizons, gap, cash_filter in TRIALS:
        result = evaluate_development(
            all_sessions,
            start_timestamp=FOLDS[0][0],
            horizons=horizons,
            gap=gap,
            cash_filter=cash_filter,
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
            )
            folds.append(stats(fold.interval_returns) | {"fills": len(fold.fills)})
        trial_results[name] = {
            "net_return": result.net_return,
            "stats": stats(result.interval_returns),
            "folds": folds,
            "fills": len(result.fills),
        }
    primary = trial_results[TRIALS[0][0]]
    doubled = evaluate_development(all_sessions, cost_bps=28.0, start_timestamp=FOLDS[0][0])
    delayed = evaluate_development(all_sessions, delay_sessions=1, start_timestamp=FOLDS[0][0])
    primary_folds = [item["net_return"] for item in primary["folds"]]
    primary_values = tuple(
        float(x)
        for x in evaluate_development(
            all_sessions, start_timestamp=FOLDS[0][0]
        ).interval_returns
    )
    boot = bootstrap(primary_values)
    gates = {
        "aggregate_net_return_gt": primary["net_return"] > 0,
        "annualized_sharpe_gte": primary["stats"]["annualized_sharpe"] >= 0.75,
        "maximum_drawdown_lte": primary["stats"]["maximum_drawdown"] <= 0.2,
        "fold_count": len(primary_folds) == 4,
        "positive_folds_minimum": sum(x > 0 for x in primary_folds) >= 3,
        "doubled_cost_aggregate_net_return_gt": doubled.net_return > 0,
        "additional_delay_aggregate_net_return_gt": delayed.net_return > 0,
        "positive_parameter_neighbors_minimum": (
            sum(x["net_return"] > 0 for x in trial_results.values()) - 1 >= 3
        ),
        "parameter_neighbor_count": len(TRIALS) - 1 == 6,
        "bootstrap_lower_ci_gt": boot["lower_95"] > 0,
        "deflated_sharpe_probability_gte": False,
        "probability_of_backtest_overfitting_lte": False,
        "baseline_superiority": False,
        "completed_entries_total_minimum": len(primary["folds"]) >= 24,
        "completed_holds_each_asset_minimum": False,
        "asset_net_contribution_each_gt": False,
        "exceptional_profit_gate": False,
        "regime_gate": False,
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
        "trials": trial_results,
        "gates": gates,
        "all_gates_pass": all(gates.values()),
        "dsr_registry_classification": "VALID_FAIL_CLOSED_NOT_IMPLEMENTED",
        "pbo_status": "FAIL_CLOSED_NOT_IMPLEMENTED",
        "formal_economic_result": False,
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
