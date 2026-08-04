"""Bounded development-only production validation for the final index repair."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import platform
import time
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

import pandas
import pyarrow

from strategy_control.mean_reversion_v2 import (
    ASSETS,
    Clock,
    Disposition,
    canonical_hash,
    exact_signal,
    reconcile_trace,
    require_predeclared_terminal_fill,
    trace_hashes,
)
from strategy_control.mean_reversion_v2_pipeline import (
    ALLOWLIST_SHA256,
    AllowlistEntry,
    MinuteRow,
    RepresentativeAccounting,
    build_joint_sessions,
    build_production_row_index,
    canonical_mechanical_evidence,
    fill_identities,
    materialize_rows,
    parse_verified_parquet,
    read_verified_entry,
    reconcile_representative_accounting,
    representative_row_hashes,
    terminal_fill_identity,
    verify_source_identity,
)

CONTROL_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT_ROOT = Path(__file__).resolve().parent
CONTRACT_PATH = CONTROL_ROOT / "experiments/btc-eth-vol-targeted-trend-v1/DATA_CONTRACT.json"
PREREGISTRATION_PATH = EXPERIMENT_ROOT / "PREREGISTRATION.json"
PIPELINE_PATH = CONTROL_ROOT / "src/strategy_control/mean_reversion_v2_pipeline.py"
PRODUCTION_TEST_PATH = CONTROL_ROOT / "tests/test_mean_reversion_v2_production.py"
BOUNDARY = datetime(2026, 1, 1, tzinfo=UTC)
FOLDS = (
    datetime(2025, 4, 1, tzinfo=UTC),
    datetime(2025, 7, 1, tzinfo=UTC),
    datetime(2025, 10, 1, tzinfo=UTC),
    BOUNDARY,
)
EXPECTED_ROWS = {"BTCUSDT": 790_558, "ETHUSDT": 790_558}
EXPECTED_MISSING_MINUTES = {"BTCUSDT": 2, "ETHUSDT": 2}
EXPECTED_JOINT_SESSIONS = 549


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class StageRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.started = time.monotonic()
        self.last = self.started
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    def emit(self, stage: str, **details: object) -> None:
        current = time.monotonic()
        record = {
            "at_utc": now(),
            "elapsed_seconds": current - self.started,
            "stage_duration_seconds": current - self.last,
            "stage": stage,
            **details,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        self.last = current


def independent_accounting(case: RepresentativeAccounting) -> dict[str, object]:
    prior = case.cash + sum(case.units[asset] * case.prior_prices[asset] for asset in ASSETS)
    pretrade = case.cash + sum(case.units[asset] * case.current_prices[asset] for asset in ASSETS)
    drifted = {
        asset: case.units[asset] * case.current_prices[asset] / pretrade for asset in ASSETS
    }
    turnover = sum(abs(case.target_weights[asset] - drifted[asset]) for asset in ASSETS)
    cost = pretrade * case.cost_rate * turnover
    postcost = pretrade - cost
    units = {
        asset: postcost * case.target_weights[asset] / case.current_prices[asset]
        for asset in ASSETS
    }
    cash = postcost * (1 - sum(case.target_weights.values()))
    return {
        "prior_postcost_equity": prior,
        "pretrade_equity": pretrade,
        "turnover": turnover,
        "cost": cost,
        "postcost_equity": postcost,
        "units": units,
        "cash": cash,
        "interval_return": postcost / prior - 1,
    }


def accounting_matches(observed: object, expected: dict[str, object]) -> bool:
    scalar_names = (
        "prior_postcost_equity",
        "pretrade_equity",
        "turnover",
        "cost",
        "postcost_equity",
        "cash",
        "interval_return",
    )
    if any(
        not math.isclose(
            float(getattr(observed, name)),
            float(expected[name]),
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
        for name in scalar_names
    ):
        return False
    expected_units = expected["units"]
    if not isinstance(expected_units, dict):
        return False
    return all(
        math.isclose(
            float(observed.units[asset]),
            float(expected_units[asset]),
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
        for asset in ASSETS
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--implementation-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stage-log", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    stages = StageRecorder(arguments.stage_log)
    stages.emit("process_started", implementation_commit=arguments.implementation_commit)

    freeze_path = arguments.source_root / (
        "data/frozen/historical-v2-pathc-20260723T175155Z/"
        "DATASET_FREEZE_MANIFEST_historical-v2-pathc-20260723T175155Z.json"
    )
    data_root = arguments.source_root / "data/real/historical-v2-pathc-20260723T175155Z"
    contract_raw = CONTRACT_PATH.read_bytes()
    contract = json.loads(contract_raw)
    freeze_raw = freeze_path.read_bytes()
    freeze = json.loads(freeze_raw)
    entries = [
        AllowlistEntry(
            item["bytes"],
            item["month"],
            item["relative_path"],
            item["sha256"],
            item["symbol"],
        )
        for item in contract["partitions"]
        if item["verification_scope"] == "HASH_AND_SCHEMA_METADATA_ONLY"
    ]
    entries = list(
        verify_source_identity(
            contract_bytes=contract_raw,
            source_commit=freeze["repository_commit"],
            freeze_manifest_sha256=sha256(freeze_raw),
            inventory_sha256=contract["canonical_inventory_sha256"],
            entries=entries,
        )
    )
    if len(entries) != 36 or any("year=2026" in entry.relative_path for entry in entries):
        raise RuntimeError("exact development allowlist was not isolated before path resolution")
    stages.emit(
        "global_identities_verified_before_market_path_resolution",
        development_allowlist_count=len(entries),
        holdout_entry_count=0,
    )

    rows_by_asset: dict[str, list[MinuteRow]] = {asset: [] for asset in ASSETS}
    partition_evidence: list[dict[str, object]] = []
    for number, entry in enumerate(entries, start=1):
        verified = read_verified_entry(data_root, entry)
        table = parse_verified_parquet(verified)
        rows = materialize_rows(table, verified)
        partition_evidence.append(
            {
                "relative_path": entry.relative_path,
                "bytes": len(verified.payload),
                "observed_sha256": sha256(verified.payload),
                "row_count": len(rows),
                "representative_row_hashes": list(representative_row_hashes(rows)),
            }
        )
        rows_by_asset[entry.symbol].extend(rows)
        stages.emit(
            "development_partition_materialized",
            partition_number=number,
            symbol=entry.symbol,
            month=entry.month,
            row_count=len(rows),
        )
        del rows, table, verified
        gc.collect()

    row_counts = {asset: len(rows_by_asset[asset]) for asset in ASSETS}
    if row_counts != EXPECTED_ROWS:
        raise RuntimeError(f"unexpected development row counts: {row_counts}")
    gap_neighbor_hashes: dict[str, list[str]] = {asset: [] for asset in ASSETS}
    missing_minutes: dict[str, int] = {}
    for asset in ASSETS:
        missing = 0
        rows = rows_by_asset[asset]
        for previous, current in pairwise(rows):
            delta = int((current.event_timestamp - previous.event_timestamp).total_seconds() // 60)
            if delta <= 0:
                raise RuntimeError("duplicate or nonmonotonic real row")
            if delta > 1:
                missing += delta - 1
                gap_neighbor_hashes[asset].extend((previous.identity, current.identity))
        missing_minutes[asset] = missing
    if missing_minutes != EXPECTED_MISSING_MINUTES:
        raise RuntimeError(f"unexpected development missing-minute counts: {missing_minutes}")
    stages.emit("row_and_gap_counts_verified", row_counts=row_counts, missing=missing_minutes)

    fold_evidence: dict[str, dict[str, object]] = {}
    main_index = None
    main_sessions = None
    main_fills = None
    for number, fold_boundary in enumerate(FOLDS, start=1):
        index = build_production_row_index(rows_by_asset, end=fold_boundary)
        sessions = build_joint_sessions(index, end=fold_boundary)
        fills = fill_identities(sessions, index, end=fold_boundary)
        terminal_identity = terminal_fill_identity(fills, end=fold_boundary)
        require_predeclared_terminal_fill(
            [item.base_timestamp for item in fills], terminal_identity.base_timestamp, fold_boundary
        )
        fold_evidence[str(number)] = {
            "boundary": fold_boundary,
            "retained_row_count": index.retained_row_count,
            "session_count": len(sessions),
            "incomplete_session_count": sum(not item.complete for item in sessions),
            "eligible_session_count": sum(item.segment is not None for item in sessions),
            "fill_count": len(fills),
            "terminal_fill_identity": terminal_identity.identity,
            "first_last_session_hash": canonical_hash(
                (sessions[0].identity, sessions[-1].identity)
            ),
        }
        stages.emit(
            "fold_index_sessions_and_fills_verified",
            fold_number=number,
            retained_row_count=index.retained_row_count,
            session_count=len(sessions),
            fill_count=len(fills),
        )
        if fold_boundary == BOUNDARY:
            main_index, main_sessions, main_fills = index, sessions, fills
        else:
            del index, sessions, fills, terminal_identity
            gc.collect()
    if main_index is None or main_sessions is None or main_fills is None:
        raise RuntimeError("full validation boundary was not constructed")
    if len(main_sessions) != EXPECTED_JOINT_SESSIONS:
        raise RuntimeError(f"unexpected joint session count: {len(main_sessions)}")

    fill_by_session = {item.session: item for item in main_fills}
    clocks = {asset: Clock() for asset in ASSETS}
    histories: dict[str, list[float]] = {asset: [] for asset in ASSETS}
    decisions = []
    targets = []
    trace_fills = []
    dispositions = []
    representative_target_row_hashes: dict[str, list[str]] = {asset: [] for asset in ASSETS}
    for session_index, session in enumerate(main_sessions):
        if not session.complete:
            terminal_censored = (
                session_index == len(main_sessions) - 1
                and session.session + timedelta(days=1) == BOUNDARY
            )
            if terminal_censored:
                break
            for asset in ASSETS:
                histories[asset].clear()
                clocks[asset].quarantine()
            continue
        for asset in ASSETS:
            histories[asset].append(session.closes[asset])
        identity = fill_by_session.get(session.session)
        if session.segment is None or identity is None:
            continue
        for asset in ASSETS:
            signal = exact_signal(histories[asset], len(histories[asset]) - 1)
            decision, target = clocks[asset].decide(
                asset,
                session.session,
                identity.base_timestamp,
                identity.fill_index,
                signal,
                delayed_fill_time=identity.delayed_timestamp,
            )
            decisions.append(decision)
            if target is None:
                continue
            targets.append(target)
            filled = clocks[asset].apply_fill(
                target.fill_time, identity.base_prices[asset], target.fill_index
            )
            if filled is None:
                raise RuntimeError("exact representative target did not fill")
            trace_fills.append(filled)
            dispositions.append(Disposition(target.target_id, "fill", filled.timestamp))
            if len(representative_target_row_hashes[asset]) < 2:
                representative_target_row_hashes[asset].append(
                    identity.base_row_identities[asset]
                )
    if any(len(representative_target_row_hashes[asset]) != 2 for asset in ASSETS):
        raise RuntimeError("fewer than two causal representative targets")
    stages.emit(
        "signals_targets_fills_and_dispositions_reconciled",
        decision_count=len(decisions),
        target_count=len(targets),
        fill_count=len(trace_fills),
        disposition_count=len(dispositions),
    )

    if len(main_fills) < 2:
        raise RuntimeError("insufficient representative synchronized fills")
    prior_fill, current_fill = main_fills[:2]
    representative_cases = []
    reconciliation_records = []
    for cost_rate in (0.0014, 0.0028):
        case = RepresentativeAccounting(
            units={asset: 0.5 / prior_fill.base_prices[asset] for asset in ASSETS},
            cash=0.0,
            prior_prices=prior_fill.base_prices,
            current_prices=current_fill.base_prices,
            target_weights={asset: 0.5 for asset in ASSETS},
            cost_rate=cost_rate,
        )
        observed = reconcile_representative_accounting(case)
        expected = independent_accounting(case)
        if not accounting_matches(observed, expected):
            raise RuntimeError("independent representative accounting mismatch")
        if math.isclose(observed.interval_return, 0.0, abs_tol=1e-15):
            raise RuntimeError("representative real interval is unexpectedly zero")
        representative_cases.append(observed)
        reconciliation_records.append(
            {
                "cost_rate": cost_rate,
                "reconciliation_identity": observed.identity,
                "independent_match": True,
                "nonzero_interval": True,
                "terminal_cash_hash": canonical_hash(observed.cash),
                "terminal_cash_independent_match": math.isclose(
                    observed.cash, float(expected["cash"]), rel_tol=1e-12, abs_tol=1e-12
                ),
            }
        )
    stages.emit("cost_return_and_terminal_cash_reconciled", case_count=2)

    costs = [item.cost for item in representative_cases]
    returns = [item.interval_return for item in representative_cases]
    input_identities = [item.identity for item in main_sessions]
    expected_trace_hashes = trace_hashes(
        inputs=input_identities,
        decisions=decisions,
        targets=targets,
        fills=trace_fills,
        dispositions=dispositions,
        costs=costs,
        returns=returns,
    )
    if not reconcile_trace(
        inputs=input_identities,
        decisions=decisions,
        targets=targets,
        fills=trace_fills,
        dispositions=dispositions,
        costs=costs,
        returns=returns,
        expected_hashes=expected_trace_hashes,
    ):
        raise RuntimeError("canonical representative decision trace did not reconcile")
    mechanical = canonical_mechanical_evidence(
        rows=[row for asset in ASSETS for row in rows_by_asset[asset]],
        sessions=main_sessions,
        fills=main_fills,
        trace_records=[expected_trace_hashes],
        cost_records=reconciliation_records,
        representative_returns=[
            {"identity": item.identity, "nonzero": True} for item in representative_cases
        ],
    )
    stages.emit("canonical_mechanical_evidence_complete")

    result: dict[str, Any] = {
        "schema_version": "2.0",
        "experiment_id": "btc-eth-long-only-mean-reversion-v2",
        "classification": "PASS_REAL_PRODUCTION_MECHANICS_PRE_FIDELITY_AUDIT",
        "invocation_mode": "deterministic_local",
        "implementation_commit": arguments.implementation_commit,
        "implementation_byte_hashes": {
            "pipeline": sha256(PIPELINE_PATH.read_bytes()),
            "production_tests": sha256(PRODUCTION_TEST_PATH.read_bytes()),
            "validation_driver": sha256(Path(__file__).read_bytes()),
        },
        "preregistration_byte_sha256": sha256(PREREGISTRATION_PATH.read_bytes()),
        "identity_verified_before_market_path_resolution": True,
        "source_commit_in_freeze_manifest": freeze["repository_commit"],
        "contract_byte_sha256": sha256(contract_raw),
        "contract_canonical_sha256": canonical_hash(contract),
        "freeze_manifest_byte_sha256": sha256(freeze_raw),
        "canonical_inventory_sha256": contract["canonical_inventory_sha256"],
        "development_allowlist_count": len(entries),
        "development_allowlist_sha256": ALLOWLIST_SHA256,
        "partition_evidence": partition_evidence,
        "partition_manifest_sha256": canonical_hash(partition_evidence),
        "parser_runtime": {
            "python": platform.python_version(),
            "pandas": pandas.__version__,
            "pyarrow": pyarrow.__version__,
            "list_valued_column_selector": True,
            "same_verified_buffer_parsed": True,
        },
        "row_counts": row_counts,
        "missing_minute_counts": missing_minutes,
        "gap_neighbor_hashes": gap_neighbor_hashes,
        "joint_session_count": len(main_sessions),
        "incomplete_joint_session_count": sum(not item.complete for item in main_sessions),
        "eligible_segment_session_count": sum(
            item.segment is not None for item in main_sessions
        ),
        "synchronized_fill_identity_count": len(main_fills),
        "terminal_fill_identity": terminal_fill_identity(main_fills, end=BOUNDARY).identity,
        "fold_evidence": fold_evidence,
        "index_work": {
            "retained_rows": main_index.retained_row_count,
            "expected_grid_entries": len(main_sessions) * 1440 * len(ASSETS),
            "row_by_session_product_eliminated": True,
        },
        "representative_target_row_hashes": representative_target_row_hashes,
        "decision_count": len(decisions),
        "target_count": len(targets),
        "filled_target_count": len(trace_fills),
        "disposition_count": len(dispositions),
        "trace_reconciled": True,
        "trace_hashes": expected_trace_hashes,
        "representative_accounting": reconciliation_records,
        "mechanical_evidence": mechanical.__dict__,
        "stage_log_byte_sha256_before_final_stage": sha256(arguments.stage_log.read_bytes()),
        "raw_market_values_persisted": False,
        "aggregate_strategy_returns_calculated": False,
        "sharpe_drawdown_bootstrap_dsr_pbo_or_gates_calculated": False,
        "economic_result_exists": False,
        "performance_claim_permitted": False,
        "formal_economic_attempt_authorized": False,
        "formal_economic_attempt_consumed": False,
        "holdout_entry_count_selected": 0,
        "holdout_path_resolved": False,
        "holdout_parquet_footer_or_value_read": False,
        "holdout_opened": False,
        "capital_permitted": 0,
        "gpu_seconds_used": 0,
        "vertcoin_mining": "UNCHANGED",
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    stages.emit(
        "complete_artifact_written",
        output_byte_sha256=sha256(arguments.output.read_bytes()),
    )


if __name__ == "__main__":
    main()
