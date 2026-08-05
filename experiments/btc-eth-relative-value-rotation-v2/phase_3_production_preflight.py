"""Development-only real-data preflight for Phase 3 relative-value v2.

This driver verifies the shared 36-partition contract and measures the existing
relative-value boundary/index interface.  It deliberately does not invoke the
strategy simulator, calculate an aggregate return, or interpret economics.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import inspect
import json
import time
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

from strategy_control.mean_reversion_v2 import canonical_hash
from strategy_control.mean_reversion_v2_pipeline import (
    ALLOWLIST_SHA256,
    AllowlistEntry,
    build_joint_sessions,
    build_production_row_index,
    fill_identities,
    materialize_rows,
    parse_verified_parquet,
    read_verified_entry,
    verify_source_identity,
)
from strategy_control.mean_reversion_v2_pipeline import MinuteRow as ProductionMinuteRow
from strategy_control.relative_value_v2 import (
    SYMBOLS,
    BoundaryIndex,
    CanonicalVector,
    MinuteRow,
    Observation,
    simulate_period,
    terminal_vector,
)

CONTROL_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT_ROOT = Path(__file__).resolve().parent
CONTRACT_PATH = CONTROL_ROOT / "experiments/btc-eth-vol-targeted-trend-v1/DATA_CONTRACT.json"
PREREGISTRATION_PATH = EXPERIMENT_ROOT / "PREREGISTRATION.json"
AUTHORIZATION_PATH = EXPERIMENT_ROOT / "PHASE_3_COMPLETION_AUTHORIZATION.json"
IMPLEMENTATION_PATH = CONTROL_ROOT / "src/strategy_control/relative_value_v2.py"
PIPELINE_PATH = CONTROL_ROOT / "src/strategy_control/relative_value_v2_pipeline.py"
BOUNDARY = datetime(2026, 1, 1, tzinfo=UTC)
FOLD_BOUNDARIES = (
    datetime(2025, 4, 1, tzinfo=UTC),
    datetime(2025, 7, 1, tzinfo=UTC),
    datetime(2025, 10, 1, tzinfo=UTC),
    BOUNDARY,
)
EXPECTED_ROWS = {"BTCUSDT": 790_558, "ETHUSDT": 790_558}
EXPECTED_MISSING_MINUTES = {"BTCUSDT": 2, "ETHUSDT": 2}


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


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stage-log", type=Path, required=True)
    return parser.parse_args()


def _relative_vector(fill: Any) -> CanonicalVector:
    btc = MinuteRow(
        SYMBOLS[0],
        fill.base_timestamp,
        float(fill.base_prices[SYMBOLS[0]]),
        str(fill.base_row_identities[SYMBOLS[0]]),
    )
    eth = MinuteRow(
        SYMBOLS[1],
        fill.base_timestamp,
        float(fill.base_prices[SYMBOLS[1]]),
        str(fill.base_row_identities[SYMBOLS[1]]),
    )
    return CanonicalVector(fill.base_timestamp, (btc, eth))


def _real_observations(
    sessions: Any, index: Any
) -> tuple[dict[str, tuple[Observation, ...]], tuple[datetime, ...]]:
    output: dict[str, list[Observation]] = {asset: [] for asset in SYMBOLS}
    session_ids: list[datetime] = []
    for session in sessions:
        if not session.complete or session.information_cutoff is None:
            continue
        close_stamp = session.session + timedelta(days=1)
        session_ids.append(session.session)
        for asset in SYMBOLS:
            row = index.rows_by_asset[asset].get(close_stamp)
            if row is None:
                raise RuntimeError("complete session has no exact retained close row")
            output[asset].append(
                Observation(
                    asset,
                    session.information_cutoff,
                    session.information_cutoff,
                    float(session.closes[asset]),
                    row.identity,
                )
            )
    return {asset: tuple(values) for asset, values in output.items()}, tuple(session_ids)


def _benchmark_existing_lookup(vectors: tuple[CanonicalVector, ...]) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    sizes = sorted({min(len(vectors), size) for size in (89, 178, 356) if vectors})
    for size in sizes:
        sample = vectors[:size]
        rows = tuple(row for vector in sample for row in vector.rows)
        started = time.monotonic()
        boundary_index = BoundaryIndex(rows, BOUNDARY)
        construction_seconds = time.monotonic() - started
        started = time.monotonic()
        for vector in sample:
            found = boundary_index.earliest_after(vector.timestamp - timedelta(microseconds=1))
            if found.row_ids != vector.row_ids:
                raise RuntimeError("existing exact lookup changed a retained real row identity")
        lookup_seconds = time.monotonic() - started
        results.append(
            {
                "vector_count": size,
                "retained_relative_rows": len(rows),
                "construction_seconds": construction_seconds,
                "repeated_earliest_after_seconds": lookup_seconds,
                "source_level_candidate_timestamp_visits": size * size,
            }
        )
    return results


def main() -> None:
    arguments = parse_arguments()
    stages = StageRecorder(arguments.stage_log)
    started_at = now()
    stages.emit("process_started", source_commit=arguments.source_commit)

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
    if len(entries) != 36 or any("year=2026" in item.relative_path for item in entries):
        raise RuntimeError("exact development allowlist not isolated before path resolution")
    stages.emit("development_identity_verified", allowlist_count=len(entries), holdout_count=0)

    rows_by_asset: dict[str, list[ProductionMinuteRow]] = {asset: [] for asset in SYMBOLS}
    partition_evidence: list[dict[str, object]] = []
    for number, entry in enumerate(entries, start=1):
        verified = read_verified_entry(data_root, entry)
        table = parse_verified_parquet(verified)
        rows = materialize_rows(table, verified)
        partition_evidence.append(
            {
                "relative_path": entry.relative_path,
                "byte_sha256": sha256(verified.payload),
                "row_count": len(rows),
                "first_row_identity": rows[0].identity,
                "middle_row_identity": rows[len(rows) // 2].identity,
                "last_row_identity": rows[-1].identity,
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

    row_counts = {asset: len(rows) for asset, rows in rows_by_asset.items()}
    if row_counts != EXPECTED_ROWS:
        raise RuntimeError(f"unexpected row counts: {row_counts}")
    missing_counts: dict[str, int] = {}
    for asset, rows in rows_by_asset.items():
        missing_counts[asset] = sum(
            max(0, int((right.event_timestamp - left.event_timestamp).total_seconds() // 60) - 1)
            for left, right in pairwise(rows)
        )
    if missing_counts != EXPECTED_MISSING_MINUTES:
        raise RuntimeError(f"unexpected missing-minute counts: {missing_counts}")
    stages.emit("row_and_gap_counts_verified", row_counts=row_counts, missing=missing_counts)

    fold_evidence: list[dict[str, object]] = []
    final_index = final_sessions = final_fills = None
    for number, boundary in enumerate(FOLD_BOUNDARIES, start=1):
        index_started = time.monotonic()
        index = build_production_row_index(rows_by_asset, end=boundary)
        sessions = build_joint_sessions(index, end=boundary)
        fills = fill_identities(sessions, index, end=boundary)
        elapsed = time.monotonic() - index_started
        fold_evidence.append(
            {
                "fold_number": number,
                "boundary": boundary.isoformat().replace("+00:00", "Z"),
                "retained_rows": index.retained_row_count,
                "sessions": len(sessions),
                "complete_sessions": sum(item.complete for item in sessions),
                "eligible_sessions": sum(item.segment is not None for item in sessions),
                "candidate_execution_lookups": len(fills),
                "construction_seconds": elapsed,
            }
        )
        stages.emit(
            "fold_mechanics_constructed",
            fold_number=number,
            retained_rows=index.retained_row_count,
            sessions=len(sessions),
            fills=len(fills),
        )
        if boundary == BOUNDARY:
            final_index, final_sessions, final_fills = index, sessions, fills
        else:
            del index, sessions, fills
            gc.collect()
    if final_index is None or final_sessions is None or final_fills is None:
        raise RuntimeError("final development boundary was not constructed")

    observations, observation_sessions = _real_observations(final_sessions, final_index)
    vectors = tuple(_relative_vector(fill) for fill in final_fills)
    if terminal_vector(vectors, BOUNDARY).row_ids != vectors[-1].row_ids:
        raise RuntimeError("terminal vector identity mismatch")
    overlap = min(len(observation_sessions), len(final_fills))
    positional_session_mismatches = sum(
        observation_sessions[index] != final_fills[index].session for index in range(overlap)
    )
    if positional_session_mismatches == 0:
        raise RuntimeError("expected missing explicit session mapping was not reproduced")
    stages.emit(
        "current_api_mapping_blocker_reproduced",
        observation_sessions=len(observation_sessions),
        execution_vectors=len(vectors),
        positional_session_mismatches=positional_session_mismatches,
    )

    lookup_benchmark = _benchmark_existing_lookup(vectors)
    stages.emit("existing_lookup_complexity_measured", points=len(lookup_benchmark))
    signature = str(inspect.signature(simulate_period))
    finished_at = now()
    result: dict[str, Any] = {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-relative-value-rotation-v2",
        "phase": 3,
        "classification": "LOCALIZED_PRODUCTION_ADAPTER_AND_SESSION_MAPPING_BLOCKER",
        "invocation_mode": "deterministic_local",
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "source_commit": arguments.source_commit,
        "input_bindings": {
            "authorization_byte_sha256": sha256(AUTHORIZATION_PATH.read_bytes()),
            "preregistration_byte_sha256": sha256(PREREGISTRATION_PATH.read_bytes()),
            "data_contract_byte_sha256": sha256(contract_raw),
            "freeze_manifest_byte_sha256": sha256(freeze_raw),
            "development_allowlist_sha256": ALLOWLIST_SHA256,
            "relative_value_implementation_byte_sha256": sha256(IMPLEMENTATION_PATH.read_bytes()),
            "relative_value_pipeline_byte_sha256": sha256(PIPELINE_PATH.read_bytes()),
            "preflight_driver_byte_sha256": sha256(Path(__file__).read_bytes()),
        },
        "production_observations": {
            "development_allowlist_count": len(entries),
            "holdout_entry_count": 0,
            "partition_manifest_sha256": canonical_hash(partition_evidence),
            "row_counts": row_counts,
            "known_missing_minute_counts": missing_counts,
            "folds": fold_evidence,
            "complete_observation_sessions": len(observation_sessions),
            "observations_per_asset": {
                asset: len(values) for asset, values in observations.items()
            },
            "candidate_execution_vectors": len(vectors),
            "terminal_vector_identity": canonical_hash(vectors[-1].row_ids),
        },
        "localized_blocker": {
            "simulator_signature": signature,
            "explicit_decision_session_mapping_parameter_present": False,
            "real_observation_sessions_compared": overlap,
            "positional_session_mismatches": positional_session_mismatches,
            "explanation": (
                "The current simulator pairs vector i with observation index i. Real eligible "
                "execution vectors begin only after causal recovery and are not positionally "
                "identical to the complete-session observation history. The API carries no "
                "decision-session identity or production adapter that can bind each signal "
                "session to its exact execution vector without changing mechanics."
            ),
            "strategy_simulator_invoked": False,
        },
        "complexity_benchmark": {
            "measured_points": lookup_benchmark,
            "source_level_behavior": (
                "BoundaryIndex.earliest_after builds a candidate list by scanning every indexed "
                "timestamp for every lookup."
            ),
            "measured_or_derived_work": (
                "source_level_candidate_timestamp_visits is derived exactly from the current "
                "all-timestamps list comprehension; wall times are measured."
            ),
            "required_repair_direction": (
                "one boundary-specific immutable exact timestamp/session index with direct "
                "signal-session-to-fill identity mapping"
            ),
        },
        "phase_3_mechanical_completion_round_consumed": False,
        "formal_economic_attempt_consumed": False,
        "aggregate_strategy_returns_calculated": False,
        "performance_metrics_calculated": False,
        "performance_claim_permitted": False,
        "holdout_path_resolved": False,
        "holdout_parquet_footer_or_value_read": False,
        "holdout_opened": False,
        "capital_permitted": 0,
        "gpu_seconds_used": 0,
        "vertcoin_mining": "UNCHANGED",
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    stages.emit("complete_preflight_artifact_written", output=str(arguments.output))


if __name__ == "__main__":
    main()
