"""Parent validation for Phase 3 mechanical completion round 1.

This driver reuses the committed development-only production preflight and
instruments the new no-I/O adapter.  It does not invoke a strategy simulator or
calculate returns.  Its purpose is to decide whether round 1 preserved exact
production identities and removed repeated terminal-fill scans.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar, cast

from strategy_control.mean_reversion_v2_pipeline import FillIdentity
from strategy_control.relative_value_v2_pipeline import build_production_bindings

CONTROL_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT_ROOT = Path(__file__).resolve().parent
BASE_DRIVER = EXPERIMENT_ROOT / "phase_3_production_preflight.py"
BOUNDARY = datetime(2026, 1, 1, tzinfo=UTC)
T = TypeVar("T")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class CountingSequence(Sequence[T]):
    """Read-only sequence that counts elements yielded through full iteration."""

    def __init__(self, values: Sequence[T]) -> None:
        self._values = tuple(values)
        self.iterated_elements = 0

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, index: int) -> T:
        return self._values[index]

    def __iter__(self) -> Iterator[T]:
        for value in self._values:
            self.iterated_elements += 1
            yield value


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--base-output", type=Path, required=True)
    parser.add_argument("--base-stage-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def load_base_driver() -> Any:
    spec = importlib.util.spec_from_file_location("relative_value_v2_phase3_preflight", BASE_DRIVER)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the committed production preflight")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    arguments = parse_arguments()
    started_at = now()
    module = load_base_driver()
    original_fill_identities = module.fill_identities
    measured: dict[str, object] = {}

    def instrumented_fill_identities(
        sessions: Sequence[Any], index: Any, *, end: datetime
    ) -> tuple[FillIdentity, ...]:
        fills = cast(tuple[FillIdentity, ...], original_fill_identities(sessions, index, end=end))
        if end != BOUNDARY:
            return fills
        counted = CountingSequence(fills)
        bindings = build_production_bindings(sessions, counted, end=end)
        source_by_session = {item.session: item for item in sessions}
        fill_by_session = {item.session: item for item in fills}
        observation_identity_comparisons = 0
        observation_identity_mismatches = 0
        observation_event_mismatches = 0
        base_identity_mismatches = 0
        delayed_identity_mismatches = 0
        for binding in bindings:
            source = source_by_session[binding.session_at]
            if binding.observations is not None:
                close_stamp = source.session + timedelta(days=1)
                for observation in binding.observations:
                    expected_row = index.rows_by_asset[observation.asset].get(close_stamp)
                    if expected_row is None:
                        raise RuntimeError("complete real session has no retained close identity")
                    observation_identity_comparisons += 1
                    observation_identity_mismatches += observation.identity != expected_row.identity
                    observation_event_mismatches += (
                        observation.event_at != expected_row.event_timestamp
                    )
            selected = fill_by_session.get(binding.session_at)
            if selected is None:
                continue
            if binding.base_fill is None:
                base_identity_mismatches += 1
            else:
                expected_base = tuple(
                    selected.base_row_identities[asset] for asset in module.SYMBOLS
                )
                base_identity_mismatches += binding.base_fill.row_ids != expected_base
            if selected.delayed_timestamp is not None:
                if binding.delayed_fill is None:
                    delayed_identity_mismatches += 1
                else:
                    expected_delayed = tuple(
                        selected.delayed_row_identities[asset] for asset in module.SYMBOLS
                    )
                    delayed_identity_mismatches += binding.delayed_fill.row_ids != expected_delayed
        terminal_bindings = tuple(item for item in bindings if item.terminal_fill is not None)
        measured.update(
            {
                "session_count": len(sessions),
                "fill_count": len(fills),
                "binding_count": len(bindings),
                "eligible_binding_count": sum(item.eligible for item in bindings),
                "observation_identity_comparisons": observation_identity_comparisons,
                "observation_identity_mismatches": observation_identity_mismatches,
                "observation_event_mismatches": observation_event_mismatches,
                "base_identity_mismatches": base_identity_mismatches,
                "delayed_identity_mismatches": delayed_identity_mismatches,
                "terminal_binding_count": len(terminal_bindings),
                "terminal_row_identity": (
                    list(terminal_bindings[0].terminal_fill.row_ids)
                    if len(terminal_bindings) == 1
                    and terminal_bindings[0].terminal_fill is not None
                    else None
                ),
                "fill_elements_iterated_by_adapter": counted.iterated_elements,
                "linear_fill_iteration_upper_bound": len(fills) + len(sessions),
            }
        )
        return fills

    module.fill_identities = instrumented_fill_identities
    sys.argv = [
        str(BASE_DRIVER),
        "--source-root",
        str(arguments.source_root),
        "--source-commit",
        arguments.source_commit,
        "--output",
        str(arguments.base_output),
        "--stage-log",
        str(arguments.base_stage_log),
    ]
    module.main()
    if not measured:
        raise RuntimeError("final-boundary adapter instrumentation did not execute")

    identity_exact = measured["observation_identity_mismatches"] == 0
    event_exact = measured["observation_event_mismatches"] == 0
    fill_exact = (
        measured["base_identity_mismatches"] == 0
        and measured["delayed_identity_mismatches"] == 0
    )
    terminal_linear = cast(int, measured["fill_elements_iterated_by_adapter"]) <= cast(
        int, measured["linear_fill_iteration_upper_bound"]
    )
    passed = identity_exact and event_exact and fill_exact and terminal_linear
    result = {
        "schema_version": "1.0",
        "experiment_id": "btc-eth-relative-value-rotation-v2",
        "phase": 3,
        "repair_round": 1,
        "classification": (
            "MECHANICAL_COMPLETION_ROUND_1_PARENT_VALIDATION_PASS"
            if passed
            else "MECHANICAL_COMPLETION_ROUND_1_PARENT_VALIDATION_FAILED"
        ),
        "started_at_utc": started_at,
        "finished_at_utc": now(),
        "source_commit_before_attempt": arguments.source_commit,
        "invocation_mode": "deterministic_local",
        "input_bindings": {
            "base_driver_byte_sha256": sha256(BASE_DRIVER),
            "relative_value_implementation_byte_sha256": sha256(
                CONTROL_ROOT / "src/strategy_control/relative_value_v2.py"
            ),
            "relative_value_pipeline_byte_sha256": sha256(
                CONTROL_ROOT / "src/strategy_control/relative_value_v2_pipeline.py"
            ),
            "base_output_byte_sha256": sha256(arguments.base_output),
            "base_stage_log_byte_sha256": sha256(arguments.base_stage_log),
        },
        "real_production_measurements": measured,
        "checks": {
            "full_per_asset_observation_row_identities_preserved": identity_exact,
            "observation_event_timestamps_preserved": event_exact,
            "exact_base_and_delayed_fill_identities_preserved": fill_exact,
            "terminal_selection_is_linear_not_sessions_times_fills": terminal_linear,
        },
        "verdict": "PASS" if passed else "IMPLEMENTATION_BLOCKED",
        "strategy_simulator_invoked": False,
        "aggregate_strategy_returns_calculated": False,
        "performance_metrics_calculated": False,
        "formal_economic_attempt_consumed": False,
        "holdout_path_resolved": False,
        "holdout_parquet_footer_or_value_read": False,
        "capital_permitted": 0,
        "gpu_seconds_used": 0,
        "vertcoin_mining": "UNCHANGED",
    }
    arguments.output.write_text(
        json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    if passed:
        return
    raise SystemExit(1)


if __name__ == "__main__":
    main()
