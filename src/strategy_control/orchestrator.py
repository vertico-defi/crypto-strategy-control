"""Bounded, zero-capital research-program state machine.

The controller has no exchange, wallet, order, or capital interface.  Production
model work is delegated only to an explicit, read-only Codex CLI invocation;
mock and deterministic-local modes exist for validation and can never be
reported as model-generated research.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from strategy_control.archive_audit import ArchiveAuditError, run_archive_observed_audit
from strategy_control.calendar_evaluator import (
    CalendarEvaluationError,
    evaluate_calendar_development,
)
from strategy_control.calendar_evaluator import (
    load_development_market as load_calendar_development_market,
)
from strategy_control.calendar_pipeline import (
    CalendarPipelineError,
)
from strategy_control.calendar_pipeline import (
    verify_preregistration as verify_calendar_preregistration,
)
from strategy_control.mean_reversion_pipeline import (
    evaluate_development as evaluate_mean_reversion_development,
)
from strategy_control.relative_value_pipeline import (
    evaluate_development as evaluate_relative_value_development,
)
from strategy_control.trend_data import TrendDataError, verify_trend_data
from strategy_control.trend_pipeline import (
    evaluate_development as evaluate_trend_development,
)
from strategy_control.trend_pipeline import (
    load_development_market,
)
from strategy_control.volatility_managed import VolatilityManagedError
from strategy_control.volatility_managed_evaluator import (
    evaluate_development as evaluate_volatility_managed_development,
)
from strategy_control.volatility_managed_evaluator import (
    load_development_market as load_volatility_managed_development_market,
)
from strategy_control.volatility_managed_evaluator import (
    verify_frozen_contract as verify_volatility_managed_frozen_contract,
)
from strategy_control.volatility_parity import VolatilityParityError
from strategy_control.volatility_parity_evaluator import (
    evaluate_development as evaluate_volatility_parity_development,
)
from strategy_control.volatility_parity_evaluator import (
    load_development_market as load_volatility_parity_development_market,
)
from strategy_control.volatility_parity_pipeline import (
    verify_frozen_contract as verify_volatility_parity_frozen_contract,
)

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "CURRENT_STATE.json"
LEDGER = ROOT / "EXPERIMENT_LEDGER.jsonl"
REJECTED = ROOT / "REJECTED_STRATEGIES.jsonl"
MODEL_LEDGER = ROOT / "MODEL_USAGE_LEDGER.jsonl"
PUBLICATION_LOG = ROOT / "PUBLICATION_LOG.jsonl"
SYNC = ROOT / "GITHUB_SYNC_STATE.json"
INVENTORY = ROOT / "DATA_INVENTORY.json"
LOCK = ROOT / ".research-orchestrator.lock"
COMPLETED_EXPERIMENT_ID = "cs-ranking-ptu-data-audit-v1"
ARCHIVE_EXPERIMENT_ID = "cs-ranking-binance-spot-archive-ptu-audit-v1"
TREND_EXPERIMENT_ID = "btc-eth-vol-targeted-trend-v1"
MEAN_REVERSION_EXPERIMENT_ID = "btc-eth-long-only-mean-reversion-v1"
RELATIVE_VALUE_EXPERIMENT_ID = "btc-eth-relative-value-rotation-v1"
CALENDAR_EXPERIMENT_ID = "btc-eth-intraday-calendar-seasonality-v1"
VOLATILITY_PARITY_EXPERIMENT_ID = "btc-eth-causal-volatility-parity-rebalancing-v1"
VOLATILITY_MANAGED_EXPERIMENT_ID = "btc-eth-volatility-managed-equal-weight-v1"
PHASE_2_MEAN_REVERSION_EXPERIMENT_ID = "btc-eth-long-only-mean-reversion-v2"
PHASE_2_RELATIVE_VALUE_EXPERIMENT_ID = "btc-eth-relative-value-rotation-v2"
PHASE_2_VOLATILITY_MANAGED_EXPERIMENT_ID = "btc-eth-volatility-managed-equal-weight-v2"
PHASE_2_ARCHIVE_ACQUISITION_EXPERIMENT_ID = (
    "cs-ranking-binance-spot-archive-ptu-acquisition-v3"
)
ACTIVE_PROGRAM_STATES = frozenset({"ACTIVE_RESEARCH", "ACTIVE_RESEARCH_PHASE_2"})
INVOCATION_MODES = ("live", "mock", "deterministic_local")
InvocationMode = Literal["live", "mock", "deterministic_local"]


class StateError(RuntimeError):
    """Raised when persisted research state is invalid or unsafe to mutate."""


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def atomic_json(path: Path, value: object) -> None:
    """Write JSON atomically, retaining neither partial state nor temp artifacts."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(_canonical(value) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StateError(f"state corruption or absence: {path}") from exc
    if not isinstance(value, dict):
        raise StateError(f"state must be an object: {path}")
    return value


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(_canonical(value) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


@contextmanager
def exclusive_lock(
    path: Path = LOCK,
    *,
    owner_type: str = "manual",
    run_id: str | None = None,
    stale_seconds: int | None = None,
) -> Iterator[None]:
    """Hold a kernel advisory lock for the full mutation, with owner metadata.

    ``stale_seconds`` remains as a compatibility-only argument.  Lock ownership
    is never stolen based on age; the kernel releases it when the owning process
    exits.
    """

    del stale_seconds
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise StateError("concurrent research mutation refused") from None
        metadata: dict[str, object] = {
            "pid": os.getpid(),
            "process_start_identity": _process_start_identity(),
            "owner_type": owner_type,
            "run_id": run_id,
            "started_at_utc": _now(),
            "status": "active",
        }
        os.ftruncate(descriptor, 0)
        os.write(descriptor, (_canonical(metadata) + "\n").encode())
        os.fsync(descriptor)
        yield
    finally:
        if "metadata" in locals():
            metadata.update({"released_at_utc": _now(), "status": "released"})
            os.ftruncate(descriptor, 0)
            os.lseek(descriptor, 0, os.SEEK_SET)
            os.write(descriptor, (_canonical(metadata) + "\n").encode())
            os.fsync(descriptor)
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _process_start_identity() -> str | None:
    """Return Linux process start ticks without treating their absence as safe staleness."""

    try:
        fields = Path(f"/proc/{os.getpid()}/stat").read_text(encoding="utf-8").split()
    except OSError:
        return None
    return fields[21] if len(fields) > 21 else None


def validate_state(state: dict[str, Any]) -> None:
    required = {"schema_version", "program_state", "capital_permitted", "next_task", "budgets"}
    missing = required - state.keys()
    if missing:
        raise StateError(f"state missing keys: {sorted(missing)}")
    if state["capital_permitted"] != 0:
        raise StateError("capital permission must remain zero")
    if state["program_state"] not in {
        *ACTIVE_PROGRAM_STATES,
        "DATA_BLOCKED",
        "PAUSED_FOR_USAGE",
        "PUBLICATION_PENDING",
        "RESEARCH_BUDGET_EXHAUSTED",
        "INFRASTRUCTURE_BLOCKED",
        "SAFETY_STOP",
    }:
        raise StateError("unsupported program state")


def is_active_program_state(state: dict[str, Any]) -> bool:
    """Recognize active research phases without relabeling a prior terminal phase."""

    return state.get("program_state") in ACTIVE_PROGRAM_STATES


def git_state() -> dict[str, Any]:
    import subprocess

    def run(*args: str) -> str | None:
        result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else None

    return {
        "branch": run("git", "branch", "--show-current"),
        "head": run("git", "rev-parse", "HEAD"),
        "clean": not bool(run("git", "status", "--porcelain")),
    }


def _lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    values: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise StateError(f"corrupt JSONL {path}:{number}") from exc
        if not isinstance(item, dict):
            raise StateError(f"non-object JSONL {path}:{number}")
        values.append(item)
    return values


def terminal_experiments() -> list[dict[str, Any]]:
    """Return terminal experiment rows, excluding program-level ledger events."""

    return [
        item
        for item in _lines(LEDGER)
        if item.get("record_type") != "PROGRAM_STATE_CORRECTION"
        and item.get("experiment_id")
        and item.get("classification")
    ]


def select_task(state: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return one distinct, high-information task without inspecting a holdout."""

    current = state or load_json(STATE)
    if current.get("program_state") == "ACTIVE_RESEARCH_PHASE_2":
        return {
            "task": current.get("next_task", "PHASE_2_TASK_NOT_REGISTERED"),
            "experiment_id": current.get("current_experiment_id"),
            "phase": 2,
            "information_value": (
                "Execute the explicitly authorized Phase 2 queue while preserving all "
                "Phase 1 terminal evidence."
            ),
        }
    terminal_ids = {str(item["experiment_id"]) for item in terminal_experiments()}
    if COMPLETED_EXPERIMENT_ID not in terminal_ids:
        raise StateError("frozen initial DATA_NO_GO record is missing")
    if ARCHIVE_EXPERIMENT_ID in terminal_ids:
        return {"task": "SELECT_NEXT_APPROVED_FAMILY", "reason": "archive audit is terminal"}
    if not is_active_program_state(current):
        return {"task": "PROGRAM_NOT_ACTIVE", "reason": str(current.get("program_state"))}
    return {
        "task": ARCHIVE_EXPERIMENT_ID,
        "family": "COST_AWARE_CROSS_SECTIONAL_RANKING",
        "hypothesis": (
            "Official Binance public historical spot archives may support a causal, "
            "archive-observed point-in-time USDT universe without current-listing or "
            "end-of-sample-survival contamination."
        ),
        "information_value": (
            "Resolve a distinct lawful data route before any strategy holdout is opened."
        ),
    }


def preregistration(task: dict[str, Any], source_commit: str) -> dict[str, Any]:
    preregistered_at = _now()
    return {
        "schema_version": "2.0",
        "experiment_id": ARCHIVE_EXPERIMENT_ID,
        "preregistered_at_utc": preregistered_at,
        "strategy_family": task["family"],
        "economic_hypothesis": task["hypothesis"],
        "audit_scope": "official Binance public historical spot archive metadata and bars only",
        "official_sources": {
            "documentation": "https://github.com/binance/binance-public-data/blob/master/README.md",
            "download_origin": "https://data.binance.vision",
            "bucket_list_endpoint": ("https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"),
            "spot_monthly_kline_prefix": "data/spot/monthly/klines/",
            "license": "MIT",
            "authentication_required": False,
        },
        "enumeration_contract": {
            "protocol": "S3_ListBucket_v1_XML",
            "delimiter": "/",
            "initial_marker": "",
            "pagination": "use_exact_NextMarker_until_IsTruncated_false",
            "max_keys": 1000,
            "symbol_rule": "archive_directory_basename_endswith_USDT",
            "current_exchange_info_requests": 0,
            "raw_page_storage": "hash_and_retain_sanitized_XML_metadata_only",
            "max_retrieval_delay_after_freeze_hours": 24,
        },
        "sample_contract": {
            "interval": "1d",
            "timezone": "UTC",
            "first_permitted_open_time_utc": "2017-08-01T00:00:00Z",
            "last_permitted_open_time_utc": "2026-06-30T00:00:00Z",
            "last_archive_month": "2026-06",
            "exclude_partial_months": True,
            "timestamp_unit_before_2025_01_01": "milliseconds",
            "timestamp_unit_from_2025_01_01": "microseconds",
        },
        "universe_claim": (
            "Archive-observed historical USDT spot-pair universe; formal exchange-wide "
            "archive completeness is not claimed."
        ),
        "causality": {
            "membership_information_cutoff": "prior_completed_bar",
            "liquidity_ranking": "lagged_only",
            "execution": "signal_t_execute_next_real_bar",
            "current_exchange_info_permitted": False,
            "market_capitalization_permitted": False,
            "end_of_sample_survival_filter_permitted": False,
        },
        "required_manifest_fields": [
            "symbol",
            "episode_id",
            "first_valid_bar_open_time",
            "last_valid_bar_open_time",
            "observed_months",
            "missing_months",
            "unexplained_gaps",
            "uncertainty_status",
            "source_urls",
            "source_hashes",
        ],
        "conservative_rules": {
            "listing_buffer_completed_bars": 30,
            "liquidity_lookback_completed_bars": 30,
            "gap_recovery_completed_bars": 30,
            "delisting_entry_buffer_completed_bars": 1,
            "missing_or_uncertain_periods": "quarantine",
            "rename_or_migration": "separate_episodes_unless_causally_proven",
            "absent_next_bar": "no_fill_and_quarantine_terminal_exposure",
            "duplicate_archive_key": "identical_hash_deduplicate_conflict_quarantine",
            "official_notice_use": "only_if_causally_timestamped_and_archived",
        },
        "data_sufficiency": {
            "all_symbol_directory_pages_retrieved": True,
            "all_USDT_symbol_1d_object_pages_retrieved": True,
            "minimum_archive_observed_USDT_symbols": 25,
            "first_and_last_candidate_zip_checksum_required": True,
            "first_and_last_candidate_zip_parse_required": True,
            "unexplained_missing_archive_months": "quarantine_not_impute",
            "internal_bar_validation_before_strategy_eligibility": "required",
        },
        "required_deterministic_tests": [
            "future_informed_universe_membership",
            "current_exchange_info_contamination",
            "survivorship_leakage",
            "first_bar_eligibility",
            "last_bar_treatment",
            "missing_month_treatment",
            "duplicate_symbols",
            "renamed_or_migrated_symbols",
            "causal_liquidity_ranking",
            "execution_alignment",
        ],
        "holdout_policy": (
            "No strategy holdout access, returns, tuning, or performance claim during this audit."
        ),
        "pass_rule": {
            "critical_tests": "all_pass",
            "manifest": "versioned_canonical_and_hashed",
            "uncertainty": "quarantined_under_frozen_rules",
            "independent_audit": "required",
            "limitations_disclosed": True,
        },
        "fail_closed_on": [
            "unverified_official_public_provenance",
            "nonreproducible_enumeration",
            "current_metadata_affects_membership",
            "prefix_or_survivorship_or_execution_test_failure",
            "silent_gap_or_symbol_collision_resolution",
            "source_hash_conflict",
            "post_hoc_quarantine_rule",
            "formal_completeness_claim_required",
        ],
        "gates_file": "ACCEPTANCE_GATES.yaml",
        "compute_budget": {
            "wall_seconds": 900,
            "agent_calls": 3,
            "repair_attempts": 1,
            "experiments": 1,
            "gpu_seconds": 0,
        },
        "source_commit": source_commit,
        "capital_permitted": 0,
    }


def preregistration_path() -> Path:
    return ROOT / "experiments" / ARCHIVE_EXPERIMENT_ID / "PREREGISTRATION.json"


def freeze_preregistration(*, dry_run: bool) -> dict[str, Any]:
    """Freeze the hypothesis before any data-contract result is recorded."""

    state = load_json(STATE)
    task = select_task(state)
    if task["task"] != ARCHIVE_EXPERIMENT_ID:
        raise StateError(f"archive experiment is not selectable: {task['task']}")
    path = preregistration_path()
    if path.exists():
        raise StateError("preregistration already exists; commit or evaluate it")
    git = git_state()
    if not git["clean"]:
        raise StateError("controller working tree must be clean before preregistration")
    prereg = preregistration(task, str(git["head"]))
    prereg["preregistration_sha256"] = _sha(prereg)
    if dry_run:
        return {"dry_run": True, "would_write": str(path.relative_to(ROOT)), "task": task}
    atomic_json(path, prereg)
    validate_state(state)
    state.update(
        {
            "program_state": "ACTIVE_RESEARCH",
            "current_experiment_id": ARCHIVE_EXPERIMENT_ID,
            "next_task": "commit_preregistration_then_run_data_contract",
            "updated_at_utc": _now(),
        }
    )
    atomic_json(STATE, state)
    return {"status": "PREREGISTRATION_FROZEN", "path": str(path.relative_to(ROOT))}


def model_route(outcome: str) -> dict[str, str | None]:
    """Route only on confirmed usage or temporary model-availability failures."""

    routes: dict[str, dict[str, str | None]] = {
        "success": {"model": "gpt-5.6-sol", "reasoning": "xhigh", "status": "USED"},
        "confirmed_quota_exhausted": {
            "model": None,
            "reasoning": None,
            "status": "PAUSED_FOR_USAGE",
        },
        "confirmed_rate_limit": {
            "model": "gpt-5.6-terra",
            "reasoning": "medium",
            "status": "FALLBACK_ELIGIBLE",
        },
        "confirmed_temporary_model_unavailable": {
            "model": "gpt-5.6-terra",
            "reasoning": "medium",
            "status": "FALLBACK_ELIGIBLE",
        },
        "substantive_failure": {"model": None, "reasoning": None, "status": "FAILED"},
        "coding_failure": {"model": None, "reasoning": None, "status": "FAILED"},
        "test_failure": {"model": None, "reasoning": None, "status": "FAILED"},
        "audit_rejection": {"model": None, "reasoning": None, "status": "FAILED"},
        "infrastructure_failure": {
            "model": None,
            "reasoning": None,
            "status": "INFRASTRUCTURE_BLOCKED",
        },
        "authentication_failure": {
            "model": None,
            "reasoning": None,
            "status": "AUTHENTICATION_BLOCKED",
        },
    }
    if outcome not in routes:
        raise StateError(f"unknown model outcome: {outcome}")
    return routes[outcome]


@dataclass
class InvocationResult:
    """Auditable invocation metadata; model text is intentionally transient."""

    started_at_utc: str
    ended_at_utc: str
    duration_seconds: float
    role: str
    requested_model: str | None
    actual_model: str | None
    reasoning_level: str | None
    invocation_mode: InvocationMode
    outcome: str
    model_result_received: bool
    response_identifier: str | None
    result_sha256: str | None
    exit_code: int | None
    exact_error: str | None
    fallback: str | None
    final_message: str | None

    def ledger_record(self, **extra: object) -> dict[str, Any]:
        record = asdict(self)
        record.pop("final_message")
        record.update(extra)
        return record


def codex_command(*, model: str, reasoning: str, prompt: str) -> list[str]:
    """Build an explicit, ephemeral, read-only live Codex invocation."""

    return [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
        "--ephemeral",
        "--json",
        "--model",
        model,
        "-c",
        f'model_reasoning_effort="{reasoning}"',
        "--sandbox",
        "read-only",
        "--cd",
        str(ROOT),
        prompt,
    ]


def _bounded_error(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    return normalized[-4000:] if normalized else None


def _failure_outcome(error: str | None) -> str:
    lowered = (error or "").lower()
    if any(term in lowered for term in ("not logged in", "authentication", "unauthorized")):
        return "AUTHENTICATION_FAILURE"
    if any(term in lowered for term in ("quota", "usage limit", "credits exhausted")):
        return "CONFIRMED_QUOTA_EXHAUSTED"
    if "rate limit" in lowered or "429" in lowered:
        return "CONFIRMED_RATE_LIMIT"
    if "temporarily unavailable" in lowered or "model unavailable" in lowered:
        return "CONFIRMED_TEMPORARY_MODEL_UNAVAILABLE"
    return "INFRASTRUCTURE_FAILURE"


def invoke_codex(
    *,
    invocation_mode: InvocationMode,
    role: str,
    model: str,
    reasoning: str,
    prompt: str,
    timeout_seconds: int = 180,
) -> InvocationResult:
    """Invoke Codex or a clearly labelled non-live validation mode."""

    if invocation_mode not in INVOCATION_MODES:
        raise StateError(f"unsupported invocation mode: {invocation_mode}")
    started = _now()
    monotonic_start = time.monotonic()
    if invocation_mode != "live":
        ended = _now()
        return InvocationResult(
            started_at_utc=started,
            ended_at_utc=ended,
            duration_seconds=round(time.monotonic() - monotonic_start, 6),
            role=role,
            requested_model=model if invocation_mode == "mock" else None,
            actual_model=None,
            reasoning_level=reasoning if invocation_mode == "mock" else None,
            invocation_mode=invocation_mode,
            outcome="MOCK_VALIDATION" if invocation_mode == "mock" else "DETERMINISTIC_LOCAL",
            model_result_received=False,
            response_identifier=None,
            result_sha256=None,
            exit_code=0,
            exact_error=None,
            fallback=None,
            final_message=None,
        )

    command = codex_command(model=model, reasoning=reasoning, prompt=prompt)
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        exit_code: int | None = completed.returncode
        stderr = _bounded_error(completed.stderr)
        response_identifier: str | None = None
        messages: list[str] = []
        for line in completed.stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "thread.started":
                response_identifier = str(event.get("thread_id"))
            item = event.get("item")
            if (
                event.get("type") == "item.completed"
                and isinstance(item, dict)
                and item.get("type") == "agent_message"
                and isinstance(item.get("text"), str)
            ):
                messages.append(str(item["text"]))
        final_message = messages[-1] if completed.returncode == 0 and messages else None
        model_result_received = final_message is not None
        outcome = "SUCCESS" if model_result_received else _failure_outcome(stderr)
        if completed.returncode == 0 and not model_result_received and stderr is None:
            stderr = "Codex exited successfully without a final agent message"
            outcome = "INFRASTRUCTURE_FAILURE"
    except FileNotFoundError as exc:
        exit_code = None
        stderr = str(exc)
        response_identifier = None
        final_message = None
        model_result_received = False
        outcome = "INFRASTRUCTURE_FAILURE"
    except subprocess.TimeoutExpired:
        exit_code = None
        stderr = f"Codex invocation timed out after {timeout_seconds} seconds"
        response_identifier = None
        final_message = None
        model_result_received = False
        outcome = "INFRASTRUCTURE_FAILURE"
    return InvocationResult(
        started_at_utc=started,
        ended_at_utc=_now(),
        duration_seconds=round(time.monotonic() - monotonic_start, 6),
        role=role,
        requested_model=model,
        actual_model=model if model_result_received else None,
        reasoning_level=reasoning,
        invocation_mode=invocation_mode,
        outcome=outcome,
        model_result_received=model_result_received,
        response_identifier=response_identifier,
        result_sha256=_sha({"message": final_message}) if final_message is not None else None,
        exit_code=exit_code,
        exact_error=_bounded_error(stderr) if not model_result_received else None,
        fallback=None,
        final_message=final_message,
    )


def model_generated_claim_permitted(invocation: InvocationResult) -> bool:
    """Mechanically prohibit model-generated claims from non-live or failed calls."""

    return invocation.invocation_mode == "live" and invocation.model_result_received


def _contains_preserved_no_go(value: object) -> bool:
    """Accept compact or structured JSON while requiring the immutable verdict."""

    if isinstance(value, str):
        return value in {"DATA_NO_GO", "DATA_NO_GO_CONFIRMED"}
    if isinstance(value, dict):
        return any(_contains_preserved_no_go(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_preserved_no_go(item) for item in value)
    return False


def smoke_review(*, invocation_mode: InvocationMode) -> dict[str, Any]:
    """Run a harmless governance-state review with no holdout or performance scope."""

    prompt = (
        "Harmless live smoke test only. Read AGENTS.md, RESEARCH_PROTOCOL.md, "
        "CURRENT_STATE.json, EXPERIMENT_LEDGER.jsonl, MODEL_USAGE_LEDGER.jsonl, and "
        "experiments/cs-ranking-ptu-data-audit-v1/AUDIT.json. Do not edit anything, do not "
        "inspect any strategy holdout or raw market data, do not calculate returns, and do "
        "not make any strategy-performance claim. Return one compact JSON object with keys "
        "review_scope, preserved_terminal_result, overall_state_observation, "
        "capital_permitted, holdout_opened, performance_claim_made."
    )
    invocation = invoke_codex(
        invocation_mode=invocation_mode,
        role="research_state_smoke_reviewer",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt=prompt,
    )
    contract_passed = False
    if model_generated_claim_permitted(invocation) and invocation.final_message is not None:
        try:
            payload = json.loads(invocation.final_message)
            contract_passed = bool(
                isinstance(payload, dict)
                and _contains_preserved_no_go(payload.get("preserved_terminal_result"))
                and payload.get("capital_permitted") == 0
                and payload.get("holdout_opened") is False
                and payload.get("performance_claim_made") is False
            )
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            payload = None
            invocation.exact_error = f"smoke response validation failed: {exc}"
        if not contract_passed:
            invocation.outcome = "CONTRACT_VIOLATION"
    append_jsonl(
        MODEL_LEDGER,
        invocation.ledger_record(
            record_type="MODEL_INVOCATION",
            purpose="repository_state_smoke_test",
            smoke_test_passed=contract_passed,
            model_generated_research_claim=False,
        ),
    )
    return {
        "status": "LIVE_SMOKE_PASS" if contract_passed else invocation.outcome,
        "invocation_mode": invocation.invocation_mode,
        "requested_model": invocation.requested_model,
        "actual_model": invocation.actual_model,
        "reasoning_level": invocation.reasoning_level,
        "model_result_received": invocation.model_result_received,
        "response_identifier": invocation.response_identifier,
        "smoke_test_passed": contract_passed,
        "capital_permitted": 0,
    }


def run_trend_direction_review(*, invocation_mode: InvocationMode) -> dict[str, Any]:
    """Review the trend draft before it is frozen or any holdout values are read."""

    draft_path = ROOT / "experiments" / TREND_EXPERIMENT_ID / "PREREGISTRATION_DRAFT.json"
    draft = load_json(draft_path)
    if draft.get("status") != "DRAFT_NOT_FROZEN":
        raise StateError("trend direction review requires an unfrozen draft")
    prompt = (
        "Act as the research director for a zero-capital methodology review. Read "
        "AGENTS.md, RESEARCH_PROTOCOL.md, ACCEPTANCE_GATES.yaml, CURRENT_STATE.json, and "
        "experiments/btc-eth-vol-targeted-trend-v1/PREREGISTRATION_DRAFT.json. Do not edit "
        "anything. Do not inspect raw market data, any 2026 holdout values, prior strategy "
        "returns, or model transcripts. Assess causal timing, daily aggregation, next-bar "
        "execution, cost accounting, fixed variants, walk-forward folds, bootstrap, DSR, "
        "PBO, regime and asset sensitivity, holdout gating, prospective gating, and budget. "
        "Return exactly one compact JSON object with keys verdict, strengths, "
        "required_revisions, statistical_concerns, holdout_opened, performance_claim_made, "
        "capital_permitted. verdict must be PRE_FREEZE_READY or REVISION_REQUIRED."
    )
    invocation = invoke_codex(
        invocation_mode=invocation_mode,
        role="research_director",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt=prompt,
        timeout_seconds=300,
    )
    payload: dict[str, Any] | None = None
    contract_passed = False
    if model_generated_claim_permitted(invocation) and invocation.final_message is not None:
        try:
            parsed = json.loads(invocation.final_message)
            if isinstance(parsed, dict):
                payload = parsed
            contract_passed = bool(
                payload is not None
                and payload.get("verdict") in {"PRE_FREEZE_READY", "REVISION_REQUIRED"}
                and isinstance(payload.get("strengths"), list)
                and isinstance(payload.get("required_revisions"), list)
                and isinstance(payload.get("statistical_concerns"), list)
                and payload.get("holdout_opened") is False
                and payload.get("performance_claim_made") is False
                and payload.get("capital_permitted") == 0
            )
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            invocation.exact_error = f"trend direction response validation failed: {exc}"
        if not contract_passed:
            invocation.outcome = "CONTRACT_VIOLATION"
    append_jsonl(
        MODEL_LEDGER,
        invocation.ledger_record(
            record_type="MODEL_INVOCATION",
            purpose="trend_preregistration_direction_review",
            experiment_id=TREND_EXPERIMENT_ID,
            draft_sha256=_sha(draft),
            review_contract_passed=contract_passed,
            review_verdict=payload.get("verdict") if payload is not None else None,
            model_generated_research_claim=contract_passed,
        ),
    )
    return {
        "status": "LIVE_DIRECTION_REVIEW_COMPLETE" if contract_passed else invocation.outcome,
        "invocation_mode": invocation.invocation_mode,
        "actual_model": invocation.actual_model,
        "reasoning_level": invocation.reasoning_level,
        "response_identifier": invocation.response_identifier,
        "review": payload if contract_passed else None,
        "capital_permitted": 0,
    }


def sanitize_mean_reversion_direction_payload(payload: object) -> dict[str, Any]:
    """Allowlist the distinct-family direction review and enforce no-data scope."""

    if not isinstance(payload, dict):
        raise StateError("mean-reversion direction response must be a JSON object")
    verdict = payload.get("verdict")
    if verdict not in {"PRE_FREEZE_READY", "REVISION_REQUIRED"}:
        raise StateError("mean-reversion direction response has an unsupported verdict")
    if payload.get("family_distinct_from_rejected_trend") is not True:
        raise StateError("direction review did not confirm a distinct family")
    if payload.get("preserved_trend_terminal") != (
        "btc-eth-vol-targeted-trend-v1=HISTORICAL_NO_GO_DEVELOPMENT/AUDIT_INCONCLUSIVE"
    ):
        raise StateError("direction review changed the terminal trend evidence")
    if (
        payload.get("holdout_opened") is not False
        or payload.get("holdout_values_read") is not False
        or payload.get("raw_market_data_inspected") is not False
        or payload.get("performance_claim_made") is not False
        or payload.get("capital_permitted") != 0
    ):
        raise StateError("direction review violated its no-data/no-performance scope")

    def string_list(key: str, *, nonempty: bool) -> list[str]:
        value = payload.get(key)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise StateError(f"direction review field {key} must be a string list")
        if nonempty and not value:
            raise StateError(f"direction review field {key} must be nonempty")
        return [str(item)[:1000] for item in value[:40]]

    return {
        "verdict": verdict,
        "family_distinct_from_rejected_trend": True,
        "preserved_trend_terminal": (
            "btc-eth-vol-targeted-trend-v1=HISTORICAL_NO_GO_DEVELOPMENT/AUDIT_INCONCLUSIVE"
        ),
        "holdout_opened": False,
        "holdout_values_read": False,
        "raw_market_data_inspected": False,
        "performance_claim_made": False,
        "capital_permitted": 0,
        "strengths": string_list("strengths", nonempty=True),
        "required_revisions": string_list("required_revisions", nonempty=False),
        "causal_timing_concerns": string_list("causal_timing_concerns", nonempty=False),
        "statistical_concerns": string_list("statistical_concerns", nonempty=False),
        "rationale": string_list("rationale", nonempty=True),
    }


def run_mean_reversion_direction_review(*, invocation_mode: InvocationMode) -> dict[str, Any]:
    """Obtain live research direction before freezing the distinct mean-reversion family."""

    state = load_json(STATE)
    validate_state(state)
    if (
        state.get("program_state") != "ACTIVE_RESEARCH"
        or state.get("current_experiment_id") != MEAN_REVERSION_EXPERIMENT_ID
        or state.get("next_task") != "run_mean_reversion_direction_review"
    ):
        raise StateError("mean-reversion direction review is not the current task")
    remaining_calls = int(state["budgets"].get("agent_calls_remaining", 0))
    if remaining_calls < 1:
        raise StateError("mean-reversion direction review has no remaining agent call")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before direction review")
    experiment_root = ROOT / "experiments" / MEAN_REVERSION_EXPERIMENT_ID
    draft = load_json(experiment_root / "PREREGISTRATION_DRAFT.json")
    if (
        draft.get("experiment_id") != MEAN_REVERSION_EXPERIMENT_ID
        or draft.get("status") != "DRAFT_NOT_FROZEN"
        or draft.get("holdout_opened") is not False
        or draft.get("returns_calculated") is not False
    ):
        raise StateError("mean-reversion draft is not reviewable")
    draft_hash = _sha(draft)
    prompt = (
        "Act as the Sol/xhigh research director for the bounded zero-capital experiment "
        "btc-eth-long-only-mean-reversion-v1. Read AGENTS.md, RESEARCH_PROTOCOL.md, "
        "ACCEPTANCE_GATES.yaml, CURRENT_STATE.json, REJECTED_STRATEGIES.jsonl, and only "
        "the new experiment PREREGISTRATION_DRAFT.json. Do not edit files, inspect raw "
        "market data, inspect any 2026 holdout file/value/footer, calculate returns, tune "
        "from prior returns, or make a performance claim. Review whether this is genuinely "
        "distinct from the rejected trend family and audit economic rationale, stateful "
        "entry/exit timing, holding-period definition, fold initialization, next-bar "
        "execution, self-financing costs, sparse-trade sufficiency, raw-drawdown baseline, "
        "variants, DSR, PBO, bootstrap, asset/regime/concentration gates, holdout rule, and "
        "prospective rule. Preserve btc-eth-vol-targeted-trend-v1="
        "HISTORICAL_NO_GO_DEVELOPMENT/AUDIT_INCONCLUSIVE. Return strict compact JSON only "
        "with verdict (PRE_FREEZE_READY or REVISION_REQUIRED), "
        "family_distinct_from_rejected_trend (true), preserved_trend_terminal (the exact "
        "string above), holdout_opened (false), holdout_values_read (false), "
        "raw_market_data_inspected (false), performance_claim_made (false), "
        "capital_permitted (0), strengths (nonempty string list), required_revisions "
        "(string list), causal_timing_concerns (string list), statistical_concerns "
        "(string list), and rationale (nonempty string list)."
    )
    invocation = invoke_codex(
        invocation_mode=invocation_mode,
        role="research_director",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt=prompt,
        timeout_seconds=300,
    )
    append_jsonl(
        MODEL_LEDGER,
        invocation.ledger_record(
            record_type="MODEL_INVOCATION",
            purpose="mean_reversion_preregistration_direction_review",
            experiment_id=MEAN_REVERSION_EXPERIMENT_ID,
            draft_sha256=draft_hash,
            model_generated_research_claim=model_generated_claim_permitted(invocation),
        ),
    )
    budgets = dict(state["budgets"])
    budgets["agent_calls_used"] = int(budgets.get("agent_calls_used", 0)) + 1
    budgets["agent_calls_remaining"] = remaining_calls - 1
    state["budgets"] = budgets
    state["updated_at_utc"] = invocation.ended_at_utc
    atomic_json(STATE, state)
    if not model_generated_claim_permitted(invocation) or invocation.final_message is None:
        return {
            "status": invocation.outcome,
            "invocation_mode": invocation.invocation_mode,
            "model_generated_research": False,
            "agent_calls_remaining": budgets["agent_calls_remaining"],
            "holdout_opened": False,
            "capital_permitted": 0,
        }
    try:
        review = sanitize_mean_reversion_direction_payload(json.loads(invocation.final_message))
    except (json.JSONDecodeError, StateError) as exc:
        append_jsonl(
            MODEL_LEDGER,
            {
                "record_type": "MODEL_INVOCATION_VALIDATION_FAILURE",
                "at_utc": _now(),
                "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
                "response_identifier": invocation.response_identifier,
                "invocation_mode": "live",
                "exact_error": str(exc),
                "model_generated_research_claim": False,
            },
        )
        raise StateError(f"mean-reversion direction response failed validation: {exc}") from exc
    review.update(
        {
            "schema_version": "1.0",
            "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
            "reviewed_at_utc": invocation.ended_at_utc,
            "reviewer_model": invocation.actual_model,
            "reasoning_level": invocation.reasoning_level,
            "invocation_mode": invocation.invocation_mode,
            "response_identifier": invocation.response_identifier,
            "model_result_sha256": invocation.result_sha256,
            "draft_sha256": draft_hash,
            "source_commit": git["head"],
        }
    )
    atomic_json(experiment_root / "DIRECTION_REVIEW.json", review)
    state.update(
        {
            "next_task": (
                "freeze_mean_reversion_preregistration"
                if review["verdict"] == "PRE_FREEZE_READY"
                else "revise_and_freeze_mean_reversion_preregistration"
            ),
            "updated_at_utc": invocation.ended_at_utc,
        }
    )
    atomic_json(STATE, state)
    return {
        "status": "LIVE_DIRECTION_REVIEW_COMPLETE",
        "verdict": review["verdict"],
        "invocation_mode": invocation.invocation_mode,
        "actual_model": invocation.actual_model,
        "reasoning_level": invocation.reasoning_level,
        "response_identifier": invocation.response_identifier,
        "agent_calls_remaining": budgets["agent_calls_remaining"],
        "holdout_opened": False,
        "capital_permitted": 0,
    }


def run_cycle(*, invocation_mode: InvocationMode, dry_run: bool) -> dict[str, Any]:
    state = load_json(STATE)
    validate_state(state)
    current_experiment = state.get("current_experiment_id")
    next_task = state.get("next_task")
    if dry_run:
        return {
            "dry_run": True,
            "experiment_id": current_experiment,
            "next_task": next_task,
            "state": state["program_state"],
            "codex_calls": 0,
        }
    if not is_active_program_state(state):
        return {
            "status": "PROGRAM_NOT_ACTIVE",
            "program_state": state["program_state"],
            "state_changed": False,
        }
    if not (
        current_experiment == ARCHIVE_EXPERIMENT_ID
        and next_task == "review_frozen_archive_preregistration"
    ):
        return {
            "status": "NO_AUTOMATED_STEP_REGISTERED",
            "experiment_id": current_experiment,
            "next_task": next_task,
            "state_changed": False,
            "model_invocations": 0,
        }
    git = git_state()
    if not git["clean"]:
        raise StateError("controller working tree must be clean before a cycle")
    prereg_path = preregistration_path()
    if not prereg_path.exists():
        raise StateError("freeze and commit preregistration before evaluating any data")
    prereg = load_json(prereg_path)
    if prereg.get("experiment_id") != ARCHIVE_EXPERIMENT_ID:
        raise StateError("preregistration identity mismatch")
    expected_hash = prereg.get("preregistration_sha256")
    hash_input = dict(prereg)
    hash_input.pop("preregistration_sha256", None)
    if expected_hash != _sha(hash_input):
        raise StateError("preregistration hash mismatch")
    prompt = (
        "Review the frozen preregistration for the archive-observed universe audit. "
        "Do not edit files, inspect a strategy holdout, calculate returns, or make a "
        "performance claim. Return a concise methodological checklist only."
    )
    invocation = invoke_codex(
        invocation_mode=invocation_mode,
        role="research_director",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt=prompt,
    )
    append_jsonl(
        MODEL_LEDGER,
        invocation.ledger_record(
            record_type="MODEL_INVOCATION",
            purpose="archive_universe_preregistration_review",
            experiment_id=ARCHIVE_EXPERIMENT_ID,
            model_generated_research_claim=model_generated_claim_permitted(invocation),
        ),
    )
    if not model_generated_claim_permitted(invocation):
        return {
            "status": invocation.outcome,
            "invocation_mode": invocation.invocation_mode,
            "model_generated_research": False,
        }
    state.update({"next_task": "implement_archive_enumerator", "updated_at_utc": _now()})
    atomic_json(STATE, state)
    return {
        "status": "LIVE_DIRECTION_REVIEW_COMPLETE",
        "invocation_mode": invocation.invocation_mode,
        "model_generated_research": True,
        "response_identifier": invocation.response_identifier,
    }


def run_bounded_cycles(*, invocation_mode: InvocationMode, cycles: int) -> dict[str, Any]:
    """Run at most ``cycles`` resumable steps and stop when no state transition occurs."""

    if cycles < 1 or cycles > 3:
        raise StateError("cycles must be 1..3")
    results: list[dict[str, Any]] = []
    changed_cycles = 0
    for _ in range(cycles):
        before = _sha(load_json(STATE))
        result = run_cycle(invocation_mode=invocation_mode, dry_run=False)
        after = _sha(load_json(STATE))
        changed = before != after
        result = {**result, "state_changed": changed}
        results.append(result)
        if changed:
            changed_cycles += 1
        if not changed:
            break
    return {
        "status": "BOUNDED_RESUME_COMPLETE",
        "cycles_requested": cycles,
        "cycles_attempted": len(results),
        "cycles_with_state_change": changed_cycles,
        "results": results,
    }


def run_trend_data_audit() -> dict[str, Any]:
    """Verify the frozen BTC/ETH source without parsing any holdout values."""

    state = load_json(STATE)
    if state.get("current_experiment_id") != TREND_EXPERIMENT_ID:
        raise StateError("trend data audit is not the active experiment")
    started = _now()
    monotonic_start = time.monotonic()
    source_repository = ROOT.parent / "crypto-direction-lab"
    report_path = ROOT / "experiments" / TREND_EXPERIMENT_ID / "DATA_CONTRACT.json"
    try:
        report = verify_trend_data(source_repository)
        report["invocation_mode"] = "deterministic_local"
        report["source_repository_head"] = subprocess.run(
            ["git", "-C", str(source_repository), "rev-parse", "HEAD"],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()
        atomic_json(report_path, report)
        outcome = "DATA_CONTRACT_PASS"
        exact_error = None
        result_hash = _sha(report)
    except (OSError, subprocess.SubprocessError, TrendDataError) as exc:
        outcome = "DATA_CONTRACT_FAILURE"
        exact_error = _bounded_error(f"{type(exc).__name__}: {exc}")
        result_hash = None
        report = {
            "status": "FAIL",
            "experiment_id": TREND_EXPERIMENT_ID,
            "invocation_mode": "deterministic_local",
            "holdout_opened": False,
            "returns_calculated": False,
            "performance_claim_made": False,
            "capital_permitted": 0,
            "exact_error": exact_error,
        }
        atomic_json(report_path, report)
    ended = _now()
    append_jsonl(
        MODEL_LEDGER,
        {
            "record_type": "LOCAL_PIPELINE_INVOCATION",
            "experiment_id": TREND_EXPERIMENT_ID,
            "role": "trend_data_contract",
            "requested_model": None,
            "actual_model": None,
            "reasoning_level": None,
            "invocation_mode": "deterministic_local",
            "started_at_utc": started,
            "ended_at_utc": ended,
            "duration_seconds": round(time.monotonic() - monotonic_start, 6),
            "outcome": outcome,
            "model_result_received": False,
            "model_generated_research_claim": False,
            "result_sha256": result_hash,
            "exact_error": exact_error,
            "fallback": None,
        },
    )
    return {
        "status": outcome,
        "report": str(report_path.relative_to(ROOT)),
        "result_sha256": result_hash,
        "holdout_opened": False,
        "returns_calculated": False,
        "capital_permitted": 0,
    }


def freeze_trend_preregistration() -> dict[str, Any]:
    """Freeze the reviewed trend contract before any development return is calculated."""

    state = load_json(STATE)
    if (
        state.get("current_experiment_id") != TREND_EXPERIMENT_ID
        or state.get("data_contract_status") != "PASS"
        or state.get("next_task") != "freeze_trend_preregistration"
    ):
        raise StateError("trend preregistration is not at its freeze gate")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before trend freeze")
    experiment_root = ROOT / "experiments" / TREND_EXPERIMENT_ID
    draft = load_json(experiment_root / "PREREGISTRATION_DRAFT.json")
    review = load_json(experiment_root / "DIRECTION_REVIEW.json")
    data_contract = load_json(experiment_root / "DATA_CONTRACT.json")
    if draft.get("status") != "DRAFT_NOT_FROZEN":
        raise StateError("trend draft status mismatch")
    if review.get("verdict") != "REVISION_REQUIRED" or review.get("holdout_opened") is not False:
        raise StateError("trend direction review evidence mismatch")
    if (
        data_contract.get("status") != "PASS"
        or data_contract.get("holdout_opened") is not False
        or data_contract.get("returns_calculated") is not False
    ):
        raise StateError("trend data contract is not holdout-safe PASS")
    frozen = dict(draft)
    frozen.update(
        {
            "schema_version": "1.0",
            "status": "FROZEN",
            "preregistered_at_utc": _now(),
            "source_commit": git["head"],
            "reviewed_draft_sha256": review.get("draft_sha256"),
            "revised_draft_sha256": _sha(draft),
            "direction_review_result_sha256": review.get("result_sha256"),
            "data_contract_result_sha256": _sha(data_contract),
            "holdout_opened_at_freeze": False,
            "returns_calculated_at_freeze": False,
        }
    )
    frozen["preregistration_sha256"] = _sha(frozen)
    path = experiment_root / "PREREGISTRATION.json"
    if path.exists():
        raise StateError("trend preregistration is already frozen")
    atomic_json(path, frozen)
    state.update(
        {
            "next_task": "implement_trend_development_pipeline",
            "updated_at_utc": frozen["preregistered_at_utc"],
        }
    )
    atomic_json(STATE, state)
    append_jsonl(
        LEDGER,
        {
            "record_type": "PREREGISTRATION_FROZEN",
            "experiment_id": TREND_EXPERIMENT_ID,
            "at_utc": frozen["preregistered_at_utc"],
            "source_commit": git["head"],
            "preregistration_sha256": frozen["preregistration_sha256"],
            "data_contract_result_sha256": frozen["data_contract_result_sha256"],
            "holdout_opened": False,
            "returns_calculated": False,
            "capital_permitted": 0,
        },
    )
    return {
        "status": "FROZEN",
        "experiment_id": TREND_EXPERIMENT_ID,
        "path": str(path.relative_to(ROOT)),
        "source_commit": git["head"],
        "preregistration_sha256": frozen["preregistration_sha256"],
        "holdout_opened": False,
        "returns_calculated": False,
        "capital_permitted": 0,
    }


def run_trend_development() -> dict[str, Any]:
    """Run the frozen development stage without reading any 2026 partition values."""

    state = load_json(STATE)
    if (
        state.get("current_experiment_id") != TREND_EXPERIMENT_ID
        or state.get("data_contract_status") != "PASS"
        or state.get("next_task") != "run_trend_development_evaluation"
    ):
        raise StateError("trend experiment is not at the development evaluation gate")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before development evaluation")
    experiment_root = ROOT / "experiments" / TREND_EXPERIMENT_ID
    prereg = load_json(experiment_root / "PREREGISTRATION.json")
    expected_hash = prereg.get("preregistration_sha256")
    hash_input = dict(prereg)
    hash_input.pop("preregistration_sha256", None)
    if (
        prereg.get("status") != "FROZEN"
        or prereg.get("experiment_id") != TREND_EXPERIMENT_ID
        or expected_hash != _sha(hash_input)
    ):
        raise StateError("frozen trend preregistration identity or hash mismatch")
    data_contract = load_json(experiment_root / "DATA_CONTRACT.json")
    if (
        data_contract.get("status") != "PASS"
        or data_contract.get("holdout_parquet_footers_or_values_read") is not False
    ):
        raise StateError("trend data contract is not holdout-safe PASS")
    started = _now()
    monotonic_start = time.monotonic()
    report_path = experiment_root / "DEVELOPMENT_RESULT.json"
    try:
        market = load_development_market(ROOT.parent / "crypto-direction-lab", data_contract)
        report = evaluate_trend_development(market, prereg)
        duration = round(time.monotonic() - monotonic_start, 6)
        if duration > float(state["budgets"]["max_wall_seconds"]):
            raise StateError("trend development evaluation exceeded frozen wall-clock budget")
        report.update(
            {
                "started_at_utc": started,
                "ended_at_utc": _now(),
                "duration_seconds": duration,
                "invocation_mode": "deterministic_local",
                "source_commit": git["head"],
                "preregistration_sha256": expected_hash,
                "data_contract_result_sha256": _sha(data_contract),
                "returns_calculated": True,
                "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
            }
        )
        atomic_json(report_path, report)
        outcome = str(report["classification"])
        exact_error = None
        result_hash = _sha(report)
    except (ImportError, OSError, StateError, TrendDataError, ValueError) as exc:
        duration = round(time.monotonic() - monotonic_start, 6)
        outcome = "DEVELOPMENT_PIPELINE_FAILURE"
        exact_error = _bounded_error(f"{type(exc).__name__}: {exc}")
        result_hash = None
        report = {
            "schema_version": "1.0",
            "experiment_id": TREND_EXPERIMENT_ID,
            "stage": "DEVELOPMENT",
            "classification": outcome,
            "started_at_utc": started,
            "ended_at_utc": _now(),
            "duration_seconds": duration,
            "invocation_mode": "deterministic_local",
            "holdout_values_read": False,
            "holdout_opened": False,
            "returns_calculated": False,
            "capital_permitted": 0,
            "exact_error": exact_error,
        }
        atomic_json(report_path, report)
    append_jsonl(
        MODEL_LEDGER,
        {
            "record_type": "LOCAL_PIPELINE_INVOCATION",
            "experiment_id": TREND_EXPERIMENT_ID,
            "role": "trend_development_evaluator",
            "requested_model": None,
            "actual_model": None,
            "reasoning_level": None,
            "invocation_mode": "deterministic_local",
            "started_at_utc": started,
            "ended_at_utc": report["ended_at_utc"],
            "duration_seconds": report["duration_seconds"],
            "outcome": outcome,
            "model_result_received": False,
            "model_generated_research_claim": False,
            "result_sha256": result_hash,
            "exact_error": exact_error,
            "fallback": None,
            "holdout_opened": False,
        },
    )
    if outcome in {"DEVELOPMENT_GO", "HISTORICAL_NO_GO"}:
        state.update(
            {
                "development_status": outcome,
                "next_task": (
                    "run_trend_pre_holdout_independent_audit"
                    if outcome == "DEVELOPMENT_GO"
                    else "run_trend_terminal_independent_audit"
                ),
                "updated_at_utc": report["ended_at_utc"],
            }
        )
        budgets = dict(state["budgets"])
        budgets["cycles_remaining"] = 0
        state["budgets"] = budgets
        atomic_json(STATE, state)
    return {
        "status": outcome,
        "report": str(report_path.relative_to(ROOT)),
        "result_sha256": result_hash,
        "holdout_opened": False,
        "returns_calculated": report["returns_calculated"],
        "capital_permitted": 0,
    }


def run_mean_reversion_development() -> dict[str, Any]:
    """Run one frozen development evaluation without reading any 2026 value or footer."""

    state = load_json(STATE)
    if (
        state.get("program_state") != "ACTIVE_RESEARCH"
        or state.get("current_experiment_id") != MEAN_REVERSION_EXPERIMENT_ID
        or state.get("data_contract_status") != "PASS_REUSED_HOLDOUT_CLOSED"
        or state.get("preregistration_status") != "FROZEN"
        or state.get("next_task") != "run_mean_reversion_development_evaluation"
    ):
        raise StateError("mean-reversion experiment is not at the development gate")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before development evaluation")
    experiment_root = ROOT / "experiments" / MEAN_REVERSION_EXPERIMENT_ID
    prereg = load_json(experiment_root / "PREREGISTRATION.json")
    expected_hash = prereg.get("preregistration_sha256")
    hash_input = dict(prereg)
    hash_input.pop("preregistration_sha256", None)
    if (
        prereg.get("status") != "FROZEN"
        or prereg.get("experiment_id") != MEAN_REVERSION_EXPERIMENT_ID
        or expected_hash != _sha(hash_input)
        or prereg.get("holdout_opened") is not False
        or prereg.get("holdout_values_read") is not False
        or prereg.get("returns_calculated") is not False
    ):
        raise StateError("frozen mean-reversion preregistration identity or hash mismatch")
    data_contract = load_json(ROOT / "experiments" / TREND_EXPERIMENT_ID / "DATA_CONTRACT.json")
    if (
        _sha(data_contract)
        != prereg.get("data_contract", {}).get("reused_verified_contract_sha256")
        or data_contract.get("status") != "PASS"
        or data_contract.get("holdout_parquet_footers_or_values_read") is not False
    ):
        raise StateError("reused data contract is not an exact holdout-safe PASS")
    started = _now()
    monotonic_start = time.monotonic()
    report_path = experiment_root / "DEVELOPMENT_RESULT.json"
    partial_calculations_discarded = False
    try:
        market = load_development_market(ROOT.parent / "crypto-direction-lab", data_contract)
        partial_calculations_discarded = True
        report = evaluate_mean_reversion_development(market, prereg)
        duration = round(time.monotonic() - monotonic_start, 6)
        if duration > float(state["budgets"]["max_wall_seconds"]):
            raise StateError("mean-reversion development exceeded frozen wall-clock budget")
        report.update(
            {
                "started_at_utc": started,
                "ended_at_utc": _now(),
                "duration_seconds": duration,
                "invocation_mode": "deterministic_local",
                "source_commit": git["head"],
                "preregistration_sha256": expected_hash,
                "data_contract_result_sha256": _sha(data_contract),
                "returns_calculated": True,
                "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
            }
        )
        atomic_json(report_path, report)
        outcome = str(report["classification"])
        exact_error = None
        result_hash = _sha(report)
    except (ImportError, OSError, StateError, TrendDataError, ValueError) as exc:
        duration = round(time.monotonic() - monotonic_start, 6)
        outcome = "DEVELOPMENT_PIPELINE_FAILURE"
        exact_error = _bounded_error(f"{type(exc).__name__}: {exc}")
        result_hash = None
        report = {
            "schema_version": "1.0",
            "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
            "stage": "DEVELOPMENT",
            "classification": outcome,
            "started_at_utc": started,
            "ended_at_utc": _now(),
            "duration_seconds": duration,
            "invocation_mode": "deterministic_local",
            "holdout_values_read": False,
            "holdout_opened": False,
            "candidate_promoted": False,
            "returns_calculation_completed": False,
            "partial_calculations_discarded": partial_calculations_discarded,
            "capital_permitted": 0,
            "exact_error": exact_error,
        }
        atomic_json(report_path, report)
    append_jsonl(
        MODEL_LEDGER,
        {
            "record_type": "LOCAL_PIPELINE_INVOCATION",
            "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
            "role": "mean_reversion_development_evaluator",
            "requested_model": None,
            "actual_model": None,
            "reasoning_level": None,
            "invocation_mode": "deterministic_local",
            "started_at_utc": started,
            "ended_at_utc": report["ended_at_utc"],
            "duration_seconds": report["duration_seconds"],
            "outcome": outcome,
            "model_result_received": False,
            "model_generated_research_claim": False,
            "result_sha256": result_hash,
            "exact_error": exact_error,
            "fallback": None,
            "holdout_opened": False,
        },
    )
    if outcome in {"DEVELOPMENT_GO", "HISTORICAL_NO_GO"}:
        state.update(
            {
                "development_status": outcome,
                "next_task": (
                    "run_mean_reversion_pre_holdout_independent_audit"
                    if outcome == "DEVELOPMENT_GO"
                    else "run_mean_reversion_terminal_independent_audit"
                ),
                "updated_at_utc": report["ended_at_utc"],
            }
        )
        budgets = dict(state["budgets"])
        budgets["cycles_remaining"] = 0
        state["budgets"] = budgets
        atomic_json(STATE, state)
    return {
        "status": outcome,
        "report": str(report_path.relative_to(ROOT)),
        "result_sha256": result_hash,
        "holdout_opened": False,
        "returns_calculated": report.get("returns_calculated", False),
        "capital_permitted": 0,
    }


def run_relative_value_development() -> dict[str, Any]:
    """Run the frozen relative-value development stage with the holdout closed."""

    state = load_json(STATE)
    if (
        state.get("program_state") != "ACTIVE_RESEARCH"
        or state.get("current_experiment_id") != RELATIVE_VALUE_EXPERIMENT_ID
        or state.get("data_contract_status") != "PASS_REUSED_HOLDOUT_CLOSED"
        or state.get("preregistration_status") != "FROZEN"
        or state.get("next_task") != "run_relative_value_development_evaluation"
    ):
        raise StateError("relative-value experiment is not at the development gate")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before development evaluation")
    experiment_root = ROOT / "experiments" / RELATIVE_VALUE_EXPERIMENT_ID
    prereg = load_json(experiment_root / "PREREGISTRATION.json")
    expected_hash = prereg.get("preregistration_sha256")
    hash_input = dict(prereg)
    hash_input.pop("preregistration_sha256", None)
    prereg_data = prereg.get("data_contract")
    if (
        prereg.get("status") != "FROZEN"
        or prereg.get("experiment_id") != RELATIVE_VALUE_EXPERIMENT_ID
        or expected_hash != _sha(hash_input)
        or prereg.get("holdout_opened") is not False
        or prereg.get("returns_calculated") is not False
        or not isinstance(prereg_data, dict)
        or prereg_data.get("holdout_parquet_footers_or_values_read") is not False
    ):
        raise StateError("frozen relative-value preregistration identity or hash mismatch")
    data_contract = load_json(ROOT / "experiments" / TREND_EXPERIMENT_ID / "DATA_CONTRACT.json")
    if (
        _sha(data_contract) != prereg_data.get("reused_verified_contract_sha256")
        or data_contract.get("status") != "PASS"
        or data_contract.get("holdout_parquet_footers_or_values_read") is not False
    ):
        raise StateError("reused data contract is not an exact holdout-safe PASS")
    started = _now()
    monotonic_start = time.monotonic()
    report_path = experiment_root / "DEVELOPMENT_RESULT.json"
    partial_calculations_discarded = False
    try:
        market = load_development_market(ROOT.parent / "crypto-direction-lab", data_contract)
        partial_calculations_discarded = True
        report = evaluate_relative_value_development(market, prereg)
        duration = round(time.monotonic() - monotonic_start, 6)
        if duration > float(state["budgets"]["max_wall_seconds"]):
            raise StateError("relative-value development exceeded frozen wall-clock budget")
        report.update(
            {
                "started_at_utc": started,
                "ended_at_utc": _now(),
                "duration_seconds": duration,
                "invocation_mode": "deterministic_local",
                "source_commit": git["head"],
                "preregistration_sha256": expected_hash,
                "data_contract_result_sha256": _sha(data_contract),
                "returns_calculated": True,
                "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
            }
        )
        atomic_json(report_path, report)
        outcome = str(report["classification"])
        exact_error = None
        result_hash = _sha(report)
    except (ImportError, OSError, StateError, TrendDataError, ValueError) as exc:
        duration = round(time.monotonic() - monotonic_start, 6)
        outcome = "DEVELOPMENT_PIPELINE_FAILURE"
        exact_error = _bounded_error(f"{type(exc).__name__}: {exc}")
        result_hash = None
        report = {
            "schema_version": "1.0",
            "experiment_id": RELATIVE_VALUE_EXPERIMENT_ID,
            "stage": "DEVELOPMENT",
            "classification": outcome,
            "started_at_utc": started,
            "ended_at_utc": _now(),
            "duration_seconds": duration,
            "invocation_mode": "deterministic_local",
            "holdout_values_read": False,
            "holdout_opened": False,
            "candidate_promoted": False,
            "returns_calculation_completed": False,
            "partial_calculations_discarded": partial_calculations_discarded,
            "capital_permitted": 0,
            "exact_error": exact_error,
        }
        atomic_json(report_path, report)
    append_jsonl(
        MODEL_LEDGER,
        {
            "record_type": "LOCAL_PIPELINE_INVOCATION",
            "experiment_id": RELATIVE_VALUE_EXPERIMENT_ID,
            "role": "relative_value_development_evaluator",
            "requested_model": None,
            "actual_model": None,
            "reasoning_level": None,
            "invocation_mode": "deterministic_local",
            "started_at_utc": started,
            "ended_at_utc": report["ended_at_utc"],
            "duration_seconds": report["duration_seconds"],
            "outcome": outcome,
            "model_result_received": False,
            "model_generated_research_claim": False,
            "result_sha256": result_hash,
            "exact_error": exact_error,
            "fallback": None,
            "holdout_opened": False,
        },
    )
    if outcome in {"DEVELOPMENT_GO", "HISTORICAL_NO_GO"}:
        state.update(
            {
                "development_status": outcome,
                "next_task": (
                    "run_relative_value_pre_holdout_independent_audit"
                    if outcome == "DEVELOPMENT_GO"
                    else "run_relative_value_terminal_independent_audit"
                ),
                "updated_at_utc": report["ended_at_utc"],
            }
        )
        budgets = dict(state["budgets"])
        budgets["cycles_remaining"] = 0
        state["budgets"] = budgets
        atomic_json(STATE, state)
    return {
        "status": outcome,
        "report": str(report_path.relative_to(ROOT)),
        "result_sha256": result_hash,
        "holdout_opened": False,
        "returns_calculated": report.get("returns_calculated", False),
        "capital_permitted": 0,
    }


def run_calendar_development() -> dict[str, Any]:
    """Run the single frozen 2025 calendar evaluation with 2026 still unopened."""

    state = load_json(STATE)
    if (
        state.get("program_state") != "ACTIVE_RESEARCH"
        or state.get("current_experiment_id") != CALENDAR_EXPERIMENT_ID
        or state.get("data_contract_status") != "FROZEN_REUSED_FIXED_PAIR_CONTRACT_PASS"
        or state.get("implementation_status") != "PASS_PRE_DATA"
        or state.get("next_task") != "run_one_shot_calendar_development_after_implementation_commit"
    ):
        raise StateError("calendar experiment is not at the development gate")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before calendar evaluation")
    experiment_root = ROOT / "experiments" / CALENDAR_EXPERIMENT_ID
    wrapper = load_json(experiment_root / "PREREGISTRATION.json")
    effective_path = experiment_root / "PREREGISTRATION_REVISED_DRAFT.json"
    effective_bytes = effective_path.read_bytes()
    effective = load_json(effective_path)
    try:
        verify_calendar_preregistration(
            wrapper,
            effective,
            effective_bytes=effective_bytes,
        )
    except CalendarPipelineError as exc:
        raise StateError("frozen calendar preregistration hash mismatch") from exc
    data_contract = load_json(ROOT / "experiments" / TREND_EXPERIMENT_ID / "DATA_CONTRACT.json")
    started = _now()
    monotonic_start = time.monotonic()
    report_path = experiment_root / "DEVELOPMENT_RESULT.json"
    partial_calculations_discarded = False
    try:
        market = load_calendar_development_market(
            ROOT.parent / "crypto-direction-lab",
            data_contract,
        )
        partial_calculations_discarded = True
        report = evaluate_calendar_development(market, effective, ROOT / "experiments")
        duration = round(time.monotonic() - monotonic_start, 6)
        if duration > float(state["budgets"]["max_wall_seconds"]):
            raise StateError("calendar development exceeded frozen wall-clock budget")
        report.update(
            {
                "started_at_utc": started,
                "ended_at_utc": _now(),
                "duration_seconds": duration,
                "invocation_mode": "deterministic_local",
                "source_commit": git["head"],
                "preregistration_sha256": wrapper["preregistration_sha256"],
                "effective_contract_sha256": wrapper["effective_contract"]["canonical_sha256"],
                "data_contract_result_sha256": _sha(data_contract),
                "returns_calculated": True,
                "performance_claim_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
            }
        )
        atomic_json(report_path, report)
        outcome = str(report["classification"])
        exact_error = None
        result_hash = _sha(report)
    except (
        CalendarEvaluationError,
        CalendarPipelineError,
        ImportError,
        OSError,
        StateError,
        ValueError,
    ) as exc:
        duration = round(time.monotonic() - monotonic_start, 6)
        outcome = "DEVELOPMENT_PIPELINE_FAILURE"
        exact_error = _bounded_error(f"{type(exc).__name__}: {exc}")
        result_hash = None
        report = {
            "schema_version": "1.0",
            "experiment_id": CALENDAR_EXPERIMENT_ID,
            "stage": "DEVELOPMENT",
            "classification": outcome,
            "started_at_utc": started,
            "ended_at_utc": _now(),
            "duration_seconds": duration,
            "invocation_mode": "deterministic_local",
            "holdout_values_read": False,
            "holdout_opened": False,
            "candidate_promoted": False,
            "returns_calculation_completed": False,
            "partial_calculations_discarded": partial_calculations_discarded,
            "capital_permitted": 0,
            "exact_error": exact_error,
        }
        atomic_json(report_path, report)
    append_jsonl(
        MODEL_LEDGER,
        {
            "record_type": "LOCAL_PIPELINE_INVOCATION",
            "experiment_id": CALENDAR_EXPERIMENT_ID,
            "role": "calendar_development_evaluator",
            "requested_model": None,
            "actual_model": None,
            "reasoning_level": None,
            "invocation_mode": "deterministic_local",
            "started_at_utc": started,
            "ended_at_utc": report["ended_at_utc"],
            "duration_seconds": report["duration_seconds"],
            "outcome": outcome,
            "model_result_received": False,
            "model_generated_research_claim": False,
            "result_sha256": result_hash,
            "exact_error": exact_error,
            "fallback": None,
            "holdout_opened": False,
        },
    )
    if outcome in {"DEVELOPMENT_GO", "HISTORICAL_NO_GO"}:
        state.update(
            {
                "development_status": outcome,
                "next_task": (
                    "run_calendar_pre_holdout_independent_audit"
                    if outcome == "DEVELOPMENT_GO"
                    else "run_calendar_terminal_independent_audit"
                ),
                "updated_at_utc": report["ended_at_utc"],
            }
        )
        budgets = dict(state["budgets"])
        budgets["cycles_remaining"] = 0
        state["budgets"] = budgets
        atomic_json(STATE, state)
    return {
        "status": outcome,
        "report": str(report_path.relative_to(ROOT)),
        "result_sha256": result_hash,
        "holdout_opened": False,
        "returns_calculated": report.get("returns_calculated", False),
        "capital_permitted": 0,
    }


def run_volatility_parity_development() -> dict[str, Any]:
    """Run the sole frozen 2024--2025 volatility-parity development evaluation."""

    state = load_json(STATE)
    if (
        state.get("program_state") != "ACTIVE_RESEARCH"
        or state.get("current_experiment_id") != VOLATILITY_PARITY_EXPERIMENT_ID
        or state.get("data_contract_status") != "FROZEN_REUSED_FIXED_PAIR_CONTRACT_PASS"
        or state.get("implementation_status") != "PASS_REPAIRED_PRODUCTION_PRE_DATA"
        or state.get("next_task") != "run_single_bounded_volatility_parity_development_evaluation"
        or state.get("repair_status") != "USED_PRE_DATA_IMPLEMENTATION_VALIDATION"
        or int(state["budgets"].get("cycles_remaining", 0)) != 1
        or int(state["budgets"].get("repair_attempts_remaining", 0)) != 0
    ):
        raise StateError("volatility-parity experiment is not at the development gate")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before development evaluation")
    implementation_commit = state.get("implementation_evidence_commit")
    artifact_hashes = state.get("implementation_artifact_sha256")
    required_artifacts = {
        "src/strategy_control/orchestrator.py",
        "src/strategy_control/volatility_parity.py",
        "src/strategy_control/volatility_parity_evaluator.py",
        "tests/test_volatility_parity_evaluator.py",
    }
    if (
        not isinstance(implementation_commit, str)
        or not implementation_commit
        or not isinstance(artifact_hashes, dict)
        or set(artifact_hashes) != required_artifacts
    ):
        raise StateError("development evaluator evidence binding is missing")
    for relative, expected_hash in artifact_hashes.items():
        path = ROOT / relative
        if (
            not isinstance(expected_hash, str)
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash
        ):
            raise StateError(f"development evaluator artifact hash mismatch: {relative}")

    experiment_root = ROOT / "experiments" / VOLATILITY_PARITY_EXPERIMENT_ID
    wrapper_path = experiment_root / "PREREGISTRATION.json"
    effective_path = experiment_root / "PREREGISTRATION_REVISED_DRAFT.json"
    wrapper = load_json(wrapper_path)
    effective = load_json(effective_path)
    try:
        verify_volatility_parity_frozen_contract(wrapper, effective, effective_path.read_bytes())
    except VolatilityParityError as exc:
        raise StateError("frozen volatility-parity contract hash mismatch") from exc
    data_contract = load_json(ROOT / "experiments" / TREND_EXPERIMENT_ID / "DATA_CONTRACT.json")
    if _sha(data_contract) != effective["data_contract"]["reused_verified_contract_sha256"]:
        raise StateError("reused volatility-parity data contract hash mismatch")

    started = _now()
    monotonic_start = time.monotonic()
    report_path = experiment_root / "DEVELOPMENT_RESULT.json"
    partial_calculations_discarded = False
    try:
        market = load_volatility_parity_development_market(
            ROOT.parent / "crypto-direction-lab", data_contract
        )
        partial_calculations_discarded = True
        report = evaluate_volatility_parity_development(market, effective, ROOT / "experiments")
        duration = round(time.monotonic() - monotonic_start, 6)
        if duration > float(state["budgets"]["max_wall_seconds"]):
            raise StateError("volatility-parity development exceeded frozen wall budget")
        report.update(
            {
                "started_at_utc": started,
                "ended_at_utc": _now(),
                "duration_seconds": duration,
                "invocation_mode": "deterministic_local",
                "source_commit": git["head"],
                "preregistration_sha256": wrapper["preregistration_sha256"],
                "effective_contract_sha256": wrapper["effective_contract"]["canonical_sha256"],
                "data_contract_result_sha256": _sha(data_contract),
            }
        )
        atomic_json(report_path, report)
        outcome = str(report["classification"])
        exact_error = None
        result_hash = _sha(report)
    except (
        ImportError,
        OSError,
        StateError,
        ValueError,
        VolatilityParityError,
    ) as exc:
        duration = round(time.monotonic() - monotonic_start, 6)
        outcome = "DEVELOPMENT_PIPELINE_FAILURE"
        exact_error = _bounded_error(f"{type(exc).__name__}: {exc}")
        result_hash = None
        report = {
            "schema_version": "1.0",
            "experiment_id": VOLATILITY_PARITY_EXPERIMENT_ID,
            "stage": "DEVELOPMENT",
            "classification": outcome,
            "started_at_utc": started,
            "ended_at_utc": _now(),
            "duration_seconds": duration,
            "invocation_mode": "deterministic_local",
            "holdout_values_read": False,
            "holdout_opened": False,
            "candidate_promoted": False,
            "returns_calculated": False,
            "partial_calculations_discarded": partial_calculations_discarded,
            "capital_permitted": 0,
            "exact_error": exact_error,
        }
        atomic_json(report_path, report)
    append_jsonl(
        MODEL_LEDGER,
        {
            "record_type": "LOCAL_PIPELINE_INVOCATION",
            "experiment_id": VOLATILITY_PARITY_EXPERIMENT_ID,
            "role": "volatility_parity_development_evaluator",
            "requested_model": None,
            "actual_model": None,
            "reasoning_level": None,
            "invocation_mode": "deterministic_local",
            "started_at_utc": started,
            "ended_at_utc": report["ended_at_utc"],
            "duration_seconds": report["duration_seconds"],
            "outcome": outcome,
            "model_result_received": False,
            "model_generated_research_claim": False,
            "result_sha256": result_hash,
            "exact_error": exact_error,
            "fallback": None,
            "holdout_opened": False,
        },
    )
    state.update(
        {
            "development_status": outcome,
            "next_task": (
                "run_volatility_parity_pre_holdout_independent_audit"
                if outcome == "DEVELOPMENT_GO"
                else "run_volatility_parity_terminal_independent_audit"
            ),
            "updated_at_utc": report["ended_at_utc"],
        }
    )
    budgets = dict(state["budgets"])
    budgets["cycles_remaining"] = 0
    state["budgets"] = budgets
    atomic_json(STATE, state)
    return {
        "status": outcome,
        "report": str(report_path.relative_to(ROOT)),
        "result_sha256": result_hash,
        "holdout_opened": False,
        "returns_calculated": report.get("returns_calculated", False),
        "capital_permitted": 0,
    }


def run_volatility_managed_development() -> dict[str, Any]:
    """Run the sole frozen 2024--2025 equal-sleeve development evaluation."""

    state = load_json(STATE)
    if (
        state.get("program_state") != "ACTIVE_RESEARCH"
        or state.get("current_experiment_id") != VOLATILITY_MANAGED_EXPERIMENT_ID
        or state.get("data_contract_status")
        != "FROZEN_REUSED_FIXED_PAIR_CONTRACT_PRODUCTION_VERIFIED"
        or state.get("implementation_status") != "PASS_REPAIRED_PRODUCTION_PRE_DATA"
        or state.get("next_task")
        != "run_single_bounded_volatility_managed_development_evaluation"
        or int(state["budgets"].get("cycles_remaining", 0)) != 1
        or int(state["budgets"].get("repair_attempts_remaining", 0)) != 0
    ):
        raise StateError("volatility-managed experiment is not at the development gate")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before development evaluation")
    artifact_hashes = state.get("implementation_artifact_sha256")
    required_artifacts = {
        "src/strategy_control/orchestrator.py",
        "src/strategy_control/volatility_managed.py",
        "src/strategy_control/volatility_managed_evaluator.py",
        "tests/test_volatility_managed.py",
        "tests/test_volatility_managed_evaluator.py",
    }
    if not isinstance(artifact_hashes, dict) or set(artifact_hashes) != required_artifacts:
        raise StateError("volatility-managed evaluator evidence binding is missing")
    for relative, expected_hash in artifact_hashes.items():
        if (
            not isinstance(expected_hash, str)
            or hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() != expected_hash
        ):
            raise StateError(f"volatility-managed evaluator hash mismatch: {relative}")

    experiment_root = ROOT / "experiments" / VOLATILITY_MANAGED_EXPERIMENT_ID
    wrapper_path = experiment_root / "PREREGISTRATION.json"
    effective_path = experiment_root / "PREREGISTRATION_REVISED_DRAFT.json"
    wrapper = load_json(wrapper_path)
    effective = load_json(effective_path)
    try:
        verify_volatility_managed_frozen_contract(
            wrapper, effective, effective_path.read_bytes()
        )
    except VolatilityManagedError as exc:
        raise StateError("frozen volatility-managed contract hash mismatch") from exc
    data_contract = load_json(ROOT / "experiments" / TREND_EXPERIMENT_ID / "DATA_CONTRACT.json")
    source_repository = ROOT.parent / "crypto-direction-lab"
    source_result = subprocess.run(
        ["git", "-C", str(source_repository), "rev-parse", "HEAD"],
        text=True,
        capture_output=True,
        check=False,
    )
    source_commit = source_result.stdout.strip()
    if source_result.returncode != 0 or len(source_commit) != 40:
        raise StateError("source repository HEAD is unavailable")

    started = _now()
    monotonic_start = time.monotonic()
    report_path = experiment_root / "DEVELOPMENT_RESULT.json"
    partial_calculations_discarded = False
    try:
        market = load_volatility_managed_development_market(
            source_repository, effective, data_contract
        )
        partial_calculations_discarded = True
        report = evaluate_volatility_managed_development(
            market, effective, ROOT / "experiments"
        )
        duration = round(time.monotonic() - monotonic_start, 6)
        if duration > float(state["budgets"]["max_wall_seconds"]):
            raise StateError("volatility-managed development exceeded frozen wall budget")
        report.update(
            {
                "started_at_utc": started,
                "ended_at_utc": _now(),
                "duration_seconds": duration,
                "invocation_mode": "deterministic_local",
                "controller_source_commit": git["head"],
                "source_commit": market.source_commit,
                "source_repository_head": source_commit,
                "preregistration_sha256": wrapper["preregistration_sha256"],
                "effective_contract_sha256": wrapper["effective_contract"][
                    "canonical_sha256"
                ],
                "data_contract_result_sha256": _sha(data_contract),
            }
        )
        atomic_json(report_path, report)
        outcome = str(report["classification"])
        exact_error = None
        result_hash = _sha(report)
    except (ImportError, OSError, StateError, ValueError, VolatilityManagedError) as exc:
        duration = round(time.monotonic() - monotonic_start, 6)
        outcome = "DEVELOPMENT_PIPELINE_FAILURE"
        exact_error = _bounded_error(f"{type(exc).__name__}: {exc}")
        result_hash = None
        report = {
            "schema_version": "1.0",
            "experiment_id": VOLATILITY_MANAGED_EXPERIMENT_ID,
            "stage": "DEVELOPMENT",
            "classification": outcome,
            "started_at_utc": started,
            "ended_at_utc": _now(),
            "duration_seconds": duration,
            "invocation_mode": "deterministic_local",
            "holdout_values_read": False,
            "holdout_opened": False,
            "candidate_promoted": False,
            "returns_calculated": False,
            "partial_calculations_discarded": partial_calculations_discarded,
            "capital_permitted": 0,
            "exact_error": exact_error,
        }
        atomic_json(report_path, report)
    append_jsonl(
        MODEL_LEDGER,
        {
            "record_type": "LOCAL_PIPELINE_INVOCATION",
            "experiment_id": VOLATILITY_MANAGED_EXPERIMENT_ID,
            "role": "volatility_managed_development_evaluator",
            "requested_model": None,
            "actual_model": None,
            "reasoning_level": None,
            "invocation_mode": "deterministic_local",
            "started_at_utc": started,
            "ended_at_utc": report["ended_at_utc"],
            "duration_seconds": report["duration_seconds"],
            "outcome": outcome,
            "model_result_received": False,
            "model_generated_research_claim": False,
            "result_sha256": result_hash,
            "exact_error": exact_error,
            "fallback": None,
            "holdout_opened": False,
        },
    )
    state.update(
        {
            "development_status": outcome,
            "next_task": (
                "run_volatility_managed_pre_holdout_independent_audit"
                if outcome == "DEVELOPMENT_GO"
                else "run_volatility_managed_terminal_independent_audit"
            ),
            "updated_at_utc": report["ended_at_utc"],
        }
    )
    budgets = dict(state["budgets"])
    budgets["cycles_remaining"] = 0
    state["budgets"] = budgets
    atomic_json(STATE, state)
    return {
        "status": outcome,
        "report": str(report_path.relative_to(ROOT)),
        "result_sha256": result_hash,
        "holdout_opened": False,
        "returns_calculated": report.get("returns_calculated", False),
        "capital_permitted": 0,
    }


def sanitize_trend_audit_payload(payload: object, *, result_sha256: str) -> dict[str, Any]:
    """Allowlist a terminal trend audit and preserve the closed-holdout boundary."""

    if not isinstance(payload, dict):
        raise StateError("trend audit response must be a JSON object")
    verdict = payload.get("verdict")
    if verdict not in {"HISTORICAL_NO_GO_CONFIRMED", "AUDIT_REJECTED"}:
        raise StateError("trend audit returned an unsupported verdict")
    if payload.get("preserved_prior_result") not in {
        "cs-ranking-ptu-data-audit-v1=DATA_NO_GO",
        "DATA_NO_GO",
    }:
        raise StateError("trend audit did not preserve the original DATA_NO_GO")
    if payload.get("development_classification") != "HISTORICAL_NO_GO":
        raise StateError("trend audit changed the frozen development classification")
    if payload.get("development_result_sha256") != result_sha256:
        raise StateError("trend audit cited the wrong development result hash")
    if (
        payload.get("performance_scope") != "DEVELOPMENT_ONLY_NOT_A_CANDIDATE"
        or payload.get("holdout_opened") is not False
        or payload.get("holdout_values_read") is not False
        or payload.get("candidate_promoted") is not False
        or payload.get("capital_permitted") != 0
    ):
        raise StateError("trend audit violated the rejection-only/closed-holdout contract")

    def string_list(key: str, *, nonempty: bool) -> list[str]:
        value = payload.get(key)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise StateError(f"trend audit field {key} must be a string list")
        if nonempty and not value:
            raise StateError(f"trend audit field {key} must be nonempty")
        return [str(item)[:1000] for item in value[:40]]

    return {
        "verdict": verdict,
        "preserved_prior_result": "cs-ranking-ptu-data-audit-v1=DATA_NO_GO",
        "development_classification": "HISTORICAL_NO_GO",
        "development_result_sha256": result_sha256,
        "performance_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "holdout_opened": False,
        "holdout_values_read": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
        "methodology_integrity": str(payload.get("methodology_integrity"))[:500],
        "gate_failures_confirmed": string_list("gate_failures_confirmed", nonempty=True),
        "critical_issues": string_list("critical_issues", nonempty=False),
        "limitations": string_list("limitations", nonempty=True),
        "rationale": string_list("rationale", nonempty=True),
    }


def sanitize_mean_reversion_audit_payload(payload: object, *, result_sha256: str) -> dict[str, Any]:
    """Apply the rejection-only trend audit contract to mean-reversion evidence."""

    return sanitize_trend_audit_payload(payload, result_sha256=result_sha256)


def run_independent_trend_audit() -> dict[str, Any]:
    """Independently audit a development no-go without opening the final holdout."""

    state = load_json(STATE)
    validate_state(state)
    if (
        state.get("program_state") != "ACTIVE_RESEARCH"
        or state.get("current_experiment_id") != TREND_EXPERIMENT_ID
        or state.get("development_status") != "HISTORICAL_NO_GO"
        or state.get("next_task") != "run_trend_terminal_independent_audit"
    ):
        raise StateError("trend terminal audit is not the current task")
    remaining_calls = int(state["budgets"].get("agent_calls_remaining", 0))
    if remaining_calls < 1:
        raise StateError("trend terminal audit has no remaining agent call")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before trend audit")

    experiment_root = ROOT / "experiments" / TREND_EXPERIMENT_ID
    prereg = load_json(experiment_root / "PREREGISTRATION.json")
    prereg_hash = prereg.get("preregistration_sha256")
    prereg_hash_input = dict(prereg)
    prereg_hash_input.pop("preregistration_sha256", None)
    if prereg_hash != _sha(prereg_hash_input):
        raise StateError("trend preregistration hash mismatch before audit")
    report = load_json(experiment_root / "DEVELOPMENT_RESULT.json")
    result_hash = _sha(report)
    validation = load_json(experiment_root / "DEVELOPMENT_VALIDATION.json")
    if (
        report.get("classification") != "HISTORICAL_NO_GO"
        or report.get("all_development_gates_pass") is not False
        or report.get("holdout_opened") is not False
        or report.get("holdout_values_read") is not False
        or report.get("performance_claim_scope") != "DEVELOPMENT_ONLY_NOT_A_CANDIDATE"
        or validation.get("development_result_sha256") != result_hash
        or validation.get("pytest") != "PASS_74_TESTS"
        or validation.get("ruff") != "PASS"
        or validation.get("mypy") != "PASS_12_SOURCE_FILES"
    ):
        raise StateError("trend development evidence is not audit-ready")

    prompt = (
        "Act as the independent final methodological auditor for the bounded zero-capital "
        "experiment btc-eth-vol-targeted-trend-v1. Read AGENTS.md, RESEARCH_PROTOCOL.md, "
        "ACCEPTANCE_GATES.yaml, the experiment PREREGISTRATION.json, DATA_CONTRACT.json, "
        "DEVELOPMENT_ATTEMPT_1_FAILURE.json, DEVELOPMENT_RESULT.json, "
        "DEVELOPMENT_VALIDATION.json, src/strategy_control/trend.py, "
        "src/strategy_control/trend_pipeline.py, and the trend tests. Do not edit files, "
        "inspect raw market data or any 2026 holdout file/value/footer, tune parameters, "
        "open the holdout, or promote a candidate. Audit causal timing, gap quarantine, "
        "self-financing costs, folds, variants, bootstrap, DSR, PBO, regime, asset, "
        "benchmark, concentration, and gate evaluation. Preserve "
        "cs-ranking-ptu-data-audit-v1=DATA_NO_GO and the exact development result hash "
        f"{result_hash}. Return strict compact JSON only with: verdict "
        "(HISTORICAL_NO_GO_CONFIRMED or AUDIT_REJECTED), preserved_prior_result "
        "(cs-ranking-ptu-data-audit-v1=DATA_NO_GO), development_classification "
        "(HISTORICAL_NO_GO), development_result_sha256, performance_scope "
        "(DEVELOPMENT_ONLY_NOT_A_CANDIDATE), holdout_opened (false), "
        "holdout_values_read (false), candidate_promoted (false), capital_permitted (0), "
        "methodology_integrity, gate_failures_confirmed (nonempty string list), "
        "critical_issues (string list, empty allowed), limitations (nonempty string list), "
        "and rationale (nonempty string list)."
    )
    invocation = invoke_codex(
        invocation_mode="live",
        role="independent_methodology_auditor",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt=prompt,
        timeout_seconds=300,
    )
    append_jsonl(
        MODEL_LEDGER,
        invocation.ledger_record(
            record_type="MODEL_INVOCATION",
            purpose="trend_development_independent_terminal_audit",
            experiment_id=TREND_EXPERIMENT_ID,
            development_result_sha256=result_hash,
            model_generated_research_claim=model_generated_claim_permitted(invocation),
        ),
    )
    budgets = dict(state["budgets"])
    budgets["agent_calls_used"] = int(budgets.get("agent_calls_used", 0)) + 1
    budgets["agent_calls_remaining"] = remaining_calls - 1
    state["budgets"] = budgets
    state["updated_at_utc"] = invocation.ended_at_utc
    atomic_json(STATE, state)
    if not model_generated_claim_permitted(invocation) or invocation.final_message is None:
        return {
            "status": invocation.outcome,
            "invocation_mode": invocation.invocation_mode,
            "model_generated_research": False,
            "agent_calls_remaining": budgets["agent_calls_remaining"],
            "holdout_opened": False,
            "capital_permitted": 0,
        }
    try:
        audit = sanitize_trend_audit_payload(
            json.loads(invocation.final_message), result_sha256=result_hash
        )
    except (json.JSONDecodeError, StateError) as exc:
        append_jsonl(
            MODEL_LEDGER,
            {
                "record_type": "MODEL_INVOCATION_VALIDATION_FAILURE",
                "at_utc": _now(),
                "experiment_id": TREND_EXPERIMENT_ID,
                "response_identifier": invocation.response_identifier,
                "invocation_mode": "live",
                "exact_error": str(exc),
                "model_generated_research_claim": False,
            },
        )
        raise StateError(f"independent trend audit response failed validation: {exc}") from exc

    audit.update(
        {
            "schema_version": "1.0",
            "experiment_id": TREND_EXPERIMENT_ID,
            "audited_at_utc": invocation.ended_at_utc,
            "auditor_model": invocation.actual_model,
            "reasoning_level": invocation.reasoning_level,
            "invocation_mode": invocation.invocation_mode,
            "response_identifier": invocation.response_identifier,
            "model_result_sha256": invocation.result_sha256,
            "preregistration_sha256": prereg_hash,
            "source_commit": git["head"],
        }
    )
    audit_path = experiment_root / "AUDIT.json"
    atomic_json(audit_path, audit)
    terminal_classification = (
        "HISTORICAL_NO_GO" if audit["verdict"] == "HISTORICAL_NO_GO_CONFIRMED" else "AUDIT_REJECTED"
    )
    record = {
        "record_type": "TERMINAL_EXPERIMENT",
        "experiment_id": TREND_EXPERIMENT_ID,
        "terminal_at_utc": invocation.ended_at_utc,
        "classification": terminal_classification,
        "audit_verdict": audit["verdict"],
        "source_commit": git["head"],
        "preregistration_sha256": prereg_hash,
        "development_result_sha256": result_hash,
        "report": str((experiment_root / "DEVELOPMENT_RESULT.json").relative_to(ROOT)),
        "audit": str(audit_path.relative_to(ROOT)),
        "performance_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "holdout_opened": False,
        "holdout_values_read": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
    }
    append_jsonl(LEDGER, record)
    append_jsonl(
        REJECTED,
        {
            "strategy_id": TREND_EXPERIMENT_ID,
            "classification": terminal_classification,
            "reason": (
                "DEVELOPMENT_GATES_FAILED_INDEPENDENTLY_CONFIRMED"
                if terminal_classification == "HISTORICAL_NO_GO"
                else "INDEPENDENT_METHODOLOGY_AUDIT_REJECTED"
            ),
            "frozen_configuration": prereg_hash,
            "development_result_sha256": result_hash,
            "at_utc": invocation.ended_at_utc,
        },
    )
    state.pop("data_contract_status", None)
    state.pop("development_status", None)
    state.update(
        {
            "program_state": "ACTIVE_RESEARCH",
            "current_experiment_id": "btc-eth-long-only-mean-reversion-v1",
            "last_terminal_experiment_id": TREND_EXPERIMENT_ID,
            "last_terminal_verdict": terminal_classification,
            "next_task": "allocate_and_preregister_btc_eth_long_only_mean_reversion",
            "updated_at_utc": invocation.ended_at_utc,
        }
    )
    atomic_json(STATE, state)
    return {
        "status": terminal_classification,
        "audit_verdict": audit["verdict"],
        "experiment_id": TREND_EXPERIMENT_ID,
        "next_experiment_id": state["current_experiment_id"],
        "invocation_mode": "live",
        "actual_model": invocation.actual_model,
        "reasoning_level": invocation.reasoning_level,
        "response_identifier": invocation.response_identifier,
        "holdout_opened": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
    }


def _finalize_mean_reversion_terminal(
    *,
    state: dict[str, Any],
    git: dict[str, Any],
    prereg_hash: str,
    result_hash: str,
    audit: dict[str, Any],
    terminal_classification: str,
    terminal_at: str,
) -> dict[str, Any]:
    """Persist one terminal mean-reversion result and advance to a distinct family."""

    experiment_root = ROOT / "experiments" / MEAN_REVERSION_EXPERIMENT_ID
    audit_path = experiment_root / "AUDIT.json"
    atomic_json(audit_path, audit)
    append_jsonl(
        LEDGER,
        {
            "record_type": "TERMINAL_EXPERIMENT",
            "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
            "terminal_at_utc": terminal_at,
            "classification": terminal_classification,
            "development_classification": "HISTORICAL_NO_GO",
            "audit_verdict": audit["verdict"],
            "source_commit": git["head"],
            "preregistration_sha256": prereg_hash,
            "development_result_sha256": result_hash,
            "report": str((experiment_root / "DEVELOPMENT_RESULT.json").relative_to(ROOT)),
            "audit": str(audit_path.relative_to(ROOT)),
            "performance_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
            "holdout_opened": False,
            "holdout_values_read": False,
            "candidate_promoted": False,
            "capital_permitted": 0,
        },
    )
    append_jsonl(
        REJECTED,
        {
            "strategy_id": MEAN_REVERSION_EXPERIMENT_ID,
            "classification": terminal_classification,
            "development_classification": "HISTORICAL_NO_GO",
            "reason": (
                "DEVELOPMENT_GATES_FAILED_INDEPENDENTLY_CONFIRMED"
                if terminal_classification == "HISTORICAL_NO_GO"
                else (
                    "INDEPENDENT_METHODOLOGY_AUDIT_REJECTED"
                    if terminal_classification == "AUDIT_REJECTED"
                    else "INDEPENDENT_AUDIT_NOT_OBTAINED_WITHIN_FROZEN_CALL_BUDGET"
                )
            ),
            "frozen_configuration": prereg_hash,
            "development_result_sha256": result_hash,
            "at_utc": terminal_at,
        },
    )
    state.pop("data_contract_status", None)
    state.pop("development_status", None)
    state.pop("preregistration_status", None)
    state.update(
        {
            "program_state": "ACTIVE_RESEARCH",
            "current_experiment_id": RELATIVE_VALUE_EXPERIMENT_ID,
            "last_terminal_experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
            "last_terminal_verdict": terminal_classification,
            "next_task": "allocate_and_preregister_btc_eth_relative_value_rotation",
            "updated_at_utc": terminal_at,
        }
    )
    atomic_json(STATE, state)
    return {
        "status": terminal_classification,
        "audit_verdict": audit["verdict"],
        "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
        "next_experiment_id": RELATIVE_VALUE_EXPERIMENT_ID,
        "invocation_mode": audit["invocation_mode"],
        "actual_model": audit.get("auditor_model"),
        "reasoning_level": audit.get("reasoning_level"),
        "response_identifier": audit.get("response_identifier"),
        "holdout_opened": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
    }


def run_independent_mean_reversion_audit() -> dict[str, Any]:
    """Audit the frozen development rejection without touching the final holdout."""

    state = load_json(STATE)
    validate_state(state)
    if (
        state.get("program_state") != "ACTIVE_RESEARCH"
        or state.get("current_experiment_id") != MEAN_REVERSION_EXPERIMENT_ID
        or state.get("development_status") != "HISTORICAL_NO_GO"
        or state.get("next_task") != "run_mean_reversion_terminal_independent_audit"
    ):
        raise StateError("mean-reversion terminal audit is not the current task")
    remaining_calls = int(state["budgets"].get("agent_calls_remaining", 0))
    if remaining_calls < 1:
        raise StateError("mean-reversion terminal audit has no remaining agent call")
    git = git_state()
    if not git["clean"] or not isinstance(git.get("head"), str):
        raise StateError("controller working tree must be clean before mean-reversion audit")

    experiment_root = ROOT / "experiments" / MEAN_REVERSION_EXPERIMENT_ID
    prereg = load_json(experiment_root / "PREREGISTRATION.json")
    prereg_hash = str(prereg.get("preregistration_sha256"))
    prereg_hash_input = dict(prereg)
    prereg_hash_input.pop("preregistration_sha256", None)
    report = load_json(experiment_root / "DEVELOPMENT_RESULT.json")
    result_hash = _sha(report)
    validation = load_json(experiment_root / "DEVELOPMENT_VALIDATION.json")
    if prereg_hash != _sha(prereg_hash_input):
        raise StateError("mean-reversion preregistration hash mismatch before audit")
    if (
        report.get("classification") != "HISTORICAL_NO_GO"
        or report.get("all_development_gates_pass") is not False
        or report.get("holdout_opened") is not False
        or report.get("holdout_values_read") is not False
        or report.get("performance_claim_scope") != "DEVELOPMENT_ONLY_NOT_A_CANDIDATE"
        or validation.get("development_result_sha256") != result_hash
        or validation.get("pytest") != "PASS_73_TESTS"
        or validation.get("ruff") != "PASS"
        or validation.get("mypy") != "PASS_14_SOURCE_FILES"
        or validation.get("diff_check") != "PASS"
    ):
        raise StateError("mean-reversion development evidence is not audit-ready")

    prompt = (
        "Independently audit the terminal development rejection for "
        "btc-eth-long-only-mean-reversion-v1. Read only AGENTS.md, RESEARCH_PROTOCOL.md, "
        "ACCEPTANCE_GATES.yaml, its PREREGISTRATION.json, DEVELOPMENT_ATTEMPT_1_FAILURE.json, "
        "DEVELOPMENT_RESULT.json, DEVELOPMENT_VALIDATION.json, "
        "src/strategy_control/mean_reversion.py, mean_reversion_pipeline.py, trend.py, "
        "trend_pipeline.py, and their tests. Do not edit, inspect raw data or any 2026 "
        "file/value/footer, tune, open the holdout, or promote. Audit timing, quarantine, "
        "holding clock, costs, folds, variants, statistics, regimes, assets, baselines, "
        f"concentration, and gates. Preserve result hash {result_hash} and "
        "cs-ranking-ptu-data-audit-v1=DATA_NO_GO. Return compact strict JSON only: verdict "
        "HISTORICAL_NO_GO_CONFIRMED or AUDIT_REJECTED; preserved_prior_result; "
        "development_classification HISTORICAL_NO_GO; development_result_sha256; "
        "performance_scope DEVELOPMENT_ONLY_NOT_A_CANDIDATE; holdout_opened false; "
        "holdout_values_read false; candidate_promoted false; capital_permitted 0; "
        "methodology_integrity string; gate_failures_confirmed nonempty string list; "
        "critical_issues string list; limitations nonempty string list; rationale nonempty "
        "string list."
    )
    invocation = invoke_codex(
        invocation_mode="live",
        role="independent_methodology_auditor",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt=prompt,
        timeout_seconds=300,
    )
    append_jsonl(
        MODEL_LEDGER,
        invocation.ledger_record(
            record_type="MODEL_INVOCATION",
            purpose="mean_reversion_development_independent_terminal_audit",
            experiment_id=MEAN_REVERSION_EXPERIMENT_ID,
            development_result_sha256=result_hash,
            model_generated_research_claim=model_generated_claim_permitted(invocation),
        ),
    )
    budgets = dict(state["budgets"])
    budgets["agent_calls_used"] = int(budgets.get("agent_calls_used", 0)) + 1
    budgets["agent_calls_remaining"] = remaining_calls - 1
    state["budgets"] = budgets
    state["updated_at_utc"] = invocation.ended_at_utc
    atomic_json(STATE, state)

    audit_payload: dict[str, Any] | None = None
    validation_error: str | None = None
    if model_generated_claim_permitted(invocation) and invocation.final_message is not None:
        try:
            audit_payload = sanitize_mean_reversion_audit_payload(
                json.loads(invocation.final_message), result_sha256=result_hash
            )
        except (json.JSONDecodeError, StateError) as exc:
            validation_error = _bounded_error(str(exc))
            append_jsonl(
                MODEL_LEDGER,
                {
                    "record_type": "MODEL_INVOCATION_VALIDATION_FAILURE",
                    "at_utc": _now(),
                    "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
                    "response_identifier": invocation.response_identifier,
                    "invocation_mode": "live",
                    "exact_error": validation_error,
                    "model_generated_research_claim": False,
                },
            )
    if audit_payload is None:
        if budgets["agent_calls_remaining"] > 0:
            return {
                "status": "CONTRACT_VIOLATION" if validation_error else invocation.outcome,
                "invocation_mode": "live",
                "model_generated_research": False,
                "agent_calls_remaining": budgets["agent_calls_remaining"],
                "holdout_opened": False,
                "capital_permitted": 0,
            }
        failure_reason = validation_error or invocation.exact_error or invocation.outcome
        audit_payload = {
            "schema_version": "1.0",
            "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
            "audited_at_utc": invocation.ended_at_utc,
            "auditor_model": None,
            "reasoning_level": "xhigh",
            "invocation_mode": "live",
            "response_identifier": invocation.response_identifier,
            "model_result_sha256": invocation.result_sha256,
            "preregistration_sha256": prereg_hash,
            "development_result_sha256": result_hash,
            "development_classification": "HISTORICAL_NO_GO",
            "performance_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
            "verdict": f"NOT_OBTAINED_{invocation.outcome}",
            "exact_error": failure_reason,
            "holdout_opened": False,
            "holdout_values_read": False,
            "candidate_promoted": False,
            "capital_permitted": 0,
            "source_commit": git["head"],
        }
        return _finalize_mean_reversion_terminal(
            state=state,
            git=git,
            prereg_hash=prereg_hash,
            result_hash=result_hash,
            audit=audit_payload,
            terminal_classification="AUDIT_INCONCLUSIVE",
            terminal_at=invocation.ended_at_utc,
        )

    audit_payload.update(
        {
            "schema_version": "1.0",
            "experiment_id": MEAN_REVERSION_EXPERIMENT_ID,
            "audited_at_utc": invocation.ended_at_utc,
            "auditor_model": invocation.actual_model,
            "reasoning_level": invocation.reasoning_level,
            "invocation_mode": invocation.invocation_mode,
            "response_identifier": invocation.response_identifier,
            "model_result_sha256": invocation.result_sha256,
            "preregistration_sha256": prereg_hash,
            "source_commit": git["head"],
        }
    )
    terminal_classification = (
        "HISTORICAL_NO_GO"
        if audit_payload["verdict"] == "HISTORICAL_NO_GO_CONFIRMED"
        else "AUDIT_REJECTED"
    )
    return _finalize_mean_reversion_terminal(
        state=state,
        git=git,
        prereg_hash=prereg_hash,
        result_hash=result_hash,
        audit=audit_payload,
        terminal_classification=terminal_classification,
        terminal_at=invocation.ended_at_utc,
    )


def run_archive_data_audit() -> dict[str, Any]:
    """Execute only the frozen archive data contract; never open strategy data."""

    state = load_json(STATE)
    validate_state(state)
    if state.get("program_state") != "ACTIVE_RESEARCH":
        raise StateError("archive audit requires ACTIVE_RESEARCH")
    if state.get("current_experiment_id") != ARCHIVE_EXPERIMENT_ID:
        raise StateError("archive audit experiment identity mismatch")
    if state.get("next_task") != "implement_archive_enumerator":
        raise StateError(f"archive audit is not at implementation gate: {state.get('next_task')}")
    git = git_state()
    if not git["clean"]:
        raise StateError("controller working tree must be clean before archive retrieval")
    prereg_path = preregistration_path()
    prereg = load_json(prereg_path)
    expected_hash = prereg.get("preregistration_sha256")
    hash_input = dict(prereg)
    hash_input.pop("preregistration_sha256", None)
    if prereg.get("experiment_id") != ARCHIVE_EXPERIMENT_ID or expected_hash != _sha(hash_input):
        raise StateError("frozen archive preregistration identity or hash mismatch")
    frozen_at = datetime.fromisoformat(str(prereg["preregistered_at_utc"]).replace("Z", "+00:00"))
    retrieval_delay_hours = (datetime.now(UTC) - frozen_at).total_seconds() / 3600
    max_delay = float(prereg["enumeration_contract"]["max_retrieval_delay_after_freeze_hours"])
    if retrieval_delay_hours < 0 or retrieval_delay_hours > max_delay:
        raise StateError("frozen archive retrieval window expired")
    started_at = _now()
    monotonic_start = time.monotonic()
    try:
        manifest, report = run_archive_observed_audit(
            first_month="2017-08",
            last_month=str(prereg["sample_contract"]["last_archive_month"]),
            minimum_symbols=int(
                prereg["data_sufficiency"]["minimum_archive_observed_USDT_symbols"]
            ),
            max_workers=16,
        )
    except ArchiveAuditError as exc:
        ended_at = _now()
        duration = round(time.monotonic() - monotonic_start, 6)
        append_jsonl(
            MODEL_LEDGER,
            {
                "record_type": "LOCAL_PIPELINE_INVOCATION",
                "started_at_utc": started_at,
                "ended_at_utc": ended_at,
                "duration_seconds": duration,
                "role": "archive_data_pipeline",
                "invocation_mode": "deterministic_local",
                "requested_model": None,
                "actual_model": None,
                "reasoning_level": None,
                "model_result_received": False,
                "model_generated_research_claim": False,
                "outcome": "INFRASTRUCTURE_OR_DATA_RETRIEVAL_FAILURE",
                "exact_error": str(exc),
                "fallback": None,
                "experiment_id": ARCHIVE_EXPERIMENT_ID,
            },
        )
        raise StateError(f"archive retrieval failed closed: {exc}") from exc
    duration = round(time.monotonic() - monotonic_start, 6)
    if duration > int(state["budgets"]["max_wall_seconds"]):
        raise StateError("archive audit exceeded frozen wall-clock budget")
    experiment_root = ROOT / "experiments" / ARCHIVE_EXPERIMENT_ID
    manifest_path = experiment_root / "SYMBOL_MANIFEST.json"
    report_path = experiment_root / "DATA_INTEGRITY_REPORT.json"
    atomic_json(manifest_path, manifest)
    atomic_json(report_path, report)
    append_jsonl(
        MODEL_LEDGER,
        {
            "record_type": "LOCAL_PIPELINE_INVOCATION",
            "started_at_utc": started_at,
            "ended_at_utc": str(report["ended_at_utc"]),
            "duration_seconds": duration,
            "role": "archive_data_pipeline",
            "invocation_mode": "deterministic_local",
            "requested_model": None,
            "actual_model": None,
            "reasoning_level": None,
            "model_result_received": False,
            "model_generated_research_claim": False,
            "outcome": "TECHNICAL_ROUTE_VALIDATED_PENDING_INDEPENDENT_AUDIT",
            "exact_error": None,
            "fallback": None,
            "experiment_id": ARCHIVE_EXPERIMENT_ID,
            "result_sha256": str(report["manifest_sha256"]),
        },
    )
    state.update(
        {
            "next_task": "run_deterministic_tests_and_independent_audit",
            "updated_at_utc": _now(),
        }
    )
    atomic_json(STATE, state)
    return {
        "status": report["data_contract_result"],
        "experiment_id": ARCHIVE_EXPERIMENT_ID,
        "invocation_mode": "deterministic_local",
        "manifest_sha256": report["manifest_sha256"],
        "archive_observed_symbol_directories": report["archive_observed_symbol_directories"],
        "boundary_valid_in_sample_symbols": report["boundary_valid_in_sample_symbols"],
        "holdout_opened": False,
        "returns_calculated": False,
        "capital_permitted": 0,
    }


def sanitize_archive_audit_payload(payload: object) -> dict[str, Any]:
    """Allowlist the independent audit result and enforce zero-holdout semantics."""

    if not isinstance(payload, dict):
        raise StateError("independent audit response must be a JSON object")
    verdict = payload.get("verdict")
    if verdict not in {"DATA_CONTRACT_GO", "DATA_NO_GO"}:
        raise StateError("independent audit returned an unsupported verdict")
    if payload.get("preserved_prior_result") not in {
        "cs-ranking-ptu-data-audit-v1=DATA_NO_GO",
        "DATA_NO_GO",
    }:
        raise StateError("independent audit did not preserve the prior DATA_NO_GO")
    if (
        payload.get("holdout_opened") is not False
        or payload.get("returns_calculated") is not False
        or payload.get("performance_claim_made") is not False
        or payload.get("capital_permitted") != 0
    ):
        raise StateError("independent audit violated the zero-capital/no-holdout contract")
    if payload.get("archive_completeness_claim") != "NOT_FORMALLY_COMPLETE":
        raise StateError("independent audit overstated archive completeness")

    def string_list(key: str) -> list[str]:
        value = payload.get(key)
        if (
            not isinstance(value, list)
            or not value
            or not all(isinstance(item, str) for item in value)
        ):
            raise StateError(f"independent audit field {key} must be a nonempty string list")
        return [str(item)[:1000] for item in value[:30]]

    return {
        "verdict": verdict,
        "preserved_prior_result": "cs-ranking-ptu-data-audit-v1=DATA_NO_GO",
        "holdout_opened": False,
        "returns_calculated": False,
        "performance_claim_made": False,
        "capital_permitted": 0,
        "archive_completeness_claim": "NOT_FORMALLY_COMPLETE",
        "internal_bars_status": str(payload.get("internal_bars_status"))[:500],
        "critical_tests_reviewed": string_list("critical_tests_reviewed"),
        "limitations": string_list("limitations"),
        "rationale": string_list("rationale"),
    }


def run_independent_archive_audit() -> dict[str, Any]:
    """Run the final independent methodology audit and terminally record the data contract."""

    state = load_json(STATE)
    validate_state(state)
    if state.get("current_experiment_id") != ARCHIVE_EXPERIMENT_ID:
        raise StateError("independent audit experiment identity mismatch")
    if state.get("next_task") != "run_deterministic_tests_and_independent_audit":
        raise StateError("independent archive audit is not the current task")
    if int(state["budgets"].get("agent_calls_remaining", 0)) != 1:
        raise StateError("independent archive audit requires exactly one remaining agent call")
    git = git_state()
    if not git["clean"]:
        raise StateError("controller working tree must be clean before independent audit")
    experiment_root = ROOT / "experiments" / ARCHIVE_EXPERIMENT_ID
    manifest = load_json(experiment_root / "SYMBOL_MANIFEST.json")
    manifest_hash = manifest.pop("manifest_sha256", None)
    if manifest_hash != _sha(manifest):
        raise StateError("archive manifest hash mismatch before independent audit")
    report = load_json(experiment_root / "DATA_INTEGRITY_REPORT.json")
    validation = load_json(experiment_root / "DETERMINISTIC_VALIDATION.json")
    if report.get("manifest_sha256") != manifest_hash:
        raise StateError("data report and manifest hash disagree")
    if validation.get("verdict") != "DETERMINISTIC_VALIDATION_PASS":
        raise StateError("deterministic validation did not pass")
    prompt = (
        "Act as the independent final methodological auditor for the bounded experiment "
        "cs-ranking-binance-spot-archive-ptu-audit-v1. Read AGENTS.md, RESEARCH_PROTOCOL.md, "
        "the frozen PREREGISTRATION.json, DATA_INTEGRITY_REPORT.json, "
        "DETERMINISTIC_VALIDATION.json, SYMBOL_MANIFEST.json, src/strategy_control/"
        "archive_audit.py, src/strategy_control/archive_universe.py, and the archive tests. "
        "Do not edit files, inspect any strategy holdout, calculate returns, or make a "
        "strategy-performance claim. Preserve cs-ranking-ptu-data-audit-v1=DATA_NO_GO. "
        "Audit whether the archive-observed universe CONTRACT passes exactly as frozen; "
        "all internal bars must remain ineligible/quarantined until later full checksum and "
        "row validation, and formal archive completeness must not be claimed. Return strict "
        "compact JSON only with: verdict (DATA_CONTRACT_GO or DATA_NO_GO), "
        "preserved_prior_result (cs-ranking-ptu-data-audit-v1=DATA_NO_GO), holdout_opened "
        "(false), returns_calculated (false), performance_claim_made (false), "
        "capital_permitted (0), archive_completeness_claim (NOT_FORMALLY_COMPLETE), "
        "internal_bars_status, critical_tests_reviewed (nonempty string list), limitations "
        "(nonempty string list), and rationale (nonempty string list)."
    )
    invocation = invoke_codex(
        invocation_mode="live",
        role="independent_methodology_auditor",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt=prompt,
    )
    append_jsonl(
        MODEL_LEDGER,
        invocation.ledger_record(
            record_type="MODEL_INVOCATION",
            purpose="archive_universe_independent_terminal_audit",
            experiment_id=ARCHIVE_EXPERIMENT_ID,
            model_generated_research_claim=model_generated_claim_permitted(invocation),
        ),
    )
    if not model_generated_claim_permitted(invocation) or invocation.final_message is None:
        return {
            "status": invocation.outcome,
            "invocation_mode": invocation.invocation_mode,
            "model_generated_research": False,
        }
    try:
        raw_payload = json.loads(invocation.final_message)
        audit = sanitize_archive_audit_payload(raw_payload)
    except (json.JSONDecodeError, StateError) as exc:
        append_jsonl(
            MODEL_LEDGER,
            {
                "record_type": "MODEL_INVOCATION_VALIDATION_FAILURE",
                "at_utc": _now(),
                "response_identifier": invocation.response_identifier,
                "invocation_mode": "live",
                "exact_error": str(exc),
                "model_generated_research_claim": False,
            },
        )
        raise StateError(f"independent audit response failed validation: {exc}") from exc
    audit.update(
        {
            "schema_version": "1.0",
            "experiment_id": ARCHIVE_EXPERIMENT_ID,
            "audited_at_utc": invocation.ended_at_utc,
            "auditor_model": invocation.actual_model,
            "reasoning_level": invocation.reasoning_level,
            "invocation_mode": invocation.invocation_mode,
            "response_identifier": invocation.response_identifier,
            "model_result_sha256": invocation.result_sha256,
            "manifest_sha256": manifest_hash,
            "source_commit": git["head"],
        }
    )
    audit_path = experiment_root / "AUDIT.json"
    atomic_json(audit_path, audit)
    verdict = str(audit["verdict"])
    record = {
        "record_type": "TERMINAL_EXPERIMENT",
        "experiment_id": ARCHIVE_EXPERIMENT_ID,
        "terminal_at_utc": invocation.ended_at_utc,
        "classification": verdict,
        "source_commit": git["head"],
        "preregistration_sha256": load_json(preregistration_path())["preregistration_sha256"],
        "manifest_sha256": manifest_hash,
        "report": str((experiment_root / "DATA_INTEGRITY_REPORT.json").relative_to(ROOT)),
        "audit": str(audit_path.relative_to(ROOT)),
        "audit_verdict": verdict,
        "holdout_opened": False,
        "returns_calculated": False,
        "capital_permitted": 0,
    }
    append_jsonl(LEDGER, record)
    inventory = load_json(INVENTORY)
    for dataset in inventory.get("datasets", []):
        if (
            isinstance(dataset, dict)
            and dataset.get("id") == "binance-spot-archive-observed-usdt-v1"
        ):
            dataset["audit_status"] = verdict
            dataset["point_in_time_universe"] = (
                "archive_observed_contract_passed"
                if verdict == "DATA_CONTRACT_GO"
                else "archive_observed_contract_rejected"
            )
            dataset["reason"] = (
                "Universe contract passed independently; internal bars remain ineligible until "
                "full checksum and row-level validation."
                if verdict == "DATA_CONTRACT_GO"
                else "Independent audit rejected the archive-observed universe contract."
            )
    inventory["generated_at_utc"] = invocation.ended_at_utc
    atomic_json(INVENTORY, inventory)
    if verdict == "DATA_NO_GO":
        append_jsonl(
            REJECTED,
            {
                "strategy_id": ARCHIVE_EXPERIMENT_ID,
                "classification": "DATA_NO_GO",
                "reason": "INDEPENDENT_ARCHIVE_UNIVERSE_CONTRACT_REJECTION",
                "frozen_configuration": record["preregistration_sha256"],
                "at_utc": invocation.ended_at_utc,
            },
        )
        next_experiment = "btc-eth-vol-targeted-trend-v1"
        next_task = "preregister_btc_eth_vol_targeted_trend"
    else:
        next_experiment = "cs-ranking-archive-momentum-baseline-v1"
        next_task = "acquire_full_validated_bars_and_preregister_cs_momentum_baseline"
    budgets = dict(state["budgets"])
    budgets.update({"agent_calls_used": 3, "agent_calls_remaining": 0, "cycles_remaining": 0})
    state.update(
        {
            "budgets": budgets,
            "program_state": "ACTIVE_RESEARCH",
            "current_experiment_id": next_experiment,
            "last_terminal_experiment_id": ARCHIVE_EXPERIMENT_ID,
            "last_terminal_verdict": verdict,
            "next_task": next_task,
            "updated_at_utc": invocation.ended_at_utc,
        }
    )
    atomic_json(STATE, state)
    return {
        "status": verdict,
        "experiment_id": ARCHIVE_EXPERIMENT_ID,
        "next_experiment_id": next_experiment,
        "invocation_mode": "live",
        "actual_model": invocation.actual_model,
        "reasoning_level": invocation.reasoning_level,
        "response_identifier": invocation.response_identifier,
        "holdout_opened": False,
        "returns_calculated": False,
        "capital_permitted": 0,
    }


def status() -> dict[str, Any]:
    state = load_json(STATE)
    validate_state(state)
    return {
        "state": state,
        "git": git_state(),
        "experiments": len(terminal_experiments()),
        "program_events": len(_lines(LEDGER)) - len(terminal_experiments()),
        "rejected": len(_lines(REJECTED)),
        "model_invocations": len(_lines(MODEL_LEDGER)),
    }


def require_scheduled_continuation_authority(state: dict[str, Any]) -> None:
    """Refuse scheduled work while interactive ownership or disablement is recorded."""

    continuation = state.get("continuation")
    if not isinstance(continuation, dict) or continuation.get("scheduled_enabled") is not True:
        raise StateError("scheduled continuation is disabled")
    active_owner = continuation.get("active_owner_type")
    if active_owner is not None:
        raise StateError("scheduled continuation requires cleared active ownership")
    if continuation.get("bounded_cycles_per_run") != 1:
        raise StateError("scheduled continuation requires one bounded cycle per run")


def public_snapshot(*, dry_run: bool) -> dict[str, Any]:
    """Produce an allowlisted, static snapshot for the portfolio consumer."""

    manifest = load_json(ROOT / "PUBLICATION_MANIFEST.json")
    public_fields = set(manifest["public_fields"])
    terminal_rows = terminal_experiments()
    terminal = terminal_rows[-1] if terminal_rows else {}
    state = load_json(STATE)
    git = git_state()
    publication_source_commit = (
        git["head"]
        if git["clean"] and isinstance(git.get("head"), str)
        else terminal.get("source_commit")
    )
    limitation = (
        "Development-only historical rejection: the final holdout remained closed, no "
        "candidate was promoted, and no prospective or deployable performance conclusion "
        "is permitted."
        if terminal.get("performance_scope") == "DEVELOPMENT_ONLY_NOT_A_CANDIDATE"
        else "Data-contract result only: no holdout was opened, no returns were calculated, "
        "no candidate was promoted, capital remains zero, and no profitability conclusion "
        "is permitted."
    )
    snapshot = {
        "schema_version": "1.0",
        "generated_at_utc": _now(),
        "program_state": state["program_state"],
        "capital_permitted": state["capital_permitted"],
        "experiment_id": terminal.get("experiment_id"),
        "classification": terminal.get("classification"),
        "source_commit": publication_source_commit,
        "preregistration_sha256": terminal.get("preregistration_sha256"),
        "limitation": limitation,
    }
    if set(snapshot) - {"schema_version", "generated_at_utc", *public_fields}:
        raise StateError("publication allowlist violation")
    serialized = _canonical(snapshot).lower()
    if any(term in serialized for term in manifest["prohibited_fields"]):
        raise StateError("publication prohibited-field violation")
    path = ROOT / "publication" / "research-program-snapshot.json"
    if dry_run:
        return {"dry_run": True, "path": str(path.relative_to(ROOT)), "snapshot": snapshot}
    atomic_json(path, snapshot)
    append_jsonl(
        PUBLICATION_LOG,
        {
            "at_utc": _now(),
            "artifact": str(path.relative_to(ROOT)),
            "status": "SANITIZED_SNAPSHOT_BUILT",
            "sha256": _sha(snapshot),
        },
    )
    return {"status": "SANITIZED_SNAPSHOT_BUILT", "path": str(path.relative_to(ROOT))}


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded zero-capital crypto research controller")
    parser.add_argument(
        "command",
        choices=(
            "status",
            "freeze",
            "dry-run",
            "mock-validate",
            "cycle",
            "run",
            "resume",
            "publish",
            "publication-dry-run",
            "prospective",
            "snapshot",
            "smoke",
            "archive-audit",
            "archive-independent-audit",
            "trend-direction-review",
            "trend-data-audit",
            "trend-freeze",
            "trend-development",
            "trend-independent-audit",
            "mean-reversion-direction-review",
            "mean-reversion-development",
            "mean-reversion-independent-audit",
            "relative-value-development",
            "calendar-development",
            "volatility-parity-development",
            "volatility-managed-development",
        ),
    )
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--invocation-mode", choices=INVOCATION_MODES, default="live")
    parser.add_argument(
        "--owner-type",
        choices=("manual", "interactive_goal", "scheduled_continuation"),
        default="manual",
    )
    args = parser.parse_args()
    invocation_mode = args.invocation_mode
    if invocation_mode == "mock" and os.environ.get("CRYPTO_STRATEGY_CONTROL_TESTING") != "1":
        raise StateError("mock invocation mode is restricted to unit tests")
    with exclusive_lock(owner_type=args.owner_type):
        if args.owner_type == "scheduled_continuation":
            require_scheduled_continuation_authority(load_json(STATE))
        if args.command == "status":
            result = status()
        elif args.command == "freeze":
            result = freeze_preregistration(dry_run=False)
        elif args.command == "publication-dry-run":
            result = public_snapshot(dry_run=True)
        elif args.command == "dry-run":
            result = run_cycle(invocation_mode="deterministic_local", dry_run=True)
        elif args.command == "mock-validate":
            result = run_cycle(invocation_mode="mock", dry_run=True)
        elif args.command in {"cycle", "run", "resume"}:
            if args.cycles < 1 or args.cycles > 3:
                raise StateError("cycles must be 1..3")
            result = run_bounded_cycles(invocation_mode=invocation_mode, cycles=args.cycles)
        elif args.command == "smoke":
            result = smoke_review(invocation_mode=invocation_mode)
        elif args.command == "archive-audit":
            result = run_archive_data_audit()
        elif args.command == "archive-independent-audit":
            result = run_independent_archive_audit()
        elif args.command == "trend-direction-review":
            result = run_trend_direction_review(invocation_mode=invocation_mode)
        elif args.command == "trend-data-audit":
            result = run_trend_data_audit()
        elif args.command == "trend-freeze":
            result = freeze_trend_preregistration()
        elif args.command == "trend-development":
            result = run_trend_development()
        elif args.command == "trend-independent-audit":
            result = run_independent_trend_audit()
        elif args.command == "mean-reversion-direction-review":
            result = run_mean_reversion_direction_review(invocation_mode=invocation_mode)
        elif args.command == "mean-reversion-development":
            result = run_mean_reversion_development()
        elif args.command == "mean-reversion-independent-audit":
            result = run_independent_mean_reversion_audit()
        elif args.command == "relative-value-development":
            result = run_relative_value_development()
        elif args.command == "calendar-development":
            result = run_calendar_development()
        elif args.command == "volatility-parity-development":
            result = run_volatility_parity_development()
        elif args.command == "volatility-managed-development":
            result = run_volatility_managed_development()
        elif args.command == "prospective":
            result = {"status": "NO_FROZEN_PROSPECTIVE_CANDIDATE", "capital_permitted": 0}
        elif args.command == "snapshot":
            result = public_snapshot(dry_run=False)
        else:
            result = {
                "status": "PUBLICATION_PENDING",
                "reason": "invoke portfolio adapter after a source checkpoint",
            }
    print(json.dumps(result, sort_keys=True))
