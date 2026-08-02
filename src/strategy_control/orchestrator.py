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
        metadata = {
            "pid": os.getpid(),
            "process_start_identity": _process_start_identity(),
            "owner_type": owner_type,
            "run_id": run_id,
            "started_at_utc": _now(),
        }
        os.ftruncate(descriptor, 0)
        os.write(descriptor, (_canonical(metadata) + "\n").encode())
        os.fsync(descriptor)
        yield
    finally:
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
        "ACTIVE_RESEARCH",
        "DATA_BLOCKED",
        "PAUSED_FOR_USAGE",
        "PUBLICATION_PENDING",
        "RESEARCH_BUDGET_EXHAUSTED",
        "INFRASTRUCTURE_BLOCKED",
        "SAFETY_STOP",
    }:
        raise StateError("unsupported program state")


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
    terminal_ids = {str(item["experiment_id"]) for item in terminal_experiments()}
    if COMPLETED_EXPERIMENT_ID not in terminal_ids:
        raise StateError("frozen initial DATA_NO_GO record is missing")
    if ARCHIVE_EXPERIMENT_ID in terminal_ids:
        return {"task": "SELECT_NEXT_APPROVED_FAMILY", "reason": "archive audit is terminal"}
    if current.get("program_state") != "ACTIVE_RESEARCH":
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
            "bucket_list_endpoint": (
                "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
            ),
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
    except subprocess.TimeoutExpired as exc:
        exit_code = None
        stderr = f"Codex invocation timed out after {timeout_seconds} seconds: {exc}"
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


def run_cycle(*, invocation_mode: InvocationMode, dry_run: bool) -> dict[str, Any]:
    state = load_json(STATE)
    validate_state(state)
    task = select_task(state)
    if dry_run:
        return {"dry_run": True, "task": task, "state": state["program_state"], "codex_calls": 0}
    if task["task"] != ARCHIVE_EXPERIMENT_ID:
        raise StateError(f"no runnable archive task: {task['task']}")
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
    if continuation.get("active_owner_type") == "interactive_goal":
        raise StateError("scheduled continuation refused during active Goal ownership")


def public_snapshot(*, dry_run: bool) -> dict[str, Any]:
    """Produce an allowlisted, static snapshot for the portfolio consumer."""

    manifest = load_json(ROOT / "PUBLICATION_MANIFEST.json")
    public_fields = set(manifest["public_fields"])
    terminal_rows = terminal_experiments()
    terminal = terminal_rows[-1] if terminal_rows else {}
    state = load_json(STATE)
    snapshot = {
        "schema_version": "1.0",
        "generated_at_utc": _now(),
        "program_state": state["program_state"],
        "capital_permitted": state["capital_permitted"],
        "experiment_id": terminal.get("experiment_id"),
        "classification": terminal.get("classification"),
        "source_commit": terminal.get("source_commit"),
        "preregistration_sha256": terminal.get("preregistration_sha256"),
        "limitation": (
            "Data-contract result only: no holdout was opened, no returns were "
            "calculated, and no profitability conclusion is permitted."
        ),
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
            result = run_cycle(invocation_mode=invocation_mode, dry_run=False)
        elif args.command == "smoke":
            result = smoke_review(invocation_mode=invocation_mode)
        elif args.command == "archive-audit":
            result = run_archive_data_audit()
        elif args.command == "archive-independent-audit":
            result = run_independent_archive_audit()
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
