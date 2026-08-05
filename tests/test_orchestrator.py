"""Safety properties for the persistent research controller."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from strategy_control import orchestrator


def test_atomic_json_replaces_complete_document(tmp_path: Path) -> None:
    target = tmp_path / "state.json"
    orchestrator.atomic_json(target, {"complete": True})
    assert json.loads(target.read_text()) == {"complete": True}
    assert not target.with_suffix(".json.tmp").exists()


def test_corrupt_state_fails_closed(tmp_path: Path) -> None:
    target = tmp_path / "state.json"
    target.write_text("{not-json")
    with pytest.raises(orchestrator.StateError, match="corruption"):
        orchestrator.load_json(target)


def test_live_lock_refuses_concurrent_mutation(tmp_path: Path) -> None:
    lock = tmp_path / "cycle.lock"
    with (
        orchestrator.exclusive_lock(lock),
        pytest.raises(orchestrator.StateError, match="concurrent"),
        orchestrator.exclusive_lock(lock),
    ):
        pass


def test_unlocked_old_lockfile_is_reused_without_age_based_stealing(tmp_path: Path) -> None:
    lock = tmp_path / "cycle.lock"
    lock.write_text("stale")
    with orchestrator.exclusive_lock(lock, stale_seconds=1):
        assert lock.exists()
        metadata = json.loads(lock.read_text())
        assert metadata["pid"] == os.getpid()
        assert metadata["owner_type"] == "manual"
        assert metadata["status"] == "active"
    assert lock.exists()
    released = json.loads(lock.read_text())
    assert released["status"] == "released"
    assert released["released_at_utc"]
    assert not list(tmp_path.glob("cycle.lock.stale-*"))


@pytest.mark.parametrize(
    ("outcome", "status"),
    [
        ("success", "USED"),
        ("confirmed_rate_limit", "FALLBACK_ELIGIBLE"),
        ("confirmed_temporary_model_unavailable", "FALLBACK_ELIGIBLE"),
        ("confirmed_quota_exhausted", "PAUSED_FOR_USAGE"),
        ("substantive_failure", "FAILED"),
        ("coding_failure", "FAILED"),
        ("test_failure", "FAILED"),
        ("audit_rejection", "FAILED"),
        ("infrastructure_failure", "INFRASTRUCTURE_BLOCKED"),
    ],
)
def test_model_routing_preserves_quality_or_stops(outcome: str, status: str) -> None:
    route = orchestrator.model_route(outcome)
    assert route["status"] == status
    if outcome in {"confirmed_rate_limit", "confirmed_temporary_model_unavailable"}:
        assert route["reasoning"] == "medium"
    if outcome in {
        "substantive_failure",
        "coding_failure",
        "test_failure",
        "audit_rejection",
        "infrastructure_failure",
    }:
        assert route["model"] is None


def test_unconfirmed_model_unavailability_is_not_a_fallback_signal() -> None:
    with pytest.raises(orchestrator.StateError, match="unknown model outcome"):
        orchestrator.model_route("unavailable")


def test_live_codex_command_is_explicit_ephemeral_and_read_only() -> None:
    command = orchestrator.codex_command(model="gpt-5.6-sol", reasoning="xhigh", prompt="safe")
    assert command[:4] == ["codex", "--ask-for-approval", "never", "exec"]
    assert "--ephemeral" in command
    assert command[command.index("--sandbox") + 1] == "read-only"
    assert command[command.index("--model") + 1] == "gpt-5.6-sol"
    assert 'model_reasoning_effort="xhigh"' in command


@pytest.mark.parametrize("mode", ["mock", "deterministic_local"])
def test_non_live_modes_can_never_authorize_model_generated_claims(mode: str) -> None:
    result = orchestrator.invoke_codex(
        invocation_mode=mode,  # type: ignore[arg-type]
        role="test",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt="unused",
    )
    assert result.invocation_mode == mode
    assert result.model_result_received is False
    assert orchestrator.model_generated_claim_permitted(result) is False


def test_failed_live_startup_produces_no_model_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failed(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["codex"],
            returncode=1,
            stdout="",
            stderr="failed to initialize client",
        )

    monkeypatch.setattr(orchestrator.subprocess, "run", failed)
    result = orchestrator.invoke_codex(
        invocation_mode="live",
        role="test",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt="safe",
    )
    assert result.outcome == "INFRASTRUCTURE_FAILURE"
    assert result.exact_error == "failed to initialize client"
    assert result.actual_model is None
    assert orchestrator.model_generated_claim_permitted(result) is False


def test_successful_live_result_records_response_without_transcript(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = "\n".join(
        [
            json.dumps({"type": "thread.started", "thread_id": "thread-1"}),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": '{"safe":true}'},
                }
            ),
        ]
    )

    def succeeded(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=["codex"], returncode=0, stdout=events, stderr="")

    monkeypatch.setattr(orchestrator.subprocess, "run", succeeded)
    result = orchestrator.invoke_codex(
        invocation_mode="live",
        role="test",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt="safe",
    )
    record = result.ledger_record()
    assert result.actual_model == "gpt-5.6-sol"
    assert result.response_identifier == "thread-1"
    assert orchestrator.model_generated_claim_permitted(result) is True
    assert "final_message" not in record
    assert result.result_sha256


def test_timeout_error_does_not_embed_prompt_or_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def timed_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["codex", "secret prompt"], timeout=3)

    monkeypatch.setattr(orchestrator.subprocess, "run", timed_out)
    result = orchestrator.invoke_codex(
        invocation_mode="live",
        role="test",
        model="gpt-5.6-sol",
        reasoning="xhigh",
        prompt="secret prompt",
        timeout_seconds=3,
    )
    assert result.exact_error == "Codex invocation timed out after 3 seconds"
    assert "secret prompt" not in result.exact_error


def test_smoke_verdict_accepts_compact_or_structured_json() -> None:
    assert orchestrator._contains_preserved_no_go("DATA_NO_GO_CONFIRMED")
    assert orchestrator._contains_preserved_no_go(
        {"classification": "DATA_NO_GO", "audit": "DATA_NO_GO_CONFIRMED"}
    )
    assert not orchestrator._contains_preserved_no_go({"classification": "GO"})


def test_program_correction_is_not_a_terminal_experiment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        json.dumps(
            {
                "experiment_id": orchestrator.COMPLETED_EXPERIMENT_ID,
                "classification": "DATA_NO_GO",
                "terminal_at_utc": "2026-01-01T00:00:00Z",
            }
        )
        + "\n"
        + json.dumps(
            {
                "record_type": "PROGRAM_STATE_CORRECTION",
                "preserved_terminal_experiment_id": orchestrator.COMPLETED_EXPERIMENT_ID,
            }
        )
        + "\n"
    )
    monkeypatch.setattr(orchestrator, "LEDGER", ledger)
    rows = orchestrator.terminal_experiments()
    assert len(rows) == 1
    task = orchestrator.select_task({"program_state": "ACTIVE_RESEARCH"})
    assert task["task"] == orchestrator.ARCHIVE_EXPERIMENT_ID


def test_phase_2_is_a_supported_active_state() -> None:
    state = {
        "schema_version": "2.0",
        "program_state": "ACTIVE_RESEARCH_PHASE_2",
        "capital_permitted": 0,
        "next_task": "draft_mean_reversion_v2_preregistration",
        "budgets": {},
    }
    orchestrator.validate_state(state)
    assert orchestrator.is_active_program_state(state) is True


def test_phase_2_selection_does_not_reopen_the_phase_1_archive_route() -> None:
    state = {
        "program_state": "ACTIVE_RESEARCH_PHASE_2",
        "current_experiment_id": orchestrator.PHASE_2_MEAN_REVERSION_EXPERIMENT_ID,
        "next_task": "draft_mean_reversion_v2_preregistration",
    }
    task = orchestrator.select_task(state)
    assert task == {
        "task": "draft_mean_reversion_v2_preregistration",
        "experiment_id": orchestrator.PHASE_2_MEAN_REVERSION_EXPERIMENT_ID,
        "phase": 2,
        "information_value": (
            "Execute the explicitly authorized Phase 2 workstream while preserving all "
            "prior-phase terminal evidence."
        ),
    }


def test_phase_3_is_a_supported_active_state() -> None:
    state = {
        "schema_version": "2.0",
        "program_state": "ACTIVE_RESEARCH_PHASE_3_ADAPTIVE_PORTFOLIO",
        "capital_permitted": 0,
        "next_task": "run_mean_reversion_v2_phase_3_preflight",
        "budgets": {},
    }
    orchestrator.validate_state(state)
    assert orchestrator.is_active_program_state(state) is True


def test_phase_3_selection_preserves_explicit_workstream_routing() -> None:
    state = {
        "program_state": "ACTIVE_RESEARCH_PHASE_3_ADAPTIVE_PORTFOLIO",
        "current_experiment_id": orchestrator.PHASE_2_MEAN_REVERSION_EXPERIMENT_ID,
        "next_task": "run_mean_reversion_v2_phase_3_preflight",
    }
    task = orchestrator.select_task(state)
    assert task == {
        "task": "run_mean_reversion_v2_phase_3_preflight",
        "experiment_id": orchestrator.PHASE_2_MEAN_REVERSION_EXPERIMENT_ID,
        "phase": 3,
        "information_value": (
            "Execute the explicitly authorized Phase 3 workstream while preserving all "
            "prior-phase terminal evidence."
        ),
    }


def test_scheduled_continuation_refuses_active_goal_and_disabled_state() -> None:
    with pytest.raises(orchestrator.StateError, match="disabled"):
        orchestrator.require_scheduled_continuation_authority(
            {"continuation": {"scheduled_enabled": False}}
        )
    with pytest.raises(orchestrator.StateError, match="cleared active ownership"):
        orchestrator.require_scheduled_continuation_authority(
            {
                "continuation": {
                    "scheduled_enabled": True,
                    "active_owner_type": "interactive_goal",
                    "bounded_cycles_per_run": 1,
                }
            }
        )


def test_scheduled_continuation_accepts_explicit_noninteractive_handoff() -> None:
    orchestrator.require_scheduled_continuation_authority(
        {
            "continuation": {
                "scheduled_enabled": True,
                "active_owner_type": None,
                "bounded_cycles_per_run": 1,
            }
        }
    )


def test_scheduled_continuation_requires_exactly_one_cycle() -> None:
    with pytest.raises(orchestrator.StateError, match="one bounded cycle"):
        orchestrator.require_scheduled_continuation_authority(
            {
                "continuation": {
                    "scheduled_enabled": True,
                    "active_owner_type": None,
                    "bounded_cycles_per_run": 2,
                }
            }
        )


def test_unregistered_resume_task_is_a_no_mutation_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "CURRENT_STATE.json"
    payload = {
        "schema_version": "1.0",
        "program_state": "ACTIVE_RESEARCH",
        "capital_permitted": 0,
        "current_experiment_id": "new-experiment",
        "next_task": "native_review_required",
        "budgets": {},
    }
    state.write_text(json.dumps(payload))
    monkeypatch.setattr(orchestrator, "STATE", state)
    result = orchestrator.run_cycle(invocation_mode="live", dry_run=False)
    assert result["status"] == "NO_AUTOMATED_STEP_REGISTERED"
    assert result["model_invocations"] == 0
    assert json.loads(state.read_text()) == payload


def test_unregistered_phase_2_task_is_a_no_mutation_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "CURRENT_STATE.json"
    payload = {
        "schema_version": "2.0",
        "program_state": "ACTIVE_RESEARCH_PHASE_2",
        "capital_permitted": 0,
        "current_experiment_id": orchestrator.PHASE_2_MEAN_REVERSION_EXPERIMENT_ID,
        "next_task": "draft_mean_reversion_v2_preregistration",
        "budgets": {},
    }
    state.write_text(json.dumps(payload))
    monkeypatch.setattr(orchestrator, "STATE", state)
    result = orchestrator.run_cycle(invocation_mode="live", dry_run=False)
    assert result["status"] == "NO_AUTOMATED_STEP_REGISTERED"
    assert result["model_invocations"] == 0
    assert json.loads(state.read_text()) == payload


def test_calendar_development_refuses_before_exact_committed_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "CURRENT_STATE.json"
    state.write_text(
        json.dumps(
            {
                "program_state": "ACTIVE_RESEARCH",
                "current_experiment_id": orchestrator.CALENDAR_EXPERIMENT_ID,
                "data_contract_status": "FROZEN_REUSED_FIXED_PAIR_CONTRACT_PASS",
                "implementation_status": "PASS_PRE_DATA",
                "next_task": "commit_still_required",
            }
        )
    )
    monkeypatch.setattr(orchestrator, "STATE", state)
    with pytest.raises(orchestrator.StateError, match="not at the development gate"):
        orchestrator.run_calendar_development()


def test_bounded_cycles_honors_limit_and_stops_without_transition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "CURRENT_STATE.json"
    state.write_text(json.dumps({"counter": 0}))
    monkeypatch.setattr(orchestrator, "STATE", state)

    def changing_then_idle(*, invocation_mode: str, dry_run: bool) -> dict[str, object]:
        del invocation_mode, dry_run
        payload = json.loads(state.read_text())
        if payload["counter"] < 2:
            payload["counter"] += 1
            state.write_text(json.dumps(payload))
        return {"status": "STEP"}

    monkeypatch.setattr(orchestrator, "run_cycle", changing_then_idle)
    result = orchestrator.run_bounded_cycles(invocation_mode="live", cycles=3)
    assert result["cycles_requested"] == 3
    assert result["cycles_attempted"] == 3
    assert result["cycles_with_state_change"] == 2
    assert json.loads(state.read_text())["counter"] == 2


def test_independent_archive_audit_payload_is_fail_closed_and_allowlisted() -> None:
    payload = {
        "verdict": "DATA_CONTRACT_GO",
        "preserved_prior_result": "cs-ranking-ptu-data-audit-v1=DATA_NO_GO",
        "holdout_opened": False,
        "returns_calculated": False,
        "performance_claim_made": False,
        "capital_permitted": 0,
        "archive_completeness_claim": "NOT_FORMALLY_COMPLETE",
        "internal_bars_status": "QUARANTINED_PENDING_FULL_VALIDATION",
        "critical_tests_reviewed": ["prefix invariance"],
        "limitations": ["not formally complete"],
        "rationale": ["frozen contract passes"],
        "unexpected_transcript": "must not persist",
    }
    sanitized = orchestrator.sanitize_archive_audit_payload(payload)
    assert sanitized["verdict"] == "DATA_CONTRACT_GO"
    assert "unexpected_transcript" not in sanitized
    with pytest.raises(orchestrator.StateError, match="zero-capital"):
        orchestrator.sanitize_archive_audit_payload({**payload, "holdout_opened": True})


def test_independent_trend_audit_payload_is_fail_closed_and_allowlisted() -> None:
    result_hash = "a" * 64
    payload = {
        "verdict": "HISTORICAL_NO_GO_CONFIRMED",
        "preserved_prior_result": "cs-ranking-ptu-data-audit-v1=DATA_NO_GO",
        "development_classification": "HISTORICAL_NO_GO",
        "development_result_sha256": result_hash,
        "performance_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "holdout_opened": False,
        "holdout_values_read": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
        "methodology_integrity": "sufficient for rejection",
        "gate_failures_confirmed": ["Sharpe"],
        "critical_issues": [],
        "limitations": ["development only"],
        "rationale": ["multiple gates failed"],
        "unexpected_transcript": "must not persist",
    }
    sanitized = orchestrator.sanitize_trend_audit_payload(payload, result_sha256=result_hash)
    assert sanitized["verdict"] == "HISTORICAL_NO_GO_CONFIRMED"
    assert "unexpected_transcript" not in sanitized
    with pytest.raises(orchestrator.StateError, match="closed-holdout"):
        orchestrator.sanitize_trend_audit_payload(
            {**payload, "holdout_values_read": True}, result_sha256=result_hash
        )
    with pytest.raises(orchestrator.StateError, match="wrong development result hash"):
        orchestrator.sanitize_trend_audit_payload(payload, result_sha256="b" * 64)


def test_mean_reversion_direction_payload_is_no_data_and_allowlisted() -> None:
    payload = {
        "verdict": "REVISION_REQUIRED",
        "family_distinct_from_rejected_trend": True,
        "preserved_trend_terminal": (
            "btc-eth-vol-targeted-trend-v1=HISTORICAL_NO_GO_DEVELOPMENT/AUDIT_INCONCLUSIVE"
        ),
        "holdout_opened": False,
        "holdout_values_read": False,
        "raw_market_data_inspected": False,
        "performance_claim_made": False,
        "capital_permitted": 0,
        "strengths": ["distinct family"],
        "required_revisions": ["clarify holding clock"],
        "causal_timing_concerns": [],
        "statistical_concerns": ["sparse trades"],
        "rationale": ["review before freeze"],
        "unexpected_transcript": "must not persist",
    }
    sanitized = orchestrator.sanitize_mean_reversion_direction_payload(payload)
    assert sanitized["verdict"] == "REVISION_REQUIRED"
    assert "unexpected_transcript" not in sanitized
    with pytest.raises(orchestrator.StateError, match="no-data"):
        orchestrator.sanitize_mean_reversion_direction_payload(
            {**payload, "raw_market_data_inspected": True}
        )
    with pytest.raises(orchestrator.StateError, match="distinct family"):
        orchestrator.sanitize_mean_reversion_direction_payload(
            {**payload, "family_distinct_from_rejected_trend": False}
        )


def test_mean_reversion_audit_payload_is_rejection_only_and_allowlisted() -> None:
    result_hash = "c" * 64
    payload = {
        "verdict": "HISTORICAL_NO_GO_CONFIRMED",
        "preserved_prior_result": "cs-ranking-ptu-data-audit-v1=DATA_NO_GO",
        "development_classification": "HISTORICAL_NO_GO",
        "development_result_sha256": result_hash,
        "performance_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
        "holdout_opened": False,
        "holdout_values_read": False,
        "candidate_promoted": False,
        "capital_permitted": 0,
        "methodology_integrity": "sufficient for rejection",
        "gate_failures_confirmed": ["asset standalone", "bootstrap"],
        "critical_issues": [],
        "limitations": ["development only"],
        "rationale": ["six frozen gates failed"],
        "unexpected_transcript": "must not persist",
    }
    sanitized = orchestrator.sanitize_mean_reversion_audit_payload(
        payload, result_sha256=result_hash
    )
    assert sanitized["verdict"] == "HISTORICAL_NO_GO_CONFIRMED"
    assert "unexpected_transcript" not in sanitized
    with pytest.raises(orchestrator.StateError, match="closed-holdout"):
        orchestrator.sanitize_mean_reversion_audit_payload(
            {**payload, "holdout_opened": True}, result_sha256=result_hash
        )


def test_preregistration_hash_detects_mutation() -> None:
    prereg = orchestrator.preregistration({"family": "x", "hypothesis": "y"}, "abc")
    prereg["preregistration_sha256"] = orchestrator._sha(prereg)
    changed = dict(prereg)
    changed["target"] = "post-hoc target"
    digest = changed.pop("preregistration_sha256")
    assert digest != orchestrator._sha(changed)


def test_public_snapshot_allowlist_is_static_and_zero_capital(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "CURRENT_STATE.json"
    ledger = tmp_path / "EXPERIMENT_LEDGER.jsonl"
    manifest = tmp_path / "PUBLICATION_MANIFEST.json"
    state.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "program_state": "DATA_BLOCKED",
                "capital_permitted": 0,
                "next_task": "data",
                "budgets": {},
            }
        )
    )
    ledger.write_text(json.dumps({"experiment_id": "x", "classification": "DATA_NO_GO"}) + "\n")
    manifest.write_text(
        json.dumps(
            {
                "public_fields": [
                    "program_state",
                    "capital_permitted",
                    "experiment_id",
                    "classification",
                    "source_commit",
                    "preregistration_sha256",
                    "limitation",
                ],
                "prohibited_fields": ["credentials", "tokens", "absolute_paths"],
            }
        )
    )
    monkeypatch.setattr(orchestrator, "ROOT", tmp_path)
    monkeypatch.setattr(orchestrator, "STATE", state)
    monkeypatch.setattr(orchestrator, "LEDGER", ledger)
    monkeypatch.setattr(orchestrator, "PUBLICATION_LOG", tmp_path / "publication.jsonl")
    result = orchestrator.public_snapshot(dry_run=False)
    snapshot = json.loads((tmp_path / result["path"]).read_text())
    assert snapshot["capital_permitted"] == 0
    assert snapshot["classification"] == "DATA_NO_GO"
    assert "no candidate was promoted" in snapshot["limitation"]
    assert "capital remains zero" in snapshot["limitation"]


def test_public_snapshot_labels_development_rejection_without_candidate_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "CURRENT_STATE.json"
    ledger = tmp_path / "EXPERIMENT_LEDGER.jsonl"
    manifest = tmp_path / "PUBLICATION_MANIFEST.json"
    state.write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "program_state": "ACTIVE_RESEARCH",
                "capital_permitted": 0,
                "next_task": "next",
                "budgets": {},
            }
        )
    )
    ledger.write_text(
        json.dumps(
            {
                "experiment_id": "trend",
                "classification": "HISTORICAL_NO_GO",
                "performance_scope": "DEVELOPMENT_ONLY_NOT_A_CANDIDATE",
            }
        )
        + "\n"
    )
    manifest.write_text(
        json.dumps(
            {
                "public_fields": [
                    "program_state",
                    "capital_permitted",
                    "experiment_id",
                    "classification",
                    "source_commit",
                    "preregistration_sha256",
                    "limitation",
                ],
                "prohibited_fields": ["credentials", "tokens", "absolute_paths"],
            }
        )
    )
    monkeypatch.setattr(orchestrator, "ROOT", tmp_path)
    monkeypatch.setattr(orchestrator, "STATE", state)
    monkeypatch.setattr(orchestrator, "LEDGER", ledger)
    monkeypatch.setattr(orchestrator, "PUBLICATION_LOG", tmp_path / "publication.jsonl")
    result = orchestrator.public_snapshot(dry_run=False)
    snapshot = json.loads((tmp_path / result["path"]).read_text())
    assert "final holdout remained closed" in snapshot["limitation"]
    assert "no candidate was promoted" in snapshot["limitation"]


def test_public_snapshot_preserves_phase_1_while_reporting_phase_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "CURRENT_STATE.json"
    ledger = tmp_path / "EXPERIMENT_LEDGER.jsonl"
    manifest = tmp_path / "PUBLICATION_MANIFEST.json"
    state.write_text(
        json.dumps(
            {
                "schema_version": "2.0",
                "program_state": "ACTIVE_RESEARCH_PHASE_2",
                "capital_permitted": 0,
                "next_task": "production integration",
                "budgets": {},
                "preregistration_sha256": "frozen-v2-preregistration",
                "phase_1_terminal": {
                    "final_result": "APPROVED_SPACE_EXHAUSTED / RESEARCH_BUDGET_EXHAUSTED"
                },
            }
        )
    )
    ledger.write_text(
        json.dumps(
            {
                "experiment_id": "btc-eth-long-only-mean-reversion-v2",
                "classification": "PASS_PURE_PRE_DATA",
            }
        )
        + "\n"
    )
    manifest.write_text(
        json.dumps(
            {
                "public_fields": [
                    "program_state",
                    "capital_permitted",
                    "experiment_id",
                    "classification",
                    "source_commit",
                    "preregistration_sha256",
                    "limitation",
                ],
                "prohibited_fields": ["credentials", "tokens", "absolute_paths"],
            }
        )
    )
    monkeypatch.setattr(orchestrator, "ROOT", tmp_path)
    monkeypatch.setattr(orchestrator, "STATE", state)
    monkeypatch.setattr(orchestrator, "LEDGER", ledger)
    monkeypatch.setattr(orchestrator, "PUBLICATION_LOG", tmp_path / "publication.jsonl")
    result = orchestrator.public_snapshot(dry_run=False)
    snapshot = json.loads((tmp_path / result["path"]).read_text())
    assert snapshot["program_state"] == "ACTIVE_RESEARCH_PHASE_2"
    assert snapshot["classification"] == "PASS_PURE_PRE_DATA"
    assert snapshot["preregistration_sha256"] == "frozen-v2-preregistration"
    assert "APPROVED_SPACE_EXHAUSTED / RESEARCH_BUDGET_EXHAUSTED" in snapshot["limitation"]
    assert "implementation evidence only" in snapshot["limitation"]
    assert "no Phase 2 economic result exists" in snapshot["limitation"]
