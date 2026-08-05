"""Fail-closed deterministic pre-review validation for Phase 2 Route 4.

The validator reads contract artifacts and deterministic fixtures only. It has no
network, market-data, strategy, return, credential, order, or holdout interface.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from strategy_control.route4_contract import (
    ATTEMPT_REQUIRED_FIELDS,
    EXPERIMENT_ID,
    NO_RESPONSE_OUTCOMES,
    OUTCOMES,
    TERMINAL_REQUIRED_FIELDS,
    Route4ContractError,
    apply_json_pointer_operations,
    assert_acyclic,
    build_fixture_ledgers,
    byte_sha256,
    canonical_sha256,
    find_hash_strings,
    parsed_canonical_sha256,
    validate_fixture_scenarios,
    validate_ledger,
)

EXPERIMENT_DIRECTORY = Path("experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3")
BASE_DRAFT = EXPERIMENT_DIRECTORY / "PREREGISTRATION_DRAFT.json"
DRAFT_V2 = EXPERIMENT_DIRECTORY / "PREREGISTRATION_DRAFT_V2.json"
OUTPUT_SCHEMA_V2 = EXPERIMENT_DIRECTORY / "DIRECTION_DELTA_REVIEW_OUTPUT_SCHEMA_V2.json"
NETWORK_SCHEMA_V1 = EXPERIMENT_DIRECTORY / "NETWORK_EVENT_LEDGER_SCHEMA_V1.json"
AUTHORIZATION = EXPERIMENT_DIRECTORY / "FINAL_PROVENANCE_ARCHITECTURE_CORRECTION_AUTHORIZATION.json"
ATTEMPT4 = EXPERIMENT_DIRECTORY / "DIRECTION_REVIEW_ATTEMPT_4.json"

BASE_DRAFT_BYTE_SHA256 = "756f0be8111f93062bb033b3bbeeeba14ab556a44f530724215f26dbfd6c5774"
BASE_DRAFT_CANONICAL_SHA256 = "48ff7c37eaec1babf5a463f90900b6f188e99c8dd38056c351509b461c61d86e"
ATTEMPT4_BYTE_SHA256 = "2e65ca54291e4852d0e36b3372e6df4e8176e01d7dcb73cb066e735c8af45292"
ATTEMPT4_CANONICAL_SHA256 = "23588f15d16fd640d6942d6b1bd82cd926e01b0064e895f559f3f559db699b83"
SUPERSEDED_DRAFT_HASH = "144d7957450728650bcaa537807a5485745af2b19f04a3122ba5ef558fb7a2d7"

CONTENT_PATHS = (
    DRAFT_V2,
    OUTPUT_SCHEMA_V2,
    NETWORK_SCHEMA_V1,
    AUTHORIZATION,
    Path("src/strategy_control/route4_contract.py"),
    Path("src/strategy_control/route4_preflight.py"),
    Path("tests/test_route4_contract.py"),
    Path("tests/test_route4_preflight.py"),
)

ALLOWED_OPERATION_PATHS = (
    "/status",
    "/raw_evidence_retention_contract/write_protocol",
    "/raw_evidence_retention_contract/logical_request_identity_contract",
    "/raw_evidence_retention_contract/raw_evidence_index_record_schema",
    "/raw_evidence_retention_contract/network_event_ledger_contract",
    "/production_artifact_contract/local_untracked_artifacts/RAW_EVIDENCE_INDEX.jsonl",
    "/production_artifact_contract/local_untracked_artifacts/NETWORK_EVENT_LEDGER_V1.jsonl",
    "/production_artifact_contract/local_untracked_artifacts/ACQUISITION_STATE.json",
    "/production_artifact_contract/cross_artifact_invariants/2",
    "/production_artifact_contract/cross_artifact_invariants/3",
    "/real_production_path_requirements/independent_reconstruction",
    "/independent_audit_contract/required_input_bindings/3",
    "/required_tests_before_production_acquisition/41",
    "/required_tests_before_production_acquisition/42",
    "/required_tests_before_production_acquisition/43",
    "/required_tests_before_production_acquisition/44",
    "/required_tests_before_production_acquisition/45",
    "/provenance_architecture_v2",
)


def _load_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise Route4ContractError(f"expected JSON object: {path}")
    return cast(dict[str, object], value)


def _require_mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise Route4ContractError(f"{name} must be an object")
    return cast(Mapping[str, object], value)


def _require_sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, list):
        raise Route4ContractError(f"{name} must be an array")
    return cast(Sequence[object], value)


def _expect_failure(name: str, records: Sequence[Mapping[str, object]]) -> str:
    try:
        validate_ledger(records)
    except Route4ContractError as exc:
        return f"PASS_FAIL_CLOSED: {name}: {exc}"
    raise Route4ContractError(f"negative fixture did not fail closed: {name}")


def _negative_fixture_results() -> list[str]:
    ledgers = build_fixture_ledgers()
    results: list[str] = []

    truncated = ledgers["retry_exhaustion"][:-1]
    results.append(_expect_failure("ledger_truncation", truncated))

    retry_records = ledgers["retry_exhaustion"]
    reordered = [retry_records[1], retry_records[0], *retry_records[2:]]
    results.append(_expect_failure("ledger_reordering", reordered))

    mutated = deepcopy(ledgers["successful_http_response"])
    mutated[0]["request_url"] = "https://invalid.example/mutation"
    results.append(_expect_failure("existing_record_mutation", mutated))

    duplicate_attempt = deepcopy(ledgers["retry_exhaustion"])
    duplicate_attempt.insert(1, deepcopy(duplicate_attempt[0]))
    results.append(_expect_failure("duplicate_attempt_id", duplicate_attempt))

    duplicate_terminal = [
        *ledgers["successful_http_response"],
        deepcopy(ledgers["successful_http_response"][-1]),
    ]
    results.append(_expect_failure("duplicate_terminal_id", duplicate_terminal))

    missing_attempt = [deepcopy(ledgers["successful_http_response"][-1])]
    results.append(_expect_failure("terminal_without_attempt", missing_attempt))

    return results


def _check_network_schema(schema: Mapping[str, object]) -> None:
    definitions = _require_mapping(schema.get("$defs"), "network schema $defs")
    attempt = _require_mapping(definitions.get("attempt"), "attempt schema")
    terminal = _require_mapping(definitions.get("terminal"), "terminal schema")
    attempt_required = set(
        cast(Sequence[str], _require_sequence(attempt.get("required"), "attempt required"))
    )
    terminal_required = set(
        cast(
            Sequence[str],
            _require_sequence(terminal.get("required"), "terminal required"),
        )
    )
    if attempt_required != ATTEMPT_REQUIRED_FIELDS:
        raise Route4ContractError("network ATTEMPT schema and validator fields differ")
    if terminal_required != TERMINAL_REQUIRED_FIELDS:
        raise Route4ContractError("network TERMINAL schema and validator fields differ")
    attempt_properties = _require_mapping(attempt.get("properties"), "attempt properties")
    outcome_property = _require_mapping(
        attempt_properties.get("observable_outcome"), "observable outcome property"
    )
    outcomes = set(cast(Sequence[str], _require_sequence(outcome_property.get("enum"), "outcomes")))
    if outcomes != OUTCOMES:
        raise Route4ContractError("network outcome schema and validator differ")


def _review_check_traceability() -> list[dict[str, object]]:
    rows = [
        (
            0,
            "content_commit_and_hash_binding",
            "EXECUTABLE_DETERMINISTIC",
            "exact_hash_and_clean_content_commit",
        ),
        (
            1,
            "non_circular_provenance_binding",
            "EXECUTABLE_DETERMINISTIC_PLUS_NARROW_REVIEW",
            "stale_hash_and_dependency_cycle_rejection",
        ),
        (
            2,
            "phase1_and_prior_verdict_immutability",
            "EXECUTABLE_DETERMINISTIC",
            "historical_hash_preservation",
        ),
        (
            3,
            "new_official_bytes_only",
            "HUMAN_METHODOLOGY_ONLY",
            "No acquisition is authorized; review confirms the unchanged frozen "
            "future-acquisition clause.",
        ),
        (
            4,
            "raw_listing_reconstruction",
            "HUMAN_METHODOLOGY_ONLY",
            "No data exist yet; review confirms the unchanged raw-response reconstruction design.",
        ),
        (
            5,
            "root_and_101_month_scope",
            "EXECUTABLE_DETERMINISTIC",
            "closed_delta_path_and_predecessor_hash",
        ),
        (
            6,
            "redirect_and_evidence_cross_binding",
            "HUMAN_METHODOLOGY_ONLY",
            "The transport design is unchanged; production behavior can be tested "
            "only after separate implementation authorization.",
        ),
        (
            7,
            "zip_csv_timestamp_validation",
            "EXECUTABLE_DETERMINISTIC",
            "closed_delta_path_and_predecessor_hash",
        ),
        (
            8,
            "exhaustive_response_and_no_response_attempts",
            "EXECUTABLE_DETERMINISTIC_PLUS_NARROW_REVIEW",
            "fixture_union_serialization_attempt_count_and_lineage",
        ),
        (
            9,
            "raw_only_terminal_reconstruction",
            "EXECUTABLE_DETERMINISTIC_PLUS_NARROW_REVIEW",
            "immutable_ledger_reconstruction_and_negative_fixtures",
        ),
        (
            10,
            "symbol_label_identity_and_gap_recovery",
            "EXECUTABLE_DETERMINISTIC",
            "closed_delta_path_and_predecessor_hash",
        ),
        (
            11,
            "causal_eligibility_and_liquidity",
            "EXECUTABLE_DETERMINISTIC",
            "closed_delta_path_and_predecessor_hash",
        ),
        (
            12,
            "last_bar_metadata_noncausality",
            "EXECUTABLE_DETERMINISTIC",
            "closed_delta_path_and_predecessor_hash",
        ),
        (
            13,
            "exact_execution_fail_closed",
            "EXECUTABLE_DETERMINISTIC",
            "closed_delta_path_and_predecessor_hash",
        ),
        (
            14,
            "archive_and_identity_claim_limits",
            "HUMAN_METHODOLOGY_ONLY",
            "Review confirms the unchanged limitations language is not invalidated "
            "by the provenance delta.",
        ),
        (
            15,
            "data_sufficiency_gates",
            "HUMAN_METHODOLOGY_ONLY",
            "The numerical gates are unchanged and require methodological "
            "confirmation, not new data in this review.",
        ),
        (
            16,
            "finite_resource_and_failure_boundaries",
            "EXECUTABLE_DETERMINISTIC",
            "closed_delta_path_and_predecessor_hash",
        ),
        (
            17,
            "data_go_is_noneconomic",
            "EXECUTABLE_DETERMINISTIC",
            "authorization_and_prohibited_action_flags",
        ),
        (
            18,
            "attempt3_revision_regression",
            "HUMAN_METHODOLOGY_ONLY",
            "Narrow review confirms the bounded delta did not invalidate the "
            "previously passing substantive clauses.",
        ),
    ]
    return [
        {
            "check": check,
            "requirement": requirement,
            "validator_type": validator_type,
            "validator_or_explanation": validator,
            "preflight_disposition": (
                "PASS_DETERMINISTIC_PENDING_NARROW_REVIEW"
                if check in {1, 8, 9}
                else "UNCHANGED_OR_STRENGTHENED_FROM_ATTEMPT_4_PASS"
            ),
        }
        for check, requirement, validator_type, validator in rows
    ]


def validate_route4_content(repo_root: Path) -> dict[str, object]:
    base_path = repo_root / BASE_DRAFT
    draft_path = repo_root / DRAFT_V2
    output_schema_path = repo_root / OUTPUT_SCHEMA_V2
    network_schema_path = repo_root / NETWORK_SCHEMA_V1
    authorization_path = repo_root / AUTHORIZATION
    attempt4_path = repo_root / ATTEMPT4

    if byte_sha256(base_path) != BASE_DRAFT_BYTE_SHA256:
        raise Route4ContractError("historical predecessor draft byte hash changed")
    if parsed_canonical_sha256(base_path) != BASE_DRAFT_CANONICAL_SHA256:
        raise Route4ContractError("historical predecessor draft canonical hash changed")
    if byte_sha256(attempt4_path) != ATTEMPT4_BYTE_SHA256:
        raise Route4ContractError("direction-review attempt 4 byte hash changed")
    if parsed_canonical_sha256(attempt4_path) != ATTEMPT4_CANONICAL_SHA256:
        raise Route4ContractError("direction-review attempt 4 canonical hash changed")

    base = _load_json(base_path)
    draft = _load_json(draft_path)
    output_schema = _load_json(output_schema_path)
    network_schema = _load_json(network_schema_path)
    authorization = _load_json(authorization_path)
    if draft.get("schema_version") != "2.0":
        raise Route4ContractError("wrong revised draft version")
    if authorization.get("experiment_id") != EXPERIMENT_ID:
        raise Route4ContractError("wrong correction authorization experiment")
    if output_schema.get("type") != "object":
        raise Route4ContractError("review output schema is not a closed object schema")
    if output_schema.get("additionalProperties") is not False:
        raise Route4ContractError("review output schema must reject extra fields")
    _check_network_schema(network_schema)

    operations = _require_sequence(draft.get("operations"), "draft operations")
    operation_paths: list[str] = []
    for operation_value in operations:
        operation = _require_mapping(operation_value, "draft operation")
        path = operation.get("path")
        if not isinstance(path, str):
            raise Route4ContractError("draft operation path must be a string")
        operation_paths.append(path)
    if tuple(operation_paths) != ALLOWED_OPERATION_PATHS:
        raise Route4ContractError("draft operation paths exceed or differ from authorization")
    effective = apply_json_pointer_operations(
        base, cast(Sequence[Mapping[str, object]], operations)
    )

    raw_contract = _require_mapping(
        effective.get("raw_evidence_retention_contract"), "raw evidence contract"
    )
    if "raw_evidence_index_record_schema" in raw_contract:
        raise Route4ContractError("superseded response-only schema remains effective")
    if "network_event_ledger_contract" not in raw_contract:
        raise Route4ContractError("network-event-ledger contract is missing")
    production = _require_mapping(
        effective.get("production_artifact_contract"), "production artifact contract"
    )
    local_artifacts = _require_mapping(
        production.get("local_untracked_artifacts"), "local untracked artifacts"
    )
    if "RAW_EVIDENCE_INDEX.jsonl" in local_artifacts:
        raise Route4ContractError("superseded raw evidence index remains authoritative")
    if "NETWORK_EVENT_LEDGER_V1.jsonl" not in local_artifacts:
        raise Route4ContractError("immutable event ledger artifact is missing")

    output_hashes = find_hash_strings(output_schema)
    if SUPERSEDED_DRAFT_HASH in output_hashes:
        raise Route4ContractError("review schema retains superseded embedded draft hash")
    if BASE_DRAFT_CANONICAL_SHA256 in output_hashes:
        raise Route4ContractError("review schema embeds predecessor draft hash")
    if output_hashes:
        raise Route4ContractError("review schema embeds exact content hashes")
    assert_acyclic(
        {
            "phase2_authorization": [],
            "route4_authorization": ["phase2_authorization"],
            "correction_authorization": ["route4_authorization"],
            "predecessor_draft": ["route4_authorization"],
            "draft_v2": ["predecessor_draft", "correction_authorization", "network_schema"],
            "network_schema": [],
            "review_output_schema": [],
            "pre_review_bundle": [
                "draft_v2",
                "review_output_schema",
                "network_schema",
                "correction_authorization",
            ],
            "review_result": ["pre_review_bundle"],
            "freeze_manifest": ["review_result", "pre_review_bundle"],
        }
    )

    scenario_results = validate_fixture_scenarios()
    scenario_names = {result.name for result in scenario_results}
    required_names = {
        "successful_http_response",
        "http_error_response_nonretryable",
        "timeout_without_response",
        "dns_failure",
        "connection_establishment_failure",
        "tls_failure",
        "connection_reset_before_response",
        "unknown_transport_failure",
        "partial_transport_knowledge",
        "retryable_http_response_then_success",
        "retry_exhaustion",
    }
    if scenario_names != required_names:
        raise Route4ContractError("deterministic scenario set is incomplete")
    for records in build_fixture_ledgers().values():
        for record in records:
            if (
                record.get("record_type") == "ATTEMPT"
                and record.get("observable_outcome") in NO_RESPONSE_OUTCOMES
            ):
                response_only = (
                    "http_status",
                    "final_url",
                    "response_headers",
                    "body_byte_count",
                    "response_body_sha256",
                    "retained_blob_reference",
                )
                if record.get("response_received") is not False or any(
                    record.get(field) is not None for field in response_only
                ):
                    raise Route4ContractError("no-response fixture invents response fields")

    negative_results = _negative_fixture_results()
    check_traceability = _review_check_traceability()
    if len(check_traceability) != 19:
        raise Route4ContractError("review-check traceability must cover all 19 checks")

    return {
        "artifact_hashes": {
            "predecessor_draft_byte_sha256": byte_sha256(base_path),
            "predecessor_draft_canonical_sha256": parsed_canonical_sha256(base_path),
            "revised_draft_byte_sha256": byte_sha256(draft_path),
            "revised_draft_canonical_sha256": parsed_canonical_sha256(draft_path),
            "resolved_effective_draft_canonical_sha256": canonical_sha256(effective),
            "review_output_schema_byte_sha256": byte_sha256(output_schema_path),
            "review_output_schema_canonical_sha256": parsed_canonical_sha256(output_schema_path),
            "network_event_schema_byte_sha256": byte_sha256(network_schema_path),
            "network_event_schema_canonical_sha256": parsed_canonical_sha256(network_schema_path),
            "authorization_byte_sha256": byte_sha256(authorization_path),
            "authorization_canonical_sha256": parsed_canonical_sha256(authorization_path),
            "attempt4_byte_sha256": byte_sha256(attempt4_path),
            "attempt4_canonical_sha256": parsed_canonical_sha256(attempt4_path),
        },
        "semantic_checks": {
            "historical_hashes_preserved": True,
            "draft_and_schema_versions_exact": True,
            "closed_delta_paths_exact": True,
            "stale_embedded_hashes_absent": True,
            "reciprocal_content_hashes_absent": True,
            "content_dependency_graph_acyclic": True,
            "response_and_no_response_union_complete": True,
            "issued_attempts_counted_ordered_and_distinct": True,
            "unique_terminal_outcomes_enforced": True,
            "raw_only_reconstruction_passed": True,
            "mutable_acquisition_state_required": False,
            "truncation_reordering_and_mutation_fail_closed": True,
            "other_sixteen_checks_unchanged_or_strengthened": True,
        },
        "fixture_scenarios": [
            {
                "name": result.name,
                "attempt_count": result.attempt_count,
                "terminal_state": result.terminal_state,
            }
            for result in scenario_results
        ],
        "negative_fixture_results": negative_results,
        "review_check_traceability": check_traceability,
        "prohibited_action_counters": {
            "network_acquisition_attempts": 0,
            "market_data_rows_accessed": 0,
            "model_training_runs": 0,
            "backtests": 0,
            "return_calculations": 0,
            "holdout_paths_resolved": 0,
            "holdout_accesses": 0,
        },
        "content_verdict": "PASS",
    }


def _verify_content_at_commit(repo_root: Path, commit: str) -> list[str]:
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise Route4ContractError("source content commit must be a full lowercase Git SHA")
    verified: list[str] = []
    for relative_path in CONTENT_PATHS:
        result = subprocess.run(
            ["git", "show", f"{commit}:{relative_path.as_posix()}"],
            cwd=repo_root,
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            error = result.stderr.decode("utf-8", errors="replace").strip()
            raise Route4ContractError(
                f"content path absent from source commit: {relative_path}: {error}"
            )
        if result.stdout != (repo_root / relative_path).read_bytes():
            raise Route4ContractError(
                f"PRE_REVIEW_CONTENT_MUTATION: {relative_path} differs from {commit}"
            )
        verified.append(relative_path.as_posix())
    return verified


def build_preflight_report(
    repo_root: Path, source_content_commit: str, created_at_utc: str
) -> dict[str, object]:
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    content = validate_route4_content(repo_root)
    verified_paths = _verify_content_at_commit(repo_root, source_content_commit)
    completed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return {
        "schema_version": "1.0",
        "experiment_id": EXPERIMENT_ID,
        "validator": "route4_contract_preflight",
        "invocation_mode": "deterministic_local",
        "created_at_utc": created_at_utc,
        "stage_timestamps": {
            "preflight_started_at_utc": started_at,
            "preflight_completed_at_utc": completed_at,
        },
        "source_content_commit": source_content_commit,
        "content_paths_verified_at_commit": verified_paths,
        **content,
        "verdict": "PASS",
        "acquisition_authorized": False,
        "implementation_authorized": False,
        "review_authorized_after_bundle_only": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--source-content-commit", required=True)
    parser.add_argument("--created-at-utc", required=True)
    arguments = parser.parse_args()
    report = build_preflight_report(
        arguments.repo_root.resolve(),
        arguments.source_content_commit,
        arguments.created_at_utc,
    )
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
