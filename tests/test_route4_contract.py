from __future__ import annotations

from copy import deepcopy

import pytest

from strategy_control.route4_contract import (
    NO_RESPONSE_OUTCOMES,
    Route4ContractError,
    apply_json_pointer_operations,
    assert_acyclic,
    build_fixture_ledgers,
    canonical_sha256,
    validate_attempt,
    validate_fixture_scenarios,
    validate_ledger,
    with_record_sha256,
)


def test_all_required_response_and_no_response_scenarios_reconstruct() -> None:
    results = {result.name: result for result in validate_fixture_scenarios()}
    assert {
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
    } == set(results)
    assert results["successful_http_response"].terminal_state == "SUCCEEDED"
    assert results["retryable_http_response_then_success"].attempt_count == 2
    assert results["retry_exhaustion"].attempt_count == 3


def test_no_response_attempts_require_explicit_null_response_fields() -> None:
    response_fields = (
        "http_status",
        "final_url",
        "response_headers",
        "body_byte_count",
        "response_body_sha256",
        "retained_blob_reference",
    )
    for records in build_fixture_ledgers().values():
        for record in records:
            if record["record_type"] != "ATTEMPT":
                continue
            if record["observable_outcome"] in NO_RESPONSE_OUTCOMES:
                assert record["response_received"] is False
                assert all(record[field] is None for field in response_fields)
                validate_attempt(record)


def test_issued_attempts_remain_distinct_and_counted() -> None:
    records = build_fixture_ledgers()["retry_exhaustion"]
    attempts = records[:-1]
    assert len(attempts) == 3
    assert len({record["attempt_id"] for record in attempts}) == 3
    assert [record["attempt_ordinal"] for record in attempts] == [1, 2, 3]
    validate_ledger(records)


def test_duplicate_attempt_id_fails_closed() -> None:
    records = deepcopy(build_fixture_ledgers()["retry_exhaustion"])
    records[1]["attempt_id"] = records[0]["attempt_id"]
    records[1] = with_record_sha256(records[1])
    with pytest.raises(Route4ContractError):
        validate_ledger(records)


def test_duplicate_terminal_id_and_multiple_terminal_outcomes_fail_closed() -> None:
    records = build_fixture_ledgers()["successful_http_response"]
    duplicate = deepcopy(records[-1])
    with pytest.raises(Route4ContractError, match="multiple terminal"):
        validate_ledger([*records, duplicate])


def test_missing_terminal_evidence_and_ledger_truncation_fail_closed() -> None:
    records = build_fixture_ledgers()["retry_exhaustion"]
    with pytest.raises(Route4ContractError, match="missing terminal"):
        validate_ledger(records[:-1])
    with pytest.raises(Route4ContractError):
        validate_ledger([*records[:2], records[-1]])


def test_reordered_attempts_fail_closed() -> None:
    records = build_fixture_ledgers()["retry_exhaustion"]
    reordered = [records[1], records[0], *records[2:]]
    with pytest.raises(Route4ContractError):
        validate_ledger(reordered)


def test_mutation_of_existing_append_only_record_fails_hash_validation() -> None:
    records = deepcopy(build_fixture_ledgers()["successful_http_response"])
    records[0]["request_url"] = "https://invalid.example/mutated"
    with pytest.raises(Route4ContractError, match="canonical record hash mismatch"):
        validate_ledger(records)


def test_terminal_reconstruction_uses_no_mutable_acquisition_state() -> None:
    records = build_fixture_ledgers()["retryable_http_response_then_success"]
    fake_mutable_state = {"terminal_state": "FAILED", "authoritative": False}
    reconstructed = validate_ledger(records)
    assert set(reconstructed.values()) == {"SUCCEEDED"}
    assert fake_mutable_state["terminal_state"] != next(iter(reconstructed.values()))


def test_partial_transport_knowledge_never_invents_unknown_stages() -> None:
    record = build_fixture_ledgers()["partial_transport_knowledge"][0]
    assert record["response_received"] is False
    assert record["transport_observations"] == {
        "dns_resolution": "SUCCEEDED",
        "connection_establishment": "SUCCEEDED",
        "tls_handshake": "UNKNOWN",
        "request_transmission": "UNKNOWN",
        "response_wait": "UNKNOWN",
    }


def test_content_dependency_cycles_are_rejected() -> None:
    assert_acyclic({"draft": ["base", "network_schema"], "output_schema": []})
    with pytest.raises(Route4ContractError, match="cyclic"):
        assert_acyclic({"draft": ["schema"], "schema": ["draft"]})


def test_versioned_draft_operations_do_not_mutate_base() -> None:
    base = {"a": {"old": 1}, "items": ["base"]}
    original_hash = canonical_sha256(base)
    resolved = apply_json_pointer_operations(
        base,
        [
            {"op": "replace", "path": "/a/old", "value": 2},
            {"op": "add", "path": "/a/new", "value": 3},
            {"op": "remove", "path": "/items/0"},
        ],
    )
    assert resolved == {"a": {"old": 2, "new": 3}, "items": []}
    assert canonical_sha256(base) == original_hash
