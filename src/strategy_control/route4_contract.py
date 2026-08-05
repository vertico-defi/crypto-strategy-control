"""Deterministic no-data provenance mechanics for Phase 2 Route 4.

This module validates immutable network ATTEMPT and TERMINAL evidence.  It has no
network client, market-data reader, strategy, return, credential, or order surface.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

EXPERIMENT_ID = "cs-ranking-binance-spot-archive-ptu-acquisition-v3"
SCHEMA_VERSION = "1.0"
DECISION_RULE = "route4_retry_and_terminal_policy_v1"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
RESPONSE_OUTCOMES = frozenset({"SUCCESS_HTTP", "HTTP_ERROR_RESPONSE"})
NO_RESPONSE_OUTCOMES = frozenset(
    {
        "TIMEOUT_NO_RESPONSE",
        "DNS_FAILURE",
        "CONNECTION_ESTABLISHMENT_FAILURE",
        "TLS_FAILURE",
        "CONNECTION_RESET_BEFORE_RESPONSE",
        "UNKNOWN_TRANSPORT_FAILURE",
    }
)
OUTCOMES = RESPONSE_OUTCOMES | NO_RESPONSE_OUTCOMES
NEXT_ACTIONS = frozenset(
    {
        "ACCEPT_RESPONSE",
        "RETRY",
        "TERMINATE_NONRETRYABLE",
        "TERMINATE_RETRIES_EXHAUSTED",
    }
)
RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
RETRYABLE_NO_RESPONSE_OUTCOMES = frozenset(
    {
        "TIMEOUT_NO_RESPONSE",
        "DNS_FAILURE",
        "TLS_FAILURE",
        "CONNECTION_RESET_BEFORE_RESPONSE",
    }
)
TERMINAL_STATES = frozenset({"SUCCEEDED", "NONRETRYABLE_FAILED", "RETRIES_EXHAUSTED"})
DISPOSITIONS = frozenset({"ACCEPTED", "EXCLUDED", "QUARANTINED", "UNAVAILABLE", "FAILED"})
TRANSPORT_STATES = frozenset({"SUCCEEDED", "FAILED", "UNKNOWN"})
ATTEMPT_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "experiment_id",
        "record_type",
        "attempt_id",
        "acquisition_run_id",
        "logical_request_id",
        "pagination_parent_request_id",
        "deterministic_request_identity",
        "request_kind",
        "request_url",
        "object_key",
        "method",
        "attempt_ordinal",
        "prior_attempt_id",
        "started_at_utc",
        "ended_at_utc",
        "configured_timeout_seconds",
        "response_received",
        "http_status",
        "final_url",
        "response_headers",
        "body_byte_count",
        "response_body_sha256",
        "retained_blob_reference",
        "observable_outcome",
        "observable_exception_category",
        "transport_observations",
        "retryable_under_frozen_policy",
        "next_action",
        "evidence_references",
        "canonical_record_sha256",
    }
)
TERMINAL_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "experiment_id",
        "record_type",
        "terminal_record_id",
        "acquisition_run_id",
        "logical_request_id",
        "ordered_attempt_ids",
        "ordered_attempt_ids_sha256",
        "terminal_attempt_id",
        "terminal_state",
        "terminal_reason_code",
        "terminal_at_utc",
        "frozen_decision_rule",
        "any_response_received",
        "bytes_obtained",
        "checksum_verification_occurred",
        "parsing_occurred",
        "disposition",
        "retained_evidence_references",
        "evidence_hashes",
        "ledger_prefix_sha256",
        "canonical_record_sha256",
    }
)


class Route4ContractError(RuntimeError):
    """Raised when Route 4 provenance evidence fails closed."""


@dataclass(frozen=True)
class ScenarioResult:
    name: str
    attempt_count: int
    terminal_state: str


def canonical_json_bytes(value: object) -> bytes:
    """Canonical UTF-8 JSON used for every Route 4 content identity."""
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise Route4ContractError("value is not canonical JSON") from exc


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def byte_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parsed_canonical_sha256(path: Path) -> str:
    return canonical_sha256(json.loads(path.read_text(encoding="utf-8")))


def record_sha256(record: Mapping[str, object]) -> str:
    body = {key: value for key, value in record.items() if key != "canonical_record_sha256"}
    return canonical_sha256(body)


def with_record_sha256(record: Mapping[str, object]) -> dict[str, object]:
    result = dict(record)
    result["canonical_record_sha256"] = record_sha256(result)
    return result


def request_identity(*, request_kind: str, request_url: str, object_key: str | None) -> str:
    return canonical_sha256(
        [SCHEMA_VERSION, EXPERIMENT_ID, request_kind, "GET", request_url, object_key]
    )


def logical_request_id(acquisition_run_id: str, deterministic_request_identity: str) -> str:
    return canonical_sha256([EXPERIMENT_ID, acquisition_run_id, deterministic_request_identity])


def attempt_id(acquisition_run_id: str, request_id: str, ordinal: int) -> str:
    return canonical_sha256([EXPERIMENT_ID, acquisition_run_id, request_id, ordinal])


def terminal_record_id(acquisition_run_id: str, request_id: str) -> str:
    return canonical_sha256([EXPERIMENT_ID, acquisition_run_id, request_id, "TERMINAL"])


def ordered_ids_sha256(ids: Sequence[str]) -> str:
    return canonical_sha256(list(ids))


def ledger_prefix_sha256(records: Sequence[Mapping[str, object]]) -> str:
    return canonical_sha256([_require_sha(record, "canonical_record_sha256") for record in records])


def _require_exact_keys(record: Mapping[str, object], expected: frozenset[str]) -> None:
    actual = frozenset(record)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise Route4ContractError(f"record fields mismatch missing={missing} extra={extra}")


def _require_str(record: Mapping[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise Route4ContractError(f"{key} must be a nonempty string")
    return value


def _require_sha(record: Mapping[str, object], key: str) -> str:
    value = _require_str(record, key)
    if SHA256_PATTERN.fullmatch(value) is None:
        raise Route4ContractError(f"{key} must be lowercase SHA-256")
    return value


def _require_bool(record: Mapping[str, object], key: str) -> bool:
    value = record.get(key)
    if not isinstance(value, bool):
        raise Route4ContractError(f"{key} must be boolean")
    return value


def _require_int(record: Mapping[str, object], key: str, *, minimum: int = 0) -> int:
    value = record.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise Route4ContractError(f"{key} must be integer >= {minimum}")
    return value


def _require_optional_str(record: Mapping[str, object], key: str) -> str | None:
    value = record.get(key)
    if value is not None and not isinstance(value, str):
        raise Route4ContractError(f"{key} must be string or null")
    return value


def _require_optional_sha(record: Mapping[str, object], key: str) -> str | None:
    value = _require_optional_str(record, key)
    if value is not None and SHA256_PATTERN.fullmatch(value) is None:
        raise Route4ContractError(f"{key} must be lowercase SHA-256 or null")
    return value


def _require_string_list(record: Mapping[str, object], key: str) -> list[str]:
    value = record.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise Route4ContractError(f"{key} must be a list of nonempty strings")
    return cast(list[str], value)


def _validate_base(record: Mapping[str, object], record_type: str) -> None:
    if record.get("schema_version") != SCHEMA_VERSION:
        raise Route4ContractError("wrong record schema version")
    if record.get("experiment_id") != EXPERIMENT_ID:
        raise Route4ContractError("wrong experiment ID")
    if record.get("record_type") != record_type:
        raise Route4ContractError(f"record_type must be {record_type}")
    expected = _require_sha(record, "canonical_record_sha256")
    if record_sha256(record) != expected:
        raise Route4ContractError("canonical record hash mismatch")


def validate_attempt(record: Mapping[str, object]) -> None:
    """Validate one immutable attempt without inventing unobserved transport facts."""
    _require_exact_keys(record, ATTEMPT_REQUIRED_FIELDS)
    _validate_base(record, "ATTEMPT")
    run_id = _require_sha(record, "acquisition_run_id")
    request_id_value = _require_sha(record, "logical_request_id")
    deterministic_identity = _require_sha(record, "deterministic_request_identity")
    ordinal = _require_int(record, "attempt_ordinal", minimum=1)
    if ordinal > 3:
        raise Route4ContractError("attempt ordinal exceeds frozen maximum")
    if _require_sha(record, "attempt_id") != attempt_id(run_id, request_id_value, ordinal):
        raise Route4ContractError("attempt ID does not match immutable identity")
    request_kind = _require_str(record, "request_kind")
    request_url = _require_str(record, "request_url")
    object_key = _require_optional_str(record, "object_key")
    if deterministic_identity != request_identity(
        request_kind=request_kind, request_url=request_url, object_key=object_key
    ):
        raise Route4ContractError("deterministic request identity mismatch")
    if logical_request_id(run_id, deterministic_identity) != request_id_value:
        raise Route4ContractError("logical request ID mismatch")
    if record.get("method") != "GET":
        raise Route4ContractError("only GET is permitted")
    _require_optional_sha(record, "pagination_parent_request_id")
    prior = _require_optional_sha(record, "prior_attempt_id")
    if (ordinal == 1) != (prior is None):
        raise Route4ContractError("prior attempt lineage does not match ordinal")
    _require_str(record, "started_at_utc")
    _require_str(record, "ended_at_utc")
    _require_int(record, "configured_timeout_seconds", minimum=1)
    response_received = _require_bool(record, "response_received")
    outcome = _require_str(record, "observable_outcome")
    if outcome not in OUTCOMES:
        raise Route4ContractError("unknown observable outcome")
    _require_optional_str(record, "observable_exception_category")
    observations = record.get("transport_observations")
    if not isinstance(observations, dict) or frozenset(observations) != frozenset(
        {
            "dns_resolution",
            "connection_establishment",
            "tls_handshake",
            "request_transmission",
            "response_wait",
        }
    ):
        raise Route4ContractError("transport observations have wrong fields")
    for value in observations.values():
        if value not in TRANSPORT_STATES:
            raise Route4ContractError("transport observation must be known or UNKNOWN")
    if response_received != (outcome in RESPONSE_OUTCOMES):
        raise Route4ContractError("response_received contradicts observable outcome")
    response_fields = (
        "http_status",
        "final_url",
        "response_headers",
        "body_byte_count",
        "response_body_sha256",
        "retained_blob_reference",
    )
    if response_received:
        status = _require_int(record, "http_status", minimum=100)
        if status > 599:
            raise Route4ContractError("HTTP status outside valid range")
        _require_str(record, "final_url")
        headers = record.get("response_headers")
        if not isinstance(headers, dict):
            raise Route4ContractError("response headers must be an object")
        count = _require_int(record, "body_byte_count")
        body_hash = _require_sha(record, "response_body_sha256")
        _require_str(record, "retained_blob_reference")
        if count == 0 and body_hash != hashlib.sha256(b"").hexdigest():
            raise Route4ContractError("empty response body hash mismatch")
    elif any(record.get(field) is not None for field in response_fields):
        raise Route4ContractError("no-response attempt must use explicit null response fields")
    retryable = _require_bool(record, "retryable_under_frozen_policy")
    expected_retryable = (
        outcome == "HTTP_ERROR_RESPONSE"
        and cast(int, record.get("http_status")) in RETRYABLE_HTTP_STATUSES
    ) or outcome in RETRYABLE_NO_RESPONSE_OUTCOMES
    if retryable != expected_retryable:
        raise Route4ContractError("retryability contradicts the frozen policy")
    action = _require_str(record, "next_action")
    if action not in NEXT_ACTIONS:
        raise Route4ContractError("unknown next action")
    if action == "RETRY" and not retryable:
        raise Route4ContractError("nonretryable attempt cannot request retry")
    expected_action = (
        "ACCEPT_RESPONSE"
        if outcome == "SUCCESS_HTTP"
        else (
            "RETRY"
            if retryable and ordinal < 3
            else ("TERMINATE_RETRIES_EXHAUSTED" if retryable else "TERMINATE_NONRETRYABLE")
        )
    )
    if action != expected_action:
        raise Route4ContractError("next action contradicts frozen retry policy")
    if action == "ACCEPT_RESPONSE" and outcome != "SUCCESS_HTTP":
        raise Route4ContractError("only successful HTTP response may be accepted")
    if outcome == "SUCCESS_HTTP" and action != "ACCEPT_RESPONSE":
        raise Route4ContractError("successful HTTP response must be accepted")
    _require_string_list(record, "evidence_references")


def validate_terminal(record: Mapping[str, object]) -> None:
    _require_exact_keys(record, TERMINAL_REQUIRED_FIELDS)
    _validate_base(record, "TERMINAL")
    run_id = _require_sha(record, "acquisition_run_id")
    request_id_value = _require_sha(record, "logical_request_id")
    if _require_sha(record, "terminal_record_id") != terminal_record_id(run_id, request_id_value):
        raise Route4ContractError("terminal record ID mismatch")
    ids = _require_string_list(record, "ordered_attempt_ids")
    if not 1 <= len(ids) <= 3 or any(SHA256_PATTERN.fullmatch(item) is None for item in ids):
        raise Route4ContractError("terminal attempt IDs are invalid")
    if len(ids) != len(set(ids)):
        raise Route4ContractError("duplicate attempt ID in terminal record")
    if _require_sha(record, "ordered_attempt_ids_sha256") != ordered_ids_sha256(ids):
        raise Route4ContractError("ordered attempt-set hash mismatch")
    if _require_sha(record, "terminal_attempt_id") != ids[-1]:
        raise Route4ContractError("terminal attempt must be last ordered attempt")
    terminal_state = _require_str(record, "terminal_state")
    if terminal_state not in TERMINAL_STATES:
        raise Route4ContractError("unknown terminal state")
    _require_str(record, "terminal_reason_code")
    _require_str(record, "terminal_at_utc")
    if record.get("frozen_decision_rule") != DECISION_RULE:
        raise Route4ContractError("wrong frozen terminal decision rule")
    _require_bool(record, "any_response_received")
    bytes_obtained = _require_bool(record, "bytes_obtained")
    checksum = _require_bool(record, "checksum_verification_occurred")
    parsed = _require_bool(record, "parsing_occurred")
    if (checksum or parsed) and not bytes_obtained:
        raise Route4ContractError("checksum or parse cannot occur without bytes")
    disposition = _require_str(record, "disposition")
    if disposition not in DISPOSITIONS:
        raise Route4ContractError("unknown terminal disposition")
    if (terminal_state == "SUCCEEDED") != (disposition == "ACCEPTED"):
        raise Route4ContractError("terminal success and accepted disposition disagree")
    _require_string_list(record, "retained_evidence_references")
    hashes = _require_string_list(record, "evidence_hashes")
    if any(SHA256_PATTERN.fullmatch(item) is None for item in hashes):
        raise Route4ContractError("terminal evidence hash is invalid")
    _require_sha(record, "ledger_prefix_sha256")


def validate_ledger(records: Sequence[Mapping[str, object]]) -> dict[str, str]:
    """Validate an ordered immutable ledger and reconstruct terminal states from it only."""
    attempts_by_request: dict[str, list[Mapping[str, object]]] = {}
    terminals: dict[str, Mapping[str, object]] = {}
    seen_record_ids: set[str] = set()
    prefix_records: list[Mapping[str, object]] = []
    for record in records:
        record_type = record.get("record_type")
        if record_type == "ATTEMPT":
            validate_attempt(record)
            record_id = _require_sha(record, "attempt_id")
            request_id_value = _require_sha(record, "logical_request_id")
            attempts = attempts_by_request.setdefault(request_id_value, [])
            ordinal = _require_int(record, "attempt_ordinal", minimum=1)
            if ordinal != len(attempts) + 1:
                raise Route4ContractError("attempts must be ordered and contiguous")
            if attempts:
                if record.get("prior_attempt_id") != attempts[-1].get("attempt_id"):
                    raise Route4ContractError("prior attempt lineage mismatch")
                if attempts[-1].get("next_action") != "RETRY":
                    raise Route4ContractError("attempt follows a terminal prior action")
            attempts.append(record)
            prefix_records.append(record)
        elif record_type == "TERMINAL":
            validate_terminal(record)
            record_id = _require_sha(record, "terminal_record_id")
            request_id_value = _require_sha(record, "logical_request_id")
            if request_id_value in terminals:
                raise Route4ContractError("multiple terminal outcomes for logical request")
            attempts = attempts_by_request.get(request_id_value, [])
            if not attempts:
                raise Route4ContractError("terminal outcome lacks attempt evidence")
            actual_ids = [_require_sha(attempt, "attempt_id") for attempt in attempts]
            if record.get("ordered_attempt_ids") != actual_ids:
                raise Route4ContractError("terminal attempt set does not match ledger evidence")
            if record.get("ledger_prefix_sha256") != ledger_prefix_sha256(prefix_records):
                raise Route4ContractError("terminal ledger prefix hash mismatch")
            if record.get("any_response_received") != any(
                attempt.get("response_received") is True for attempt in attempts
            ):
                raise Route4ContractError("terminal response summary mismatch")
            if record.get("bytes_obtained") != any(
                isinstance(attempt.get("body_byte_count"), int)
                and cast(int, attempt.get("body_byte_count")) > 0
                for attempt in attempts
            ):
                raise Route4ContractError("terminal byte summary mismatch")
            final_action = attempts[-1].get("next_action")
            expected_state = {
                "ACCEPT_RESPONSE": "SUCCEEDED",
                "TERMINATE_NONRETRYABLE": "NONRETRYABLE_FAILED",
                "TERMINATE_RETRIES_EXHAUSTED": "RETRIES_EXHAUSTED",
            }.get(cast(str, final_action))
            if expected_state is None or record.get("terminal_state") != expected_state:
                raise Route4ContractError("terminal state contradicts final attempt")
            terminals[request_id_value] = record
            prefix_records.append(record)
        else:
            raise Route4ContractError("unknown ledger record type")
        if record_id in seen_record_ids:
            raise Route4ContractError("duplicate immutable record ID")
        seen_record_ids.add(record_id)
    unterminated = set(attempts_by_request) - set(terminals)
    if unterminated:
        raise Route4ContractError(f"missing terminal records for {sorted(unterminated)}")
    return {
        request_id_value: _require_str(terminal, "terminal_state")
        for request_id_value, terminal in terminals.items()
    }


def assert_acyclic(graph: Mapping[str, Sequence[str]]) -> None:
    temporary: set[str] = set()
    permanent: set[str] = set()

    def visit(node: str) -> None:
        if node in permanent:
            return
        if node in temporary:
            raise Route4ContractError(f"cyclic content-hash dependency at {node}")
        temporary.add(node)
        for dependency in graph.get(node, ()):  # dependencies need not have outgoing edges
            visit(dependency)
        temporary.remove(node)
        permanent.add(node)

    for node in graph:
        visit(node)


def apply_json_pointer_operations(
    base: Mapping[str, object], operations: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    """Resolve the versioned draft using only add, remove, and replace operations."""
    result = deepcopy(dict(base))
    for operation in operations:
        kind = operation.get("op")
        path = operation.get("path")
        if kind not in {"add", "remove", "replace"} or not isinstance(path, str):
            raise Route4ContractError("invalid draft amendment operation")
        parts = [part.replace("~1", "/").replace("~0", "~") for part in path.split("/")[1:]]
        if not parts:
            raise Route4ContractError("root replacement is prohibited")
        parent: Any = result
        for part in parts[:-1]:
            if isinstance(parent, list):
                parent = parent[int(part)]
            elif isinstance(parent, dict) and part in parent:
                parent = parent[part]
            else:
                raise Route4ContractError(f"unresolvable JSON pointer {path}")
        leaf = parts[-1]
        if isinstance(parent, list):
            index = int(leaf)
            if kind == "add":
                parent.insert(index, deepcopy(operation.get("value")))
            elif kind == "replace":
                parent[index] = deepcopy(operation.get("value"))
            else:
                parent.pop(index)
        elif isinstance(parent, dict):
            if kind == "remove":
                if leaf not in parent:
                    raise Route4ContractError(f"remove target missing {path}")
                del parent[leaf]
            else:
                if kind == "replace" and leaf not in parent:
                    raise Route4ContractError(f"replace target missing {path}")
                parent[leaf] = deepcopy(operation.get("value"))
        else:
            raise Route4ContractError(f"JSON pointer parent is not a container {path}")
    return result


def _transport(
    *,
    dns: str = "UNKNOWN",
    connection: str = "UNKNOWN",
    tls: str = "UNKNOWN",
    transmission: str = "UNKNOWN",
    wait: str = "UNKNOWN",
) -> dict[str, object]:
    return {
        "dns_resolution": dns,
        "connection_establishment": connection,
        "tls_handshake": tls,
        "request_transmission": transmission,
        "response_wait": wait,
    }


def build_attempt(
    *,
    run_id: str,
    request_kind: str,
    request_url: str,
    object_key: str | None,
    ordinal: int,
    prior_attempt: str | None,
    outcome: str,
    retryable: bool,
    next_action: str,
    transport: Mapping[str, object],
    http_status: int | None = None,
    body: bytes | None = None,
    exception_category: str | None = None,
) -> dict[str, object]:
    identity = request_identity(
        request_kind=request_kind, request_url=request_url, object_key=object_key
    )
    request_id_value = logical_request_id(run_id, identity)
    response_received = outcome in RESPONSE_OUTCOMES
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "record_type": "ATTEMPT",
        "attempt_id": attempt_id(run_id, request_id_value, ordinal),
        "acquisition_run_id": run_id,
        "logical_request_id": request_id_value,
        "pagination_parent_request_id": None,
        "deterministic_request_identity": identity,
        "request_kind": request_kind,
        "request_url": request_url,
        "object_key": object_key,
        "method": "GET",
        "attempt_ordinal": ordinal,
        "prior_attempt_id": prior_attempt,
        "started_at_utc": f"2026-08-05T00:00:{ordinal * 2:02d}Z",
        "ended_at_utc": f"2026-08-05T00:00:{ordinal * 2 + 1:02d}Z",
        "configured_timeout_seconds": 30,
        "response_received": response_received,
        "http_status": http_status if response_received else None,
        "final_url": request_url if response_received else None,
        "response_headers": {} if response_received else None,
        "body_byte_count": len(body or b"") if response_received else None,
        "response_body_sha256": hashlib.sha256(body or b"").hexdigest()
        if response_received
        else None,
        "retained_blob_reference": f"sha256/{hashlib.sha256(body or b'').hexdigest()}"
        if response_received
        else None,
        "observable_outcome": outcome,
        "observable_exception_category": exception_category,
        "transport_observations": dict(transport),
        "retryable_under_frozen_policy": retryable,
        "next_action": next_action,
        "evidence_references": [],
        "canonical_record_sha256": "",
    }
    return with_record_sha256(payload)


def build_terminal(
    *,
    preceding_records: Sequence[Mapping[str, object]],
    attempts: Sequence[Mapping[str, object]],
    terminal_state: str,
    reason: str,
    disposition: str,
) -> dict[str, object]:
    if not attempts:
        raise Route4ContractError("cannot build terminal without attempt")
    run_id = _require_sha(attempts[0], "acquisition_run_id")
    request_id_value = _require_sha(attempts[0], "logical_request_id")
    ids = [_require_sha(attempt, "attempt_id") for attempt in attempts]
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": EXPERIMENT_ID,
        "record_type": "TERMINAL",
        "terminal_record_id": terminal_record_id(run_id, request_id_value),
        "acquisition_run_id": run_id,
        "logical_request_id": request_id_value,
        "ordered_attempt_ids": ids,
        "ordered_attempt_ids_sha256": ordered_ids_sha256(ids),
        "terminal_attempt_id": ids[-1],
        "terminal_state": terminal_state,
        "terminal_reason_code": reason,
        "terminal_at_utc": "2026-08-05T00:01:00Z",
        "frozen_decision_rule": DECISION_RULE,
        "any_response_received": any(
            attempt.get("response_received") is True for attempt in attempts
        ),
        "bytes_obtained": any(
            isinstance(attempt.get("body_byte_count"), int)
            and cast(int, attempt.get("body_byte_count")) > 0
            for attempt in attempts
        ),
        "checksum_verification_occurred": False,
        "parsing_occurred": False,
        "disposition": disposition,
        "retained_evidence_references": [],
        "evidence_hashes": [],
        "ledger_prefix_sha256": ledger_prefix_sha256([*preceding_records, *attempts]),
        "canonical_record_sha256": "",
    }
    return with_record_sha256(payload)


def build_fixture_ledgers() -> dict[str, list[dict[str, object]]]:
    """Build all frozen response/no-response scenarios without network access."""
    run_id = canonical_sha256([EXPERIMENT_ID, "deterministic-fixture-run-v1"])
    root_url = (
        "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
        "?prefix=data%2Fspot%2Fmonthly%2Fklines%2F"
        "&max-keys=1000&delimiter=%2F"
    )
    scenarios: dict[str, list[dict[str, object]]] = {}

    def one(
        name: str,
        *,
        outcome: str,
        retryable: bool,
        action: str,
        state: str,
        disposition: str,
        transport: Mapping[str, object],
        status: int | None = None,
        body: bytes | None = None,
        exception: str | None = None,
    ) -> None:
        url = f"{root_url}&fixture={name}"
        attempt = build_attempt(
            run_id=run_id,
            request_kind="root_listing",
            request_url=url,
            object_key=None,
            ordinal=1,
            prior_attempt=None,
            outcome=outcome,
            retryable=retryable,
            next_action=action,
            transport=transport,
            http_status=status,
            body=body,
            exception_category=exception,
        )
        terminal = build_terminal(
            preceding_records=[],
            attempts=[attempt],
            terminal_state=state,
            reason=name.upper(),
            disposition=disposition,
        )
        scenarios[name] = [attempt, terminal]

    def retry_then_success(
        name: str,
        *,
        outcome: str,
        transport: Mapping[str, object],
        exception: str,
    ) -> None:
        url = f"{root_url}&fixture={name}"
        first = build_attempt(
            run_id=run_id,
            request_kind="root_listing",
            request_url=url,
            object_key=None,
            ordinal=1,
            prior_attempt=None,
            outcome=outcome,
            retryable=True,
            next_action="RETRY",
            transport=transport,
            exception_category=exception,
        )
        second = build_attempt(
            run_id=run_id,
            request_kind="root_listing",
            request_url=url,
            object_key=None,
            ordinal=2,
            prior_attempt=_require_sha(first, "attempt_id"),
            outcome="SUCCESS_HTTP",
            retryable=False,
            next_action="ACCEPT_RESPONSE",
            transport=_transport(wait="SUCCEEDED"),
            http_status=200,
            body=b"<ListBucketResult/>",
        )
        terminal = build_terminal(
            preceding_records=[],
            attempts=[first, second],
            terminal_state="SUCCEEDED",
            reason=f"{name.upper()}_THEN_SUCCESS",
            disposition="ACCEPTED",
        )
        scenarios[name] = [first, second, terminal]

    response_transport = _transport(wait="SUCCEEDED")
    one(
        "successful_http_response",
        outcome="SUCCESS_HTTP",
        retryable=False,
        action="ACCEPT_RESPONSE",
        state="SUCCEEDED",
        disposition="ACCEPTED",
        transport=response_transport,
        status=200,
        body=b"<ListBucketResult/>",
    )
    one(
        "http_error_response_nonretryable",
        outcome="HTTP_ERROR_RESPONSE",
        retryable=False,
        action="TERMINATE_NONRETRYABLE",
        state="NONRETRYABLE_FAILED",
        disposition="FAILED",
        transport=response_transport,
        status=404,
        body=b"not found",
    )
    retryable_no_response_cases = {
        "timeout_without_response": ("TIMEOUT_NO_RESPONSE", _transport(wait="FAILED")),
        "dns_failure": ("DNS_FAILURE", _transport(dns="FAILED")),
        "tls_failure": (
            "TLS_FAILURE",
            _transport(dns="SUCCEEDED", connection="SUCCEEDED", tls="FAILED"),
        ),
        "connection_reset_before_response": (
            "CONNECTION_RESET_BEFORE_RESPONSE",
            _transport(connection="SUCCEEDED", wait="FAILED"),
        ),
    }
    for name, (outcome, transport) in retryable_no_response_cases.items():
        retry_then_success(
            name,
            outcome=outcome,
            transport=transport,
            exception=name,
        )

    nonretryable_no_response_cases = {
        "connection_establishment_failure": (
            "CONNECTION_ESTABLISHMENT_FAILURE",
            _transport(dns="SUCCEEDED", connection="FAILED"),
        ),
        "unknown_transport_failure": ("UNKNOWN_TRANSPORT_FAILURE", _transport()),
        "partial_transport_knowledge": (
            "UNKNOWN_TRANSPORT_FAILURE",
            _transport(dns="SUCCEEDED", connection="SUCCEEDED"),
        ),
    }
    for name, (outcome, transport) in nonretryable_no_response_cases.items():
        one(
            name,
            outcome=outcome,
            retryable=False,
            action="TERMINATE_NONRETRYABLE",
            state="NONRETRYABLE_FAILED",
            disposition="UNAVAILABLE",
            transport=transport,
            exception=name,
        )

    retry_url = f"{root_url}&fixture=retryable_http_then_success"
    retry_first = build_attempt(
        run_id=run_id,
        request_kind="root_listing",
        request_url=retry_url,
        object_key=None,
        ordinal=1,
        prior_attempt=None,
        outcome="HTTP_ERROR_RESPONSE",
        retryable=True,
        next_action="RETRY",
        transport=response_transport,
        http_status=503,
        body=b"temporary",
    )
    retry_second = build_attempt(
        run_id=run_id,
        request_kind="root_listing",
        request_url=retry_url,
        object_key=None,
        ordinal=2,
        prior_attempt=_require_sha(retry_first, "attempt_id"),
        outcome="SUCCESS_HTTP",
        retryable=False,
        next_action="ACCEPT_RESPONSE",
        transport=response_transport,
        http_status=200,
        body=b"<ListBucketResult/>",
    )
    retry_terminal = build_terminal(
        preceding_records=[],
        attempts=[retry_first, retry_second],
        terminal_state="SUCCEEDED",
        reason="HTTP_RETRY_THEN_SUCCESS",
        disposition="ACCEPTED",
    )
    scenarios["retryable_http_response_then_success"] = [
        retry_first,
        retry_second,
        retry_terminal,
    ]

    exhausted_url = f"{root_url}&fixture=retry_exhaustion"
    exhausted_attempts: list[dict[str, object]] = []
    for ordinal, outcome in enumerate(
        ("DNS_FAILURE", "TIMEOUT_NO_RESPONSE", "CONNECTION_RESET_BEFORE_RESPONSE"), 1
    ):
        prior = _require_sha(exhausted_attempts[-1], "attempt_id") if exhausted_attempts else None
        exhausted_attempts.append(
            build_attempt(
                run_id=run_id,
                request_kind="root_listing",
                request_url=exhausted_url,
                object_key=None,
                ordinal=ordinal,
                prior_attempt=prior,
                outcome=outcome,
                retryable=True,
                next_action="RETRY" if ordinal < 3 else "TERMINATE_RETRIES_EXHAUSTED",
                transport=_transport(),
                exception_category=outcome.lower(),
            )
        )
    exhausted_terminal = build_terminal(
        preceding_records=[],
        attempts=exhausted_attempts,
        terminal_state="RETRIES_EXHAUSTED",
        reason="RETRY_LIMIT_REACHED",
        disposition="UNAVAILABLE",
    )
    scenarios["retry_exhaustion"] = [*exhausted_attempts, exhausted_terminal]
    return scenarios


def validate_fixture_scenarios() -> list[ScenarioResult]:
    results: list[ScenarioResult] = []
    for name, records in build_fixture_ledgers().items():
        reconstructed = validate_ledger(records)
        terminal = records[-1]
        request_id_value = _require_sha(terminal, "logical_request_id")
        results.append(
            ScenarioResult(
                name=name,
                attempt_count=len(records) - 1,
                terminal_state=reconstructed[request_id_value],
            )
        )
    return results


def find_hash_strings(value: object) -> set[str]:
    found: set[str] = set()
    if isinstance(value, str) and SHA256_PATTERN.fullmatch(value):
        found.add(value)
    elif isinstance(value, list):
        for item in value:
            found.update(find_hash_strings(item))
    elif isinstance(value, dict):
        for item in value.values():
            found.update(find_hash_strings(item))
    return found


def ensure_paths_are_within(root: Path, paths: Iterable[Path]) -> None:
    resolved_root = root.resolve()
    for path in paths:
        if not path.resolve().is_relative_to(resolved_root):
            raise Route4ContractError(f"path escapes repository: {path}")
