"""Independent, data-free recovery validation for the recorded G1 artifacts."""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

LAB = Path("/home/vertico/regime-moe-lab")
RECOVERABLE = {
    "g1-decision-interval-selection-protocol",
    "g1-point-in-time-feature-contract",
    "g1-feature-known-answer-fixtures",
    "g1-fold-normalization-and-leakage-tests",
    "g1-event-driven-execution-kernel",
    "g1-nested-walk-forward-kernel",
}


def _artifact(task_id: str) -> tuple[Path, dict[str, Any]]:
    if task_id not in RECOVERABLE:
        raise ValueError(f"not a recoverable G1 task: {task_id}")
    path = LAB / "artifacts" / f"{task_id}.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("artifact root must be an object")
    return path, value


def _safe_scope(value: dict[str, Any]) -> None:
    scope = value.get("scope")
    if not isinstance(scope, dict):
        raise ValueError("missing structured scope")
    text = json.dumps(value, sort_keys=True).lower()
    if "/home/" in text or "api_key" in text or "password" in text:
        raise ValueError("artifact contains private path or credential marker")
    if scope.get("market_data_content_read") not in (False, None):
        raise ValueError("market-data content access is not permitted")
    if scope.get("holdout_access") not in ("FORBIDDEN", None):
        raise ValueError("holdout access is not forbidden")


def _validate_decision(value: dict[str, Any]) -> list[str]:
    if value.get("artifact_type") != "G1_DECISION_INTERVAL_SELECTION_PROTOCOL":
        raise ValueError("wrong decision protocol type")
    if value.get("status") != "PREREGISTERED":
        raise ValueError("decision protocol is not preregistered")
    candidates = value["candidate_intervals"]["allowed_values"]
    selected = value["selection_criteria"]["selected_interval_calendar_days"]
    if (
        not candidates
        or len(candidates) != len(set(candidates))
        or any(not isinstance(item, int) or item <= 0 for item in candidates)
    ):
        raise ValueError("invalid interval candidates")
    ranked = sorted(candidates, key=lambda item: (abs(item - 21), math.ceil(365 / item), item))
    if selected != ranked[0] or selected != 21:
        raise ValueError("interval selection is not reproducible")
    return ["G1-DECISION-001-lexicographic-selection", "G1-DECISION-002-non-return-scope"]


def _validate_point_in_time(value: dict[str, Any]) -> list[str]:
    if value.get("artifact_type") != "G1_POINT_IN_TIME_FEATURE_CONTRACT":
        raise ValueError("wrong point-in-time contract type")
    if value.get("status") != "PREREGISTERED" or not re.fullmatch(
        r"\d+\.\d+\.\d+", str(value.get("contract_version"))
    ):
        raise ValueError("point-in-time contract is not versioned and preregistered")
    required = {
        "feature_id",
        "entity_id",
        "event_at",
        "available_at",
        "as_of_at",
        "source_row_identity",
        "feature_contract_version",
    }
    lineage = value["point_in_time_contract"]["required_lineage_fields"]
    if set(lineage) != required or len(lineage) != len(required):
        raise ValueError("incomplete point-in-time lineage")
    rules = value["deterministic_validation"]["validation_rules"]
    if len(rules) < 12 or value["deterministic_validation"].get("failure_mode") != "FAIL_CLOSED":
        raise ValueError("point-in-time contract lacks fail-closed validation")
    return ["G1-PIT-001-versioned-lineage", "G1-PIT-002-fail-closed-causality"]


def _features(observations: list[dict[str, Any]]) -> dict[str, list[float | None]]:
    closes = [item["close"] for item in observations]
    returns: list[float | None] = [None]
    returns += [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
    lagged = [None if i < 2 else returns[i - 1] for i in range(len(closes))]
    means = [None if i < 2 else (returns[i - 1] + returns[i]) / 2 for i in range(len(closes))]
    stds = [None if i < 2 else abs(returns[i - 1] - returns[i]) / 2 for i in range(len(closes))]
    return {
        "return_1": returns,
        "lagged_return_1": lagged,
        "trailing_mean_return_2": means,
        "trailing_std_return_2_population": stds,
    }


def _validate_feature_fixtures(value: dict[str, Any]) -> list[str]:
    if value.get("artifact_id") != "g1-feature-known-answer-fixtures":
        raise ValueError("wrong known-answer fixture ID")
    fixtures = value.get("fixtures")
    if not isinstance(fixtures, list) or len(fixtures) != 2:
        raise ValueError("missing known-answer fixtures")
    exact, appended = fixtures
    actual = _features(exact["observations"])
    for name, expected in exact["expected"].items():
        if any(
            (a is None) != (b is None) or (a is not None and abs(a - b) > 1e-12)
            for a, b in zip(actual[name], expected, strict=True)
        ):
            raise ValueError("known-answer arithmetic mismatch")
    before = _features(appended["base_observations"])
    after = _features(appended["appended_observations"])
    index = appended["assert_at_index"]
    if any(abs(before[name][index] - after[name][index]) > 1e-12 for name in before):
        raise ValueError("future append changes a causal feature")
    return [
        "KAT-001-exact-feature-values",
        "KAT-002-warmup-null-semantics",
        "KAT-003-future-append-invariance",
        "KAT-004-no-future-index-dependency",
    ]


def _validate_normalization(value: dict[str, Any]) -> list[str]:
    if value.get("artifact_id") != "g1-fold-normalization-and-leakage-tests":
        raise ValueError("wrong normalization fixture ID")
    fixture = value["synthetic_fixture"]
    training = fixture["training_values"]
    mean = sum(training) / len(training)
    std = math.sqrt(sum((item - mean) ** 2 for item in training) / len(training))
    expected = fixture["expected_fit"]
    if abs(mean - expected["mean"]) > 1e-12 or abs(std - expected["population_std"]) > 1e-12:
        raise ValueError("training-only normalization mismatch")
    scores = [(item - mean) / std for item in fixture["validation_values"]]
    if scores != fixture["expected_validation_z_scores"] or std == 0:
        raise ValueError("frozen normalization validation mismatch")
    return [item["id"] for item in value["deterministic_validation"]["tests_run"]]


def _validate_execution(value: dict[str, Any]) -> list[str]:
    if value.get("artifact_id") != "g1-event-driven-execution-kernel":
        raise ValueError("wrong execution kernel ID")
    fixture = value["synthetic_fixture"]
    events = fixture["events"]
    if events != sorted(
        events, key=lambda item: (item["timestamp"], item["sequence"], item["event_id"])
    ):
        raise ValueError("event ordering is not deterministic")
    remaining, fills = 3, []
    for event in events:
        if event["event_type"] == "EXECUTION_OPPORTUNITY" and remaining:
            quantity = min(remaining, event["synthetic_available_quantity"])
            fills.append(
                (
                    quantity,
                    event["synthetic_execution_price"],
                    quantity * event["synthetic_per_unit_cost"],
                )
            )
            remaining -= quantity
    expected = fixture["expected_fills"]
    expected_fills = [(x["filled_quantity"], x["fill_price"], x["fill_cost"]) for x in expected]
    if [(x[0], x[1], x[2]) for x in fills] != expected_fills:
        raise ValueError("execution accounting mismatch")
    if remaining != fixture["expected_final_state"]["order_remaining_quantity"]:
        raise ValueError("execution terminal state mismatch")
    return [item["id"] for item in value["deterministic_validation"]["tests_run"]]


def _validate_walk_forward(value: dict[str, Any]) -> list[str]:
    if value.get("artifact_id") != "g1-nested-walk-forward-kernel":
        raise ValueError("wrong walk-forward kernel ID")
    fixture = value["synthetic_fixture"]
    prior_validation: set[int] = set()
    for fold in fixture["outer_folds"]:
        train, validation = fold["outer_train_indices"], fold["outer_validation_indices"]
        if (
            max(train) >= min(validation)
            or set(train) & set(validation)
            or prior_validation & set(validation)
        ):
            raise ValueError("outer folds are not chronological and disjoint")
        prior_validation.update(validation)
        for inner in fold["inner_folds"]:
            inner_indices = set(inner["train_indices"] + inner["validation_indices"])
            if not inner_indices <= set(train) or max(inner["train_indices"]) >= min(
                inner["validation_indices"]
            ):
                raise ValueError("inner fold escapes outer training")
        losses = fold["synthetic_inner_validation_loss"]
        means = {key: sum(items) / len(items) for key, items in losses.items()}
        best = min(means.values())
        chosen = min(key for key, mean in means.items() if abs(mean - best) <= 1e-12)
        if chosen != fold["expected_selected_candidate"]:
            raise ValueError("inner candidate selection mismatch")
    return [item["id"] for item in value["deterministic_validation"]["tests_run"]]


VALIDATORS = {
    "g1-decision-interval-selection-protocol": _validate_decision,
    "g1-point-in-time-feature-contract": _validate_point_in_time,
    "g1-feature-known-answer-fixtures": _validate_feature_fixtures,
    "g1-fold-normalization-and-leakage-tests": _validate_normalization,
    "g1-event-driven-execution-kernel": _validate_execution,
    "g1-nested-walk-forward-kernel": _validate_walk_forward,
}


def validate_recoverable_artifact(task_id: str) -> dict[str, Any]:
    path, value = _artifact(task_id)
    _safe_scope(value)
    tests = VALIDATORS[task_id](value)
    return {
        "task_id": task_id,
        "classification": "VALID_RECOVERABLE_PASS",
        "artifact": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "tests": tests,
    }
