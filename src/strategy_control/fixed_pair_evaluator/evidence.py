"""Canonical stage and trace evidence primitives."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

STAGE_ORDER = (
    "identity_verified",
    "representative_rows_materialized",
    "production_trace_emitted",
    "independent_reference_reconciled",
    "development_evaluator_complete",
)


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def validate_stage_sequence(markers: tuple[StageMarker, ...]) -> None:
    names = tuple(marker.stage for marker in markers)
    if names != STAGE_ORDER:
        raise ValueError("stage sequence is incomplete or out of order")
    if any(marker.status != "PASS" for marker in markers):
        raise ValueError("stage sequence contains a failed marker")


def require_independent_sources(
    production_source_hash: str, reference_source_hash: str
) -> None:
    if not production_source_hash or not reference_source_hash:
        raise ValueError("both production and reference source hashes are required")
    if production_source_hash == reference_source_hash:
        raise ValueError("production and reference implementations must be distinct")


@dataclass(frozen=True)
class StageMarker:
    stage: str
    started_at_utc: str
    finished_at_utc: str
    input_hash: str
    output_hash: str
    status: str

    @classmethod
    def complete(cls, stage: str, payload: Any, *, input_payload: Any | None = None) -> StageMarker:
        if stage not in STAGE_ORDER:
            raise ValueError(f"unknown stage: {stage}")
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        input_hash = canonical_hash(payload if input_payload is None else input_payload)
        return cls(stage, now, now, input_hash, canonical_hash(payload), "PASS")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvidenceBundle:
    """Immutable production/reference binding for a bounded validation stage."""

    production_source_hash: str
    reference_source_hash: str
    production_trace_hash: str
    reference_trace_hash: str
    markers: tuple[StageMarker, ...]

    def validate(self) -> None:
        require_independent_sources(self.production_source_hash, self.reference_source_hash)
        if self.production_trace_hash != self.reference_trace_hash:
            raise ValueError("production/reference traces do not reconcile")
        validate_stage_sequence(self.markers)
