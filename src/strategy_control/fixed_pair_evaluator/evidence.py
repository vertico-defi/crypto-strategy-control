"""Canonical stage and trace evidence primitives."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class StageMarker:
    stage: str
    started_at_utc: str
    finished_at_utc: str
    input_hash: str
    output_hash: str
    status: str

    @classmethod
    def complete(cls, stage: str, payload: Any) -> StageMarker:
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        digest = canonical_hash(payload)
        return cls(stage, now, now, digest, digest, "PASS")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
