"""Narrow formal-development entry point.

There is intentionally no source-root option.  Market files are not a valid
input to this program: the only supported execution surface is an in-memory
``evaluate_development`` call from a separately verified 36-entry adapter.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from strategy_control.mean_reversion_v2_evaluator import FormalEvaluationError


def main() -> None:
    parser = argparse.ArgumentParser(description="Write a formal evaluator readiness record.")
    parser.add_argument("--pre-result-bindings", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    bindings = json.loads(arguments.pre_result_bindings.read_text(encoding="utf-8"))
    required = {
        "frozen_preregistration_sha256",
        "implementation_commit",
        "implementation_hashes",
        "source_commit",
        "allowlist_sha256",
        "session_input_manifest_sha256",
        "target_trace_schema_sha256",
        "fill_trace_schema_sha256",
        "environment_sha256",
        "formal_invocation_id",
    }
    if (
        set(bindings.get("values", {})) != required
        or bindings.get("verified_allowlist_count") != 36
    ):
        raise FormalEvaluationError("exact pre-result bindings are required before any evaluation")
    # Deliberately fail closed: accepting a path or deserialised price data here
    # would bypass the verified-buffer adapter and make a formal run unsafe.
    raise FormalEvaluationError(
        "no verified in-memory development bundle was supplied by the production adapter"
    )


if __name__ == "__main__":
    main()
