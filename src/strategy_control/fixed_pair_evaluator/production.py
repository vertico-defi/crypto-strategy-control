"""Manifest-bound development adapter for the shared fixed-pair evaluator.

This is the only production entry point.  It rejects holdout-labelled paths and
requires a separately committed fidelity approval before aggregate economics.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from strategy_control.mean_reversion_v2_evaluator import (
    FormalResult,
    PreResultBindings,
    VerifiedDevelopmentInput,
    evaluate_verified_development,
)
from strategy_control.mean_reversion_v2_pipeline import (
    AllowlistEntry,
    materialize_rows,
    parse_verified_parquet,
    read_verified_entry,
    verify_source_identity,
)


class ProductionEvaluationBlocked(RuntimeError):
    """A production evaluation cannot safely proceed."""


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def load_verified_development_bundle(
    source_root: Path, control_root: Path
) -> VerifiedDevelopmentInput:
    contract_path = control_root / "experiments/btc-eth-vol-targeted-trend-v1/DATA_CONTRACT.json"
    freeze_path = source_root / (
        "data/frozen/historical-v2-pathc-20260723T175155Z/"
        "DATASET_FREEZE_MANIFEST_historical-v2-pathc-20260723T175155Z.json"
    )
    contract_raw = contract_path.read_bytes()
    freeze_raw = freeze_path.read_bytes()
    contract = json.loads(contract_raw)
    freeze = json.loads(freeze_raw)
    entries = tuple(
        AllowlistEntry(
            item["bytes"], item["month"], item["relative_path"], item["sha256"], item["symbol"]
        )
        for item in contract["partitions"]
        if item["verification_scope"] == "HASH_AND_SCHEMA_METADATA_ONLY"
    )
    if len(entries) != 36 or any("2026" in entry.relative_path for entry in entries):
        raise ProductionEvaluationBlocked("development allowlist is not exact and holdout-free")
    verified = verify_source_identity(
        contract_bytes=contract_raw,
        source_commit=freeze["repository_commit"],
        freeze_manifest_sha256=sha256(freeze_raw),
        inventory_sha256=contract["canonical_inventory_sha256"],
        entries=entries,
    )
    rows_by_asset: dict[str, list[Any]] = {"BTCUSDT": [], "ETHUSDT": []}
    data_root = source_root / "data/real/historical-v2-pathc-20260723T175155Z"
    for entry in verified:
        if "2026" in entry.relative_path:
            raise ProductionEvaluationBlocked("holdout path encountered")
        verified_buffer = read_verified_entry(data_root, entry)
        table = parse_verified_parquet(verified_buffer)
        rows_by_asset[entry.symbol].extend(materialize_rows(table, verified_buffer))
    return VerifiedDevelopmentInput(
        contract_bytes=contract_raw,
        source_commit=freeze["repository_commit"],
        freeze_manifest_sha256=sha256(freeze_raw),
        inventory_sha256=contract["canonical_inventory_sha256"],
        entries=verified,
        rows_by_asset=rows_by_asset,
    )


def run_formal_mean_reversion(
    bundle: VerifiedDevelopmentInput,
    bindings: PreResultBindings,
    prior_registry: tuple[float, ...],
    *,
    fidelity_approved: bool,
) -> FormalResult:
    if not fidelity_approved:
        raise ProductionEvaluationBlocked("formal economics require independent fidelity approval")
    return evaluate_verified_development(
        verified=bundle, bindings=bindings, prior_registry=prior_registry
    )
