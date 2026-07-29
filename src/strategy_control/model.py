"""Typed control-center records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

REQUIRED_STAGES = {
    "DESIGN",
    "DATA_AUDIT",
    "HISTORICAL_DEVELOPMENT",
    "HISTORICAL_ACCEPTED",
    "HISTORICAL_NO_GO",
    "INFRASTRUCTURE_AUDIT",
    "SHADOW",
    "SHADOW_ACCEPTED",
    "PILOT_ELIGIBLE",
    "LIVE_PILOT",
    "PAUSED",
    "RETIRED",
    "INTEGRITY_FAILURE",
}


@dataclass(frozen=True)
class StrategyConfig:
    """Static, declarative and non-secret registry entry."""

    strategy_id: str
    strategy_class: str
    repository: str
    dataset_id: str | None
    experiment_or_run_id: str | None
    stage: str
    latest_verdict: str
    historical_start: str | None
    historical_end: str | None
    shadow_start: str | None
    shadow_end: str | None
    risk_budget: str
    next_gate: str
    latest_report: str | None
    artifact_files: tuple[str, ...]
    service_unit: str | None
    timer_unit: str | None

    @classmethod
    def from_mapping(cls, item: dict[str, Any]) -> StrategyConfig:
        config = cls(
            strategy_id=str(item["strategy_id"]),
            strategy_class=str(item["strategy_class"]),
            repository=str(item["repository"]),
            dataset_id=item.get("dataset_id"),
            experiment_or_run_id=item.get("experiment_or_run_id"),
            stage=str(item["stage"]),
            latest_verdict=str(item["latest_verdict"]),
            historical_start=item.get("historical_start"),
            historical_end=item.get("historical_end"),
            shadow_start=item.get("shadow_start"),
            shadow_end=item.get("shadow_end"),
            risk_budget=str(item["risk_budget"]),
            next_gate=str(item["next_gate"]),
            latest_report=item.get("latest_report"),
            artifact_files=tuple(str(value) for value in item.get("artifact_files", [])),
            service_unit=item.get("service_unit"),
            timer_unit=item.get("timer_unit"),
        )
        if config.stage not in REQUIRED_STAGES:
            raise ValueError(f"unsupported stage for {config.strategy_id}: {config.stage}")
        return config
