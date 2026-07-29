"""Command-line entry points for read-only control-center reporting."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from strategy_control.adapters import inspect
from strategy_control.model import StrategyConfig
from strategy_control.render import write_artifacts

ROOT = Path(__file__).resolve().parents[2]


def snapshot(registry: Path) -> dict[str, Any]:
    source = json.loads(registry.read_text())
    strategies = [inspect(StrategyConfig.from_mapping(item)) for item in source["strategies"]]
    return {"generated_at": datetime.now(UTC).isoformat(), "strategies": strategies}


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only crypto-strategy control center")
    parser.add_argument(
        "command", choices=["status", "report", "gates", "dashboard", "refresh", "verify"]
    )
    parser.add_argument("--registry", type=Path, default=ROOT / "registry.json")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "reports")
    args = parser.parse_args()
    current = snapshot(args.registry)
    if args.command == "verify":
        assert len(current["strategies"]) == 5
        assert all(item["current_exposure"].startswith("0.0") for item in current["strategies"])
        print("control-center verification: PASS")
        return
    paths = write_artifacts(args.output_dir, current)
    if args.command == "status":
        print(json.dumps(current, indent=2, sort_keys=True))
    elif args.command == "gates":
        for item in current["strategies"]:
            print(f"{item['strategy_id']}: {item['stage']} -> {item['next_gate']}")
    else:
        key = (
            "markdown"
            if args.command == "report"
            else "html"
            if args.command == "dashboard"
            else "json"
        )
        print(paths[key])
