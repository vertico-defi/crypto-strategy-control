"""Deterministic local artifact renderers."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


def markdown(snapshot: dict[str, Any]) -> str:
    lines = ["# Crypto strategy control report", "", f"Generated: `{snapshot['generated_at']}`", ""]
    for strategy in snapshot["strategies"]:
        lines.extend(
            [
                f"## {strategy['strategy_id']}",
                "",
                f"- Stage: `{strategy['stage']}`",
                f"- Verdict: {strategy['latest_verdict']}",
                f"- Repository: `{strategy['repository']}`",
                f"- Commit: `{strategy['current_commit'] or 'unavailable'}`",
                f"- Service / timer: `{strategy['service_state']}` / `{strategy['timer_state']}`",
                (
                    f"- Exposure / risk budget: {strategy['current_exposure']} "
                    f"/ {strategy['risk_budget']}"
                ),
                f"- Next gate: {strategy['next_gate']}",
            ]
        )
        if strategy["integrity_warnings"]:
            lines.append("- Integrity warnings:")
            lines.extend(f"  - {warning}" for warning in strategy["integrity_warnings"])
        lines.append("")
    return "\n".join(lines)


def dashboard(snapshot: dict[str, Any]) -> str:
    rows = "".join(
        "<tr>"
        + "".join(
            f"<td>{html.escape(str(strategy[key] if strategy[key] is not None else '—'))}</td>"
            for key in ("strategy_id", "stage", "latest_verdict", "current_exposure", "next_gate")
        )
        + "</tr>"
        for strategy in snapshot["strategies"]
    )
    generated = html.escape(snapshot["generated_at"])
    return (
        "<!doctype html>\n"
        '<html lang="en"><head><meta charset="utf-8">'
        "<title>Crypto Strategy Control</title>\n"
        "<style>body{font-family:system-ui;margin:2rem}table{border-collapse:collapse;width:100%}"
        "td,th{border:1px solid #bbb;padding:.5rem;text-align:left}th{background:#eee}</style>\n"
        "</head><body><h1>Crypto Strategy Control</h1>"
        f"<p>Generated {generated}. Read-only research status; no capital is permitted.</p>\n"
        "<table><thead><tr><th>Strategy</th><th>Stage</th><th>Verdict</th>"
        "<th>Exposure</th><th>Next gate</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>\n"
        "<p>Source snapshot: <code>status.json</code></p></body></html>\n"
    )


def write_artifacts(output: Path, snapshot: dict[str, Any]) -> dict[str, Path]:
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": output / "status.json",
        "markdown": output / "control-report.md",
        "html": output / "dashboard.html",
    }
    paths["json"].write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    paths["markdown"].write_text(markdown(snapshot))
    paths["html"].write_text(dashboard(snapshot))
    return paths
