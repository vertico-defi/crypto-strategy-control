# Crypto Strategy Control

Read-only registry and dashboard for independent Direction, Perp Carry, and
CTREND research projects. It never starts services, writes into a registered
repository, accesses exchange credentials, or places orders.

```bash
python -m strategy_control status
python -m strategy_control report
python -m strategy_control gates
python -m strategy_control dashboard
python -m strategy_control verify
```

Generated local artifacts are in `reports/`. Refresh them with
`python -m strategy_control refresh`. The user systemd timer runs the same
read-only refresh daily; its unit files are versioned in `systemd/`.

## Persistent zero-capital research program

`orchestrator.py` is a fail-closed coordinator. It never uses exchange
credentials, starts collectors, mines, opens a wallet, sends orders, or grants
capital. It writes atomic state and append-only JSONL ledgers, holds a kernel
advisory lock with explicit ownership, and requires a frozen preregistration to
be committed before evaluation.

Run from the repository checkout (the wrapper also works before installation):

```bash
.venv/bin/python orchestrator.py status
.venv/bin/python orchestrator.py dry-run
.venv/bin/python orchestrator.py smoke --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py freeze
# commit the frozen preregistration, then:
.venv/bin/python orchestrator.py cycle --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py publication-dry-run
.venv/bin/python orchestrator.py snapshot
.venv/bin/ruff check .
.venv/bin/mypy src
.venv/bin/pytest
```

The completed `cs-ranking-ptu-data-audit-v1` remains `DATA_NO_GO`, with no
holdout opened and no returns computed. That scoped result did not exhaust the
program. The active bounded experiment investigates an archive-observed
point-in-time universe from official Binance public historical spot archives.
It cannot claim formal archive completeness and cannot open a strategy holdout
until the universe contract and independent audit pass.

Production model calls use explicit, ephemeral, read-only Codex CLI invocations
and record `invocation_mode`, requested and actual model, reasoning, timestamps,
response identifier, result hash, outcome, and exact errors. Mock mode is for
unit tests only; deterministic-local results are never model-generated research.
