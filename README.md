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
capital. It writes atomic state and append-only JSONL ledgers, refuses a live
lock, quarantines only stale locks, and requires a frozen preregistration to
be committed before evaluation.

Run from the repository checkout (the wrapper also works before installation):

```bash
.venv/bin/python orchestrator.py status
.venv/bin/python orchestrator.py dry-run
.venv/bin/python orchestrator.py mock-validate
.venv/bin/python orchestrator.py freeze
# commit the frozen preregistration, then:
.venv/bin/python orchestrator.py cycle --mock-agents
.venv/bin/ruff check .
.venv/bin/mypy src
.venv/bin/pytest
```

The first bounded experiment is intentionally a data-contract audit for a
lawful, complete point-in-time cross-sectional perpetual universe. Existing
BTC/ETH-focused and prospective-only inputs cannot support that claim, so the
honest result is `DATA_NO_GO`, with no holdout opened and no returns computed.
The model-interface command records `MODEL_INTERFACE_UNAVAILABLE` unless an
approved future interface is configured; mock validation is deterministic and
is not a model result.
