# Runbook

```bash
.venv/bin/python orchestrator.py status
.venv/bin/python orchestrator.py dry-run
.venv/bin/python orchestrator.py smoke --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py cycle --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py run --cycles 1 --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py resume --cycles 1 --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py publication-dry-run
.venv/bin/python orchestrator.py publish
.venv/bin/python orchestrator.py prospective
```

Each call holds a kernel advisory lock with explicit owner metadata and at most three cycles; it never creates an infinite shell loop, calls an exchange, or permits capital. `mock` is restricted to unit tests. `deterministic_local` is never a model result. Only `live` plus a received model response may support a model-generated research claim.

The versioned continuation service is fail-closed while `CURRENT_STATE.json` has `scheduled_enabled=false` or `active_owner_type=interactive_goal`. Do not install or enable the timer during an active Goal. A deliberate handoff must clear interactive ownership, enable scheduling, pass all tests, and then install the unit files.
