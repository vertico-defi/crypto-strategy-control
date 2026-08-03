# Runbook

```bash
.venv/bin/python orchestrator.py status
.venv/bin/python orchestrator.py dry-run
.venv/bin/python orchestrator.py smoke --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py cycle --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py archive-audit --owner-type interactive_goal
.venv/bin/python orchestrator.py archive-independent-audit --owner-type interactive_goal
.venv/bin/python orchestrator.py run --cycles 1 --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py resume --cycles 1 --invocation-mode live --owner-type interactive_goal
.venv/bin/python orchestrator.py publication-dry-run
.venv/bin/python orchestrator.py publish
.venv/bin/python orchestrator.py prospective
```

Each call holds a kernel advisory lock with explicit active/released owner metadata and at most three cycles. The bounded runner stops as soon as a step makes no state transition; an unregistered saved `next_task` returns `NO_AUTOMATED_STEP_REGISTERED` without a model call or mutation. It never creates an infinite shell loop, calls an exchange, invokes Git, or permits capital. `mock` is restricted to unit tests. `deterministic_local` is never a model result. Only `live` plus a received model response may support a model-generated research claim.

The versioned continuation service is fail-closed while `CURRENT_STATE.json` has `scheduled_enabled=false`, any non-null `active_owner_type`, or a scheduled bound other than one cycle. Do not install or enable the timer during an active Goal. A deliberate handoff must clear ownership, set `scheduled_enabled=true` and `bounded_cycles_per_run=1`, register an explicit safe dispatcher for the saved `next_task`, pass all tests, and then install the unit files. The one-shot service has a 900-second hard timeout and creates no commit merely because the timer ran.
