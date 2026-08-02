# Runbook

```bash
.venv/bin/python orchestrator.py status
.venv/bin/python orchestrator.py dry-run
.venv/bin/python orchestrator.py mock-validate
.venv/bin/python orchestrator.py cycle --mock-agents
.venv/bin/python orchestrator.py run --cycles 1 --mock-agents
.venv/bin/python orchestrator.py resume --mock-agents
.venv/bin/python orchestrator.py publication-dry-run
.venv/bin/python orchestrator.py publish
.venv/bin/python orchestrator.py prospective
```

Each call uses one exclusive lock and at most three cycles; it never creates an infinite shell loop, calls an exchange, or permits capital.
