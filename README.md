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
