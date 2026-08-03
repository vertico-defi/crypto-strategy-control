# Durable Progress Log

- Current checkpoint: `btc-eth-relative-value-rotation-v1` has deterministic development `HISTORICAL_NO_GO` at result hash `e03d5fc44598a4c3e4b0f34b87b3f0a37427af44248b45ee78dd4154e45a3d4e`, pending independent audit.
- What changed: live Terra/medium implementation was parent-corrected and committed as `717f76a`; the single deterministic-local evaluation then loaded only 36 allowlisted 2024–2025 partitions and produced the frozen result.
- What was verified: preregistration hash `ad640b...`, data contract hash `d2a02b...`, canonical result hash, 105 tests, Ruff, strict typing across 16 source files, diff checks, closed/unread 2026 holdout, zero candidate, and zero capital.
- What failed: eight gates—maximum drawdown, exact baseline superiority, total entries, per-asset holds, bootstrap lower bound, DSR, PBO, and regime stability.
- Current best defensible result: no validated strategy candidate. Relative rotation has attractive aggregate development diagnostics but fails immutable robustness and sufficiency gates and cannot justify holdout access.
- Next experiment: run one read-only native live Sol/xhigh independent methodological audit of the exact committed development evidence; retain the fourth call only for a same-model retry after infrastructure failure.
- Current blocker: none. Scheduled continuation remains disabled; GPU and mining remain untouched.
- Exact resume state: source `717f76a`, result `e03d5fc...`, budget used 2/4, repairs 0/1, cycle consumed, GPU 0, holdout footers/values unread. Commit and push the validation checkpoint, then audit.
