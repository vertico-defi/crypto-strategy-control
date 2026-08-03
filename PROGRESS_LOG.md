# Durable Progress Log

- Current checkpoint: frozen calendar implementation is `PASS_PRE_DATA`; exact validation is `experiments/btc-eth-intraday-calendar-seasonality-v1/IMPLEMENTATION_VALIDATION.json`, call budget 3/4.
- What changed: two live Terra/medium calls were integrated into a complete development-only evaluator; bounded continuation now stops on no state transition and remains disabled during interactive ownership.
- What was verified: 128 tests, Ruff, strict typing across 19 source files, diff checks, frozen wrapper/effective/data/prior-result hashes, and read-only systemd unit verification pass. No market data, Parquet footer/value, return, 2026 holdout, credential, order, GPU, mining, or capital action occurred.
- What failed: the agents initially left numerical statistics and record-to-schedule execution injectable; parent integration implemented those frozen components. The first sandboxed systemd verifier returned `SO_PASSCRED failed: Operation not permitted`; the approved read-only retry passed.
- Current best defensible result: no validated strategy candidate and no eligible archive-derived cross-sectional universe; the calendar family has only a verified pre-data implementation.
- Next experiment: commit the implementation checkpoint, then run the one-shot deterministic 2024–2025 calendar development evaluation. If it is a no-go, preserve it and use the final live Sol/xhigh call for independent terminal audit.
- Current blocker: none. Scheduled continuation remains disabled and the interactive Goal owns execution.
- Exact resume state: source freeze commit `6cdbe55...`, wrapper `1e67b67c...`, effective contract `102a0de9...`, direction result `7fbcfc51...`, implementation status `PASS_PRE_DATA`, budget 3/4 calls with one repair unused, 1,800-second evaluation bound, zero GPU/capital; implementation checkpoint is uncommitted and no data has been read.
