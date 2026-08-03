# Durable Progress Log

- Current checkpoint: `btc-eth-volatility-managed-equal-weight-v1` has a frozen no-data contract and repaired pure-core implementation validated before data access. The prior volatility-parity result remains exactly `HISTORICAL_NO_GO` and terminal `AUDIT_REJECTED`.
- What changed: live Terra/medium call 2/4 produced the pure core; parent validation rejected its coarse 11-test inventory, consumed the sole repair, and completed the exact 30/30 frozen synthetic obligations without changing the strategy.
- What was verified: 30 focused and 206 repository tests; repository Ruff; strict typing across 23 source files and the changed test pair; exact input/target/fill hashes; causal gap and safety clocks; DSR/PBO/bootstrap formulas; fixed latch; closed holdout; no candidate; zero capital.
- What failed: attempt one found 0/30 exact frozen names and five implementation defects. Those coding defects are preserved in `IMPLEMENTATION_VALIDATION_ATTEMPT_1_FAILURE.json`; no model fallback was allowed.
- Current best defensible result: no candidate. The latest completed development diagnostics are negative and preserved as `HISTORICAL_NO_GO`, but terminal audit rejection prevents treating them as a valid frozen-strategy economic verdict.
- Next experiment: commit and push the repaired pure-core checkpoint, then build and validate the production evaluator and opened-byte identity gate without reading market values.
- Current blocker: none. Every 2026 footer/value remains unopened and unread; scheduled continuation is disabled under active interactive ownership; GPU and mining are unchanged.
- Exact resume state: active experiment `btc-eth-volatility-managed-equal-weight-v1`; calls 2/4 used, two remain; repair 1/1 used, zero remain; cycle 0/1 used; frozen contract `42f99d67...`, effective contract `d4765946...`, implementation validation `915eae08...`; market values and 2026 holdout remain unopened.
