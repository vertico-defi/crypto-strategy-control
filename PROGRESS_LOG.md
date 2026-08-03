# Durable Progress Log

- Current checkpoint: `btc-eth-volatility-managed-equal-weight-v1` has a frozen contract, pushed pure core `931ca9be...`, and a complete production evaluator awaiting its clean source checkpoint. The prior volatility-parity result remains exactly `HISTORICAL_NO_GO` and terminal `AUDIT_REJECTED`.
- What changed: the production loader now binds the hashed source freeze manifest, exact 36-file allowlist, opened byte count/hash, verified parse buffer, session trace, target hash, and fill parent/vector hashes through the real simulator and controller command.
- What was verified: 30 frozen core tests plus eight production integrations; 214 repository tests; repository Ruff; strict typing across 24 source files; pandas 2.2.3/PyArrow 25.0.0 import-only runtime; closed holdout; no candidate; zero capital.
- What failed: attempt one found 0/30 exact frozen names and five implementation defects. Those coding defects are preserved in `IMPLEMENTATION_VALIDATION_ATTEMPT_1_FAILURE.json`; no model fallback was allowed.
- Current best defensible result: no candidate. The latest completed development diagnostics are negative and preserved as `HISTORICAL_NO_GO`, but terminal audit rejection prevents treating them as a valid frozen-strategy economic verdict.
- Next experiment: commit/push the production evaluator, create a clean commit binding its five exact source/test hashes, then run the sole deterministic development evaluation.
- Current blocker: none. Every 2026 footer/value remains unopened and unread; scheduled continuation is disabled under active interactive ownership; GPU and mining are unchanged.
- Exact resume state: active experiment `btc-eth-volatility-managed-equal-weight-v1`; calls 2/4 used, two remain; repair 1/1 used, zero remain; cycle 0/1 used; production validation is pre-data and pending commit binding; market values and 2026 holdout remain unopened.
