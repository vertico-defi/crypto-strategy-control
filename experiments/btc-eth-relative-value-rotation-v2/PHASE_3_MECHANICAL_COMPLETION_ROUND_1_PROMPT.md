# Relative-value v2 Phase 3 mechanical completion round 1

Work only in `/home/vertico/crypto-strategy-control` at the clean committed HEAD containing `PHASE_3_MECHANICAL_COMPLETION_ROUND_1_AUTHORIZATION.json`. The authorization's `source_parent_commit` is the clean evidence parent and must be an ancestor of invocation HEAD.

Use live `gpt-5.6-terra` at `medium` reasoning with workspace-write permission. Do not run Git. Do not open `/home/vertico/crypto-direction-lab`, any market file, any 2026 path, or any holdout artifact. Do not run the real-data preflight. Do not calculate real returns or performance.

Read the Phase 3 completion and round-1 authorizations, frozen `PREREGISTRATION.json` and effective `PREREGISTRATION_DRAFT.json`, the Phase 2 terminal validation and audit, the Phase 3 production-preflight evidence, all three current relative-value modules, and all relative-value tests. Preserve Phase 1/2 evidence exactly.

The measured real-data geometry is binding: 549 grid sessions, 547 complete signal sessions, and 356 causally eligible exact execution vectors. The current `simulate_period` silently pairs vector position `i` with observation position `i`; all 356 comparable real positions mismatch. `BoundaryIndex.earliest_after` also scans all timestamps on every lookup.

Implement the shortest contract-faithful mechanical correction. Prefer an immutable explicit session/execution binding object plus a production-facing no-I/O adapter. It must retain the signal-session identity, full per-asset observation identities, true cutoff, segment and recovery state, exact base fill and exact delayed fill identities, and terminal identity. The simulator production entry point must consume those bindings, not unrelated positional arrays. Keep any legacy synthetic convenience API clearly non-production or replace it without breaking useful tests.

Enforce strict half-open aggregate/fold prefixes, direct indexed lookup, synchronized two-asset vectors, no forward scan after a missing exact required vector, cash/exposed quarantine precedence, no gap bridge, exactly 150 newly complete recovery sessions, separate base/delayed state, and exact terminal cash. At delayed C_s, apply the immutable due decision first, then compute the new decision from resulting state; do not introduce the v1 off-by-one defect.

Add behaviorally substantive tests for nonpositional mapping, duplicates, malformed or future suffix isolation, asynchronous availability, one-asset missing rows, missing exact fills, no-forward-scan behavior, gap boundaries, 149/150 recovery, all four fold boundaries, delayed execution, terminal replacement and identity, index reuse, and indexed-versus-separate-scan equivalence. Add a scaling/instrumentation test showing lookup work is not rows multiplied by sessions.

Do not change any frozen asset, trial/order, horizon, score, threshold, cost, fold, gate, seed, multiplicity, accounting, or holdout rule. This round is incomplete if production session/fill mapping or recovery remains implicit, if the production path can still use positional pairing, or if any required exact lookup can substitute a later row.

Run focused tests, the full suite in `/home/vertico/ctrend-lab/.venv`, Ruff, strict Mypy with Python 3.13 over `src`, and compilation. Do not install packages. Return a concise report listing changed files, exact tests, unresolved defects, and confirmation of zero market/holdout/returns/Git/GPU/capital access. Parent validation will independently inspect and then run the real 36-partition production path.
