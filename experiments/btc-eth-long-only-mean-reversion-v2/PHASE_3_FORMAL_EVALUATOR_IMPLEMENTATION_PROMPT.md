# Mean-reversion v2 formal evaluator assembly

Work only in `/home/vertico/crypto-strategy-control` at a clean committed HEAD containing `PHASE_3_FORMAL_EVALUATOR_IMPLEMENTATION_AUTHORIZATION.json`. Its `source_parent_commit` names the clean pre-authorization content checkpoint and must be an ancestor of invocation HEAD; it is not a self-referential claim that the authorization file already existed at its own parent.

Use `gpt-5.6-terra` at `medium` reasoning in live workspace-write mode. Do not run Git. Do not open `/home/vertico/crypto-direction-lab`, any market file, any 2026 path, or any holdout artifact. Do not calculate real returns or performance. This invocation is implementation and synthetic validation only.

Read the authorization, frozen `PREREGISTRATION.json` and its exact effective `PREREGISTRATION_DRAFT.json`, the passed Phase 3 production and fidelity artifacts, `mean_reversion_v2.py`, `mean_reversion_v2_pipeline.py`, the formal production-validation driver, and all v2 tests. The Phase 1 evaluator may be inspected only as contaminated historical implementation evidence; do not copy its audited defects.

Implement the shortest deterministic contract-faithful path to the single formal development evaluation. Prefer adding:

- `src/strategy_control/mean_reversion_v2_evaluator.py`
- `tests/test_mean_reversion_v2_evaluator.py`
- `experiments/btc-eth-long-only-mean-reversion-v2/formal_development_evaluation.py`

Do not change frozen strategy parameters, trials/order, clocks, gaps, recovery, fold boundaries, costs, sizing, accounting, bootstrap seed, DSR/PBO, gates, or holdout rules. Reuse audited objects and functions. Any actual run must remain impossible unless the caller supplies only the exact verified 36-entry development dataset and exact pre-result bindings.

The evaluator must independently simulate every aggregate/fold/stress/standalone/comparator path from cash; enforce exact predeclared terminal liquidation; preserve half-open fold and gap segmentation; produce all canonical trace families; reconcile targets, exact fills, accounting, and terminal cash; compute all required panels, metrics, and 19 gates; and fail closed on missing, degenerate, or inconsistent evidence. Formal output must explicitly state whether all seven trials completed, whether an economic result exists, whether the holdout was accessed, and whether the result is `HISTORICAL_NO_GO` or `DEVELOPMENT_GO_PENDING_INDEPENDENT_AUDIT`.

Add focused synthetic tests for state/fill ordering, fold initialization, gaps/quarantine, delayed execution, terminal liquidation, panel alignment, comparators, standalone weights, entry counts, regimes, concentration, multiplicity inputs, DSR/PBO degeneracy, all-gate mapping, pre-result bindings, JSON serialization, and fail-closed errors. Keep tests synthetic and non-economic.

Run focused tests, the full repository suite in the existing preinstalled research environment, Ruff, strict Mypy with Python 3.13, and compilation. Do not install anything. Return a concise structured report of files changed, tests, remaining defects, and confirmation of zero data/holdout/returns/Git/GPU/capital access.
