# Focused Sol/high Architecture Review

Verdict: **Phase 0 corrected; fail closed for Phase 1 until contracts, strict schemas, and test expansion are complete.** The review was read-only and accessed no data or holdout.

Applied controller corrections: a task enters RUNNING before an adapter executes; only the explicit read-only repository-state smoke adapter may become TERMINAL; all other Phase 0 tasks stop at HUMAN_APPROVAL with an exact blocker. Queue/state/scorecard writes use a replayable transaction journal. WAITING_EXTERNAL is ignored during READY selection. The queue is reset so previous simulated checkpoints are not treated as research completion.

Required before Phase 1: runtime model discovery with typed temporary-availability fallback only; strict artifact schemas and expanded recovery/fallback/dependency/fairness tests; explicit success/failure dependency outcomes; ISO-week fairness; enforced content/website claim provenance; concrete frozen gates/costs/delays; and typed fold-local expert/router/risk interfaces with chronological OOF lineage, purge, and embargo.

Schedule: March 2027 remains feasible only if data qualification and leakage fixtures lead the critical path, simple baselines are locked early, router candidates are time-boxed, Transformer/carry remain interface-only, and Jan--Feb reserve reruns and negative-result evidence.
