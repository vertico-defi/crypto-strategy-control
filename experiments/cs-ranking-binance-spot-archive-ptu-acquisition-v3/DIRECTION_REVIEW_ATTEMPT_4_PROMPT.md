# Route 4 provenance-correct direction-review amendment — attempt 4

You are the fresh independent research-direction and methodology auditor for Phase 2 experiment `cs-ranking-binance-spot-archive-ptu-acquisition-v3`.

This is review attempt 4 under the explicit user authorization type `provenance_correct_review_amendment`. Attempts 1–3 are immutable historical evidence. Work read-only. Do not modify any file. Do not browse or make any network request. Do not access `data/raw`, any market file or bar, any 2026 market-data identity, any holdout path/footer/row/value, or the rejected v1 symbol-manifest contents. Do not train, backtest, calculate a strategy return, infer profitability, consume an implementation repair, or consume an acquisition attempt.

First establish chain of custody:

1. Record the exact clean 40-hex HEAD and verify source parent `6f98b254d00c1dac4b2ef2789c3ca328dc2a65df` is an ancestor.
2. Independently canonicalize the revised draft and the reviewed output schema using UTF-8 JSON, sorted keys, compact separators, `ensure_ascii=false`, and `allow_nan=false`.
3. Require revised-draft canonical SHA-256 `48ff7c37eaec1babf5a463f90900b6f188e99c8dd38056c351509b461c61d86e` and reviewed output-schema canonical SHA-256 `5e8154a7c91aa5de404b05fb3d4d59e0238dc2d4152ee86253c7d709fae387f1` exactly.
4. Verify all prior attempt and incorporation byte hashes bound in `DIRECTION_REVIEW_ATTEMPT_4_PRECHECK.json` without changing or replacing prior evidence.
5. Distinguish `DIRECTION_REVIEW_OUTPUT_SCHEMA.json`, which is an immutable methodology artifact under review, from `DIRECTION_REVIEW_ATTEMPT_4_RESULT_SCHEMA.json`, which only constrains your attempt-4 response.

Open and review only these repository artifacts:

- `AGENTS.md`
- `RESEARCH_PROTOCOL.md`
- `ACCEPTANCE_GATES.yaml`
- `PHASE_2_AUTHORIZATION.json`
- `CURRENT_STATE.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/ROUTE_AUTHORIZATION.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/PREREGISTRATION_DRAFT.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/DIRECTION_REVIEW_OUTPUT_SCHEMA.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/DIRECTION_REVIEW_ATTEMPT_1_TIMEOUT.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/DIRECTION_REVIEW_ATTEMPT_2_TIMEOUT.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/DIRECTION_REVIEW_ATTEMPT_3.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/DIRECTION_REVIEW_ATTEMPT_3_MODEL_RESULT.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/PREREGISTRATION_REVISION_INCORPORATION.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/NO_DATA_REVISION_VALIDATION.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/DIRECTION_REVIEW_ATTEMPT_4_PRECHECK.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/DIRECTION_REVIEW_ATTEMPT_4_AUTHORIZATION.json`
- `experiments/cs-ranking-binance-spot-archive-ptu-acquisition-v3/DIRECTION_REVIEW_ATTEMPT_4_RESULT_SCHEMA.json`
- this committed prompt

Evaluate and report every check separately:

0. Exact repository, human-amendment, source-parent, prompt, precheck, result-schema, revised-draft, reviewed-output-schema, and prior-attempt bindings.
1. Compatibility and methodological sufficiency of the reviewed output schema for the revised preregistration, including every embedded experiment, draft-hash, verdict, field, and closed-schema semantic. Do not assume that canonical hash authorization proves semantic compatibility.
2. Immutable Phase 1 and prior archive-route verdicts and permanent v1-manifest ineligibility.
3. Requirement that every v3 observation derive from newly retained official response bytes.
4. Raw XML reconstruction of CommonPrefixes, pagination, and ObjectRecords.
5. Exact development-only root and 101-month query scope, excluding 2026 and full-symbol listings.
6. Identity-preserving redirect-hop rules and metadata/object/checksum/retrieval cross-binding.
7. Executable ZIP, strict UTF-8/CSV, integer/Decimal, ignore-field, timestamp, and OHLCV validation.
8. Fail-closed identical/conflicting duplicate keys, revisions, gaps, malformed data, and exhaustive terminal count partitions.
9. Canonical logical request and attempt identities, pagination/retry lineage, immutable terminal outcomes, raw-only reconstruction, and safe idempotent resume.
10. Symbol-label-only identity, no notice path, no cross-symbol merge, gap/reappearance splitting, and fresh recovery.
11. Membership and liquidity use only completed pre-signal rows.
12. Retrospective last-bar knowledge cannot remove earlier membership.
13. Delisting, exact next-bar, additional-delay, terminal, and synchronization paths never forward-scan or fill partially.
14. Archive completeness and asset identity claims remain narrow and accurate.
15. Ex-ante sufficiency gates are adequate for a later separately preregistered cross-sectional baseline.
16. Requests, resources, retries, repair/audit budgets, storage, CPU, failure precedence, and data-rights scope are bounded without expansion.
17. `DATA_CONTRACT_GO` remains strictly non-economic and cannot authorize a strategy, model, return, holdout, trading, credential, capital, GPU, or mining action.
18. All seven attempt-3 required revisions are represented exactly and introduce no economic-methodology, acceptance-gate, or resource-scope change.

Return exactly one complete JSON object matching `DIRECTION_REVIEW_ATTEMPT_4_RESULT_SCHEMA.json`.

Verdict semantics are strict:

- `PASS`: every material check passes; limitations are non-blocking; `required_revisions` is empty; `data_acquisition_remains_prohibited=false`; `data_acquisition_may_be_authorized_after_parent_freeze=true`.
- `REVISION_REQUIRED`: at least one substantive check fails; enumerate every required revision precisely with categories chosen only from provenance, output_schema, data_methodology, economic_methodology, acceptance_gates, resource_scope, and safety_governance; `data_acquisition_remains_prohibited=true`; `data_acquisition_may_be_authorized_after_parent_freeze=false`.
- `REVIEW_INCONCLUSIVE`: no substantive verdict is possible because the review itself cannot complete; explain the service/context/transport/evidence limitation; `data_acquisition_remains_prohibited=true`; `data_acquisition_may_be_authorized_after_parent_freeze=false`.

No other verdict is permitted. A methodology failure is `REVISION_REQUIRED`, not `REVIEW_INCONCLUSIVE`. Passing controller tests is not evidence of data-contract validity or trading profitability.
