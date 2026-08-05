# Final narrow Route 4 provenance-architecture delta review

You are the final authorized independent methodology reviewer for `cs-ranking-binance-spot-archive-ptu-acquisition-v3`. Work read-only. Do not modify any file. This is a narrow delta review, not direction-review attempt 5 and not a restart of Route 4 methodology research.

Use only committed repository evidence. Do not access any network or Binance resource, resolve or read market-data or holdout paths, train a model, implement a strategy, run a backtest, calculate returns, or authorize acquisition/implementation.

## Exact bindings

- Source content commit: `04071a14a53bfc56ef0349213cce9dc05434eda8`
- Revised draft v2 byte SHA-256: `5e579d723df05e6ad940357916099d4fd49d0ca2ea744a16310d3483111d9188`
- Revised draft v2 canonical SHA-256: `c770c134dc0e75548c352e21f57a99bcd2deffd4813b8b580c5067ad91a05034`
- Resolved effective draft canonical SHA-256: `d771589e1ece1347e1f772bc76ac6611bd64433de39f1f7a9f5b2ab295e85446`
- Delta-review output schema byte SHA-256: `8e149b74111993784ef1a24a4582d8548d54cab76ad1f3752862ed57e4fd27f2`
- Delta-review output schema canonical SHA-256: `99adeb870981faaadb1a723f963be2cce8ffc6dee81eed6dbbaeb5d99c828dec`
- Network-event schema byte SHA-256: `ce95f18c7e365e157ab273ce7673057b93e7b21fb842935dacf29f6b9fd39917`
- Network-event schema canonical SHA-256: `a205622b3be107c62f1bd307bc619a1a230dd3e115a4f3fcd3c34047c69ab159`
- Deterministic preflight byte SHA-256: `d3cb4861bda11f26bfe4b33215e51fc6e29c024abd604fac5c6fe06bc5876ea4`
- Deterministic preflight canonical SHA-256: `e34b0540a0155e784ec68df4a0ef2e407bf07d8bdcda16919c7bf052e5573ebb`
- Pre-review bundle byte SHA-256: `636d791175534623b3bd8c85e17adeb796ee722cf8458b95cdab9ea5b2b54e1f`
- Pre-review bundle canonical SHA-256: `c87da4d5356dd844d4ce56be3823ad271c44cf25793e5d6791c83dd410ffe996`
- Attempt-4 result byte SHA-256: `2e65ca54291e4852d0e36b3372e6df4e8176e01d7dcb73cb066e735c8af45292`
- Attempt-4 result canonical SHA-256: `23588f15d16fd640d6942d6b1bd82cd926e01b0064e895f559f3f559db699b83`

Independently verify these hashes and confirm that the exact draft, output schema, network-event schema, and validator files at the content commit match the worktree. If reviewed content differs, do not issue a substantive review; report `REVIEW_INCONCLUSIVE` and identify `PRE_REVIEW_CONTENT_MUTATION`.

## Required narrow review

Review substantively:

1. Attempt-4 failed check 1: independently canonicalizable draft and review schema, absence of stale/reciprocal embedded content hashes, acyclic binding through the pre-review bundle, and deterministic post-PASS freeze-manifest design.
2. Attempt-4 failed check 8: every issued response and no-response attempt is truthfully representable; response-only fields are explicit null without a response; repeated attempts remain distinct, ordered, immutable, and counted; retryability and observable transport facts do not invent unavailable facts.
3. Attempt-4 failed check 9: exactly one immutable terminal outcome per attempted logical request; terminal evidence binds ordered attempts and the ledger prefix; deleting or ignoring mutable acquisition state still permits exact terminal reconstruction; truncation, reordering, mutation, missing attempts, and multiple terminals fail closed.
4. The deterministic preflight is semantically adequate for the machine-checkable parts of all nineteen attempt-4 checks and its `HUMAN_METHODOLOGY_ONLY` classifications are explicit and reasonable.
5. The bounded delta did not alter or invalidate attempt-4 checks 0, 2-7, or 10-18. Report one regression entry for each of those sixteen check numbers.

Inspect at minimum the exact bundle, preflight, revised draft v2, predecessor draft, network-event schema, delta-review output schema, `route4_contract.py`, `route4_preflight.py`, both Route 4 test files, attempt-4 artifacts, revision-incorporation evidence, Phase 2/Route 4 authorizations, research protocol, and acceptance gates.

## Verdict rule

- `PASS` only if checks 1, 8, and 9 all pass, the binding is non-circular, raw-only reconstruction is sufficient, the preflight is correct, and none of the previous sixteen passes is invalidated.
- `REVISION_REQUIRED` for any substantive local failure or newly exposed nonlocal methodology defect. Enumerate every required revision and its category.
- `REVIEW_INCONCLUSIVE` only when durable evidence is insufficient to reach a substantive verdict; service/transport failures are classified by the parent controller, not fabricated in the review.

Return exactly one object conforming to `DIRECTION_DELTA_REVIEW_OUTPUT_SCHEMA_V2.json`. In `source_commit`, report the full clean commit currently checked out. In `source_content_commit`, use the exact content commit above. In `pre_review_bundle_sha256`, use the bundle canonical SHA-256. Echo the exact draft and output-schema byte/canonical hashes. The three `delta_checks` entries must be checks 1, 8, and 9 exactly once each; the sixteen `regression_checks` entries must be checks 0, 2-7, and 10-18 exactly once each.
