# Durable Progress Log

- Current checkpoint: BTC/ETH trend data contract passed without parsing holdout values; revised exact preregistration is ready to freeze.
- What changed: overall state restored to `ACTIVE_RESEARCH`; immutable `cs-ranking-ptu-data-audit-v1 = DATA_NO_GO` preserved; distinct archive audit frozen; production Codex invocation, safe locking, archive enumeration, manifest hashing, and causal-universe contract added.
- What was verified: Codex 0.146.0, ChatGPT authentication, Sol/Terra/Luna CLI catalog, two successful Sol/xhigh live responses, source and portfolio publication, portfolio CI and Pages deployment, zero capital, no holdout access, no performance claim, lint, strict typing, and deterministic tests.
- What failed: restricted-sandbox live startup, one initial response-shape validator attempt, the first archive run on the `KLAYUSDT` post-cutover timestamp anomaly, and the final Sol/xhigh audit timing out after 180 seconds. None triggered a model downgrade; the archive anomaly is preserved and quarantined under the single bounded repair, while the absent audit verdict is recorded as `AUDIT_INCONCLUSIVE`.
- Current best defensible result: no validated strategy candidate; BTC/ETH source integrity is verified for the trend experiment while the archive route remains ineligible.
- Next experiment: freeze and implement `btc-eth-vol-targeted-trend-v1` against the manifest-hashed BTCUSDT and ETHUSDT data while the 2026 holdout stays closed.
- Current blocker: none. The public source remote exists and meaningful checkpoints are pushed only after secret and publication-safety checks.
- Exact resume state: freeze the revised preregistration against the data-verifier source commit, then use Terra/medium for a deterministic development-only implementation. The next live call is experiment call two of four; 2026 value data remain prohibited until every development gate and a Sol/xhigh pre-holdout audit pass.
