# Phase 3 capital-readiness ladder

This document is research-only. The active maximum capital exposure and every active sleeve exposure are exactly zero. Nothing here authorizes orders, credentials, wallets, leverage, or capital use.

## Stage 0 — zero-capital historical research

Maximum capital exposure: zero. Maximum strategy exposure: zero. Daily, weekly, and total-loss limits: zero because no orders or capital are permitted. Historical work must be chronological, cost-aware, reproducible, and independently audited; the final holdout stays sealed until its gates permit access.

## Stage 1 — zero-capital frozen prospective evaluation

Maximum capital exposure: zero. Maximum strategy exposure: zero. Daily, weekly, and total-loss limits: zero. Exact code, configuration, data contract, environment, and decision rules are frozen. No retraining or discretionary override is allowed. Promotion requires the prospective gates in `ACCEPTANCE_GATES.yaml` and human review.

## Stage 2 — separately authorized micro-live evaluation, no leverage

Not authorized. A future human authorization must set an absolute maximum funded amount, and the effective capital limit must be the lesser of that amount and deposited risk capital. Borrowed capital, emergency funds, household funds, and funds needed for obligations are prohibited. Withdrawal-disabled API credentials would be mandatory. Until a separate versioned risk contract is approved, maximum capital and strategy exposure remain zero and all loss limits remain zero.

That future risk contract must set explicit currency and percentage caps for total capital, each sleeve, daily loss, weekly loss, and cumulative loss. Any missing limit fails closed. The first breached limit, data-integrity failure, unexpected order behavior, credential anomaly, excessive execution cost, strategy-health suspension, or loss of monitoring triggers the kill switch and requires manual review.

## Stage 3 — limited staged increases

Not authorized. Each increase requires a predefined forward checkpoint, intact execution/drawdown/health gates, an explicit new absolute capital ceiling, and manual approval. Monthly deposits remain outside the system unless a human explicitly assigns a bounded portion after the checkpoint; no deposit is automatically deployed.

## Stage 4 — continued scaling

Not authorized. Scaling can continue only after separately approved forward checkpoints while execution, drawdown, loss, and sleeve-health gates remain satisfied. Every increase requires manual approval. No borrowed capital or automatic funding is permitted at any stage.
