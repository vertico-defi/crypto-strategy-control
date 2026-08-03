# Durable Progress Log

- Current checkpoint: the single development cycle completed `HISTORICAL_NO_GO` against source `f8b0208...`; canonical result is `983ad070a4470c8494ee47a6f48ae146a3bb0014e228e96ca7e97cc8eddd72b1` and is pending an evidence commit.
- What changed: only the 36 allowlisted 2024–2025 partitions were evaluated once. The primary lost 7.78% net, had 0.090 annualized common-panel Sharpe and 44.34% event-level drawdown over 364 intervals and 525,653 event observations.
- What was verified: result/preregistration/data hashes, 25-gate map, asset reconciliation to `2.91e-16`, finite trial outputs, 47 focused and 176 total tests, Ruff, strict typing across 22 source files, closed holdout, no candidate, and zero capital all pass.
- What failed: 13 frozen gates, including net return, Sharpe, drawdown, doubled cost, delay, fold majority, neighbor stability, all bootstrap lower bounds, DSR, PBO, baseline superiority, BTC contribution, and regimes. Repair 1/1 and cycle 1/1 are consumed.
- Current best defensible result: deterministic development `HISTORICAL_NO_GO`, not a candidate. It is not terminally trusted until the independent Sol audit.
- Next experiment: commit result/validation and run call 3/4 as read-only Sol/xhigh terminal audit, then preserve the verdict and select a distinct family.
- Current blocker: none. The 2026 holdout remains unopened/unread; scheduled continuation remains disabled under interactive Goal ownership.
- Exact resume state: experiment `btc-eth-causal-volatility-parity-rebalancing-v1`; result `983ad070...`; calls 2/4, repairs 1/1, cycles 1/1 used, GPU/capital zero, next task terminal audit after clean evidence commit.
