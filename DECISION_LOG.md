# Decision Log

## 2026-08-02

The first family is cost-aware cross-sectional ranking. The first bounded task is a point-in-time-universe data-contract audit; no model or holdout is permitted until it passes.

Program-level correction only: `cs-ranking-ptu-data-audit-v1` remains terminal `DATA_NO_GO`, independently confirmed as `DATA_NO_GO_CONFIRMED`. That scoped registered-inventory failure opened no holdout and calculated no returns; it does not exhaust lawful zero-cost data routes or approved strategy families. The program therefore transitions from `DATA_BLOCKED` to `ACTIVE_RESEARCH` and selects `cs-ranking-binance-spot-archive-ptu-audit-v1` as the next bounded experiment.

Live Codex operation was verified with CLI 0.146.0 authenticated through ChatGPT. The available catalog includes `gpt-5.6-sol`, `gpt-5.6-terra`, and `gpt-5.6-luna` with the requested reasoning levels. A production-controller smoke review ran as `gpt-5.6-sol`/`xhigh` in `live` mode from `2026-08-02T16:07:41.669180Z` through `2026-08-02T16:08:07.617703Z` (response `019fc33b-27f3-7883-abc0-2377dbcf9ced`) and passed its zero-capital/no-holdout/no-performance-claim contract. The result text was not persisted; its hash and invocation metadata were recorded. Mock mode remains restricted to tests.

The continuation design now uses a kernel advisory lock with owner metadata rather than age-based lock stealing. Versioned systemd continuation units are bounded to one cycle and 15 minutes, but scheduling remains disabled while this live Goal owns the program.

After a credential-pattern, environment-file, large-file, raw-data, and absolute-path publication scan passed, public source repository `vertico-defi/crypto-strategy-control` was created without rewriting history and commit `8df45df236ebcd1d2917da1c11a7b88bbc0e057f` was pushed to `main`.

Official Binance documentation confirms that `data.binance.vision` publishes public spot klines in monthly archives with companion SHA-256 checksum files, and the official bucket returned a paginated `ListBucketResult` for `data/spot/monthly/klines/`. The next preregistration therefore freezes S3 marker pagination, `1d` UTC bars, a 2017-08 through 2026-06 complete-month sample, no current `exchangeInfo`, and an archive-observed—not formally complete—universe claim.
