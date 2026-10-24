# Work log excerpt (2026-10-19 to 2026-10-24)

A condensed timeline taken from the private repository's append-only work log (655 lines). Numbers are copied from
that log, which records each step's measured result. Machine paths and agent coordination details are left out.

The work was done by AI coding agents: a lead agent that planned, ruled on questions and reviewed, and short-lived
worker agents that wrote the code, ran the jobs and wrote one report per task. The owner set the goal, the language
split, the hard rules (no look-ahead, point-in-time universe, sealed holdout, costs always on, keyless public sources
only) and took the decisions marked "owner" below.

## Day 1, 2026-10-19: survey, licences, Rust core, bulk download

- Free-data survey by six scouts: Binance archive chosen as primary (free, includes delisted contracts, exact fees and
  funding); Bybit, OKX and Kraken as cross-checks; US equities rejected (no free survivorship-free daily data).
- Licence review: CoinMarketCap, Deribit and Coinbase data quarantined because their terms forbid automated
  collection; the CMC fetcher now raises before any network call. Table: `DATA_LICENSES.md`.
- Rust core (PyO3): archive listing and resumable checksum-verified download, zip/CSV to zstd parquet
  (3.3M rows/s on one aggTrades day), validator (dupes, gaps, OHLC, stuck runs, spikes; 10M bars in under 1 s),
  bar resampling, cross-venue consensus, an event-driven simulator. Modules were checked against an independent
  reference (pandas, hand-computed cases, raw CSV row counts, or the owner's library) before merging.
- Bulk download of Binance USD-M 1m klines, funding, mark, premium and index prices for 1,066 symbols, with gap-fill
  from the daily archive where the monthly zips were short.
- First data-quality scorecard (M1-M14) and a queue of suspicious price windows, verified against Kraken trades.

## Day 2, 2026-10-20: panel, cost model, overfitting controls

- Curated 5-minute panel built: 170.7M rows, 684 instruments, published by staged build and atomic symlink swap.
- Point-in-time universe (top 100 by trailing 30-day median volume, data up to the previous day only).
- Pure-noise check of the overfitting harness on the real panel shapes: the best of K noise trials has a median
  deflated Sharpe of 0.51 and 0% above the 0.95 gate (it should look uniform; it does).
- Six bugs found by a blinded smoke run, including a Rust boundary bug (Fortran-ordered arrays read row-major);
  the simulator now rejects non-C-contiguous input.
- Exploratory mini test 1, conviction trend (a rebuilt older momentum strategy): in-sample Sharpe 0.68-0.87,
  out-of-sample -0.12 to -0.91, PBO 0.94. **No edge.** The older result (Sharpe 1.44) is explained by a static
  survivor list, no funding costs and a reused holdout. Owner verdict: the test worked, the strategy loses.

## Day 3, 2026-10-21: cross-venue checks, cost eras, Rust ports

- Exploratory mini test 2, delta-neutral funding carry (short perpetual, long Binance spot): out-of-sample Sharpe
  9.33 inside its own 198-configuration grid, max drawdown -0.1%, CAGR 4.8%. Net funding minus costs by year:
  2021 +25.7%, 2022 +0.3%, 2023 +2.8%, 2024 +9.1%, 2025 to September +1.4% of capital. Read as: real, cyclical and
  decaying carry, with exchange failure, auto-deleveraging and withdrawal freezes not priced. It is not a registered
  trial and the holdout was not read.
- Spread model era multipliers registered before any research result: quoted spreads exist only from 2023-05, so
  earlier years are charged more, from an effective-spread proxy.
- Rust bookTicker reducer: one month of ETH quotes in 6 minutes and 0.52 GB, against 4.4 hours and 2.85 GB in
  Python, with equality tests. A Polars port of the panel build was reverted: equal output but 24% slower.
- Internal holes in the lake re-filled from the archive and then the public REST API (klines 1m: 506,964 missing
  minutes down to 15,921, the rest absent from both sources).

## Day 4, 2026-10-22: audit, fixes, re-audit

- Audit A, seven read-only workers: 156,335 parquet files open; 431/431 archive checksums match; no duplicate keys
  or grid holes; panel equals the exchange on close, volume, trades, mark, premium, funding and open interest on
  sampled days.
- The audit also found twelve defects (F1-F12). The worst: tickers Binance reused for a new coin carried the old
  coin's history under one id (survivorship error and fake price jumps). Fixed by one instrument per listing.
- Re-audit on a new random sample: panel bars equal the exchange's own 5m klines on 17,280 of 17,280 bars, funding
  on 2,574 of 2,574 settlements. Scorecard: 14 green, 2 proven unfixable from free sources, 1 waived, 0 red.
  Test suite at that point: 765 passed. Details: `DATA_REPORT_2026-10-22.md`, `quality_proofs.md`.
- Found by the re-audit and documented, not fixed: the panel's last bar does not carry the funding settlement at
  its close.

## Day 5, 2026-10-24: design work, not yet run

- Design of a cost-aware portfolio learner and a screen of 33 older strategies under a declared trial budget.
  Neither had produced a result at the snapshot; both are excluded from this repository.

## Not done

The pre-registered research (`RESEARCH.md`, Stage 1 alpha-decay map and Stage 2 models) has not been run; only
blinded dry runs exist, sealed and unread. There is
no registered result, no holdout result, no paper trading and no live trading.
