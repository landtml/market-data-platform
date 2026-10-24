# Timeline

Milestones and measured results, condensed from the private repository's append-only work log. Every number is
copied from that log.

## Stage 1: survey, licences, Rust core, bulk download

- Survey of free data sources. Binance archive chosen as primary (free, includes delisted contracts, exact fees and
  funding); Bybit, OKX and Kraken as cross-checks; US equities rejected (no free survivorship-free daily data).
- Licence review: CoinMarketCap, Deribit and Coinbase data quarantined because their terms forbid automated
  collection; the CMC fetcher now raises before any network call. Table: `DATA_LICENSES.md`.
- Rust core (PyO3): archive listing and resumable checksum-verified download, zip/CSV to zstd parquet
  (3.3M rows/s on one aggTrades day), validator (dupes, gaps, OHLC, stuck runs, spikes; 10M bars in under 1 s),
  bar resampling, cross-venue consensus, an event-driven simulator. Each module was checked against a reference
  (pandas, hand-computed cases, raw CSV row counts, or an external research library, not included) before merging.
- Bulk download of Binance USD-M 1m klines, funding, mark, premium and index prices for 1,066 symbols, with gap-fill
  from the daily archive where the monthly zips were short.
- First data-quality scorecard (M1-M14) and a queue of suspicious price windows, verified against Kraken trades.

## Stage 2: panel, cost model, overfitting controls

- Curated 5-minute panel built: 170.7M rows, 684 instruments, published by staged build and atomic symlink swap.
- Point-in-time universe (top 100 by trailing 30-day median volume, data up to the previous day only).
- Pure-noise check of the overfitting harness on the real panel shapes: the best of K noise trials has a median
  deflated Sharpe of 0.51 and 0% above the 0.95 gate. Under noise it should look uniform, and it does.
- A blinded smoke run found six bugs, including a Rust boundary bug (Fortran-ordered arrays read row-major); the
  simulator now rejects non-C-contiguous input.
- Exploratory mini test 1, conviction trend (a rebuilt older momentum strategy): in-sample Sharpe 0.68-0.87,
  out-of-sample -0.12 to -0.91, PBO 0.94. No edge. The older result (Sharpe 1.44) is explained by a static survivor
  list, no funding costs and a reused holdout.

## Stage 3: cross-venue checks, cost eras, Rust ports

- Exploratory mini test 2, delta-neutral funding carry (short perpetual, long Binance spot): out-of-sample Sharpe
  9.33 inside its own 198-configuration grid, max drawdown -0.1%, CAGR 4.8%. Net funding minus costs by year:
  2021 +25.7%, 2022 +0.3%, 2023 +2.8%, 2024 +9.1%, 2025 to September +1.4% of capital. Read as: real, cyclical and
  decaying carry, with exchange failure, auto-deleveraging and withdrawal freezes not priced. It is not a registered
  trial and the holdout was not read.
- Spread model era multipliers registered before any research result: quoted spreads exist only from 2023-05, so
  earlier years are charged more, from an effective-spread proxy.
- Rust bookTicker reducer: one month of ETH quotes in 6 minutes and 0.52 GB, against 4.4 hours and 2.85 GB in
  Python, with equality tests. A batching rewrite of the panel build (Polars) was reverted: equal output but 24% slower.
- Internal holes in the lake re-filled from the archive and then the public REST API (klines 1m: 506,964 missing
  minutes down to 15,921, the rest absent from both sources).

## Stage 4: data checks and fixes

- Compared the panel with the exchange's own files and fixed the defects found. The worst: tickers Binance reused for
  a new coin carried the old coin's history under one id (survivorship error and fake price jumps). Fixed by one
  instrument per listing.
- Known limitation: the panel's last bar does not carry the funding settlement at its close.

## Stage 5: design work, not yet run

- Design of a cost-aware portfolio learner and a screen of 33 older strategies under a declared trial budget.
  Neither had produced a result at the snapshot; both are excluded from this repository.

## Not done

The pre-registered research (`RESEARCH.md`, Stage 1 alpha-decay map and Stage 2 models) has not been run; only
blinded dry runs exist, sealed and unread. There is no registered result, no holdout result, no paper trading and no
live trading.
