# quant: design and interface

> From the private research repository (snapshot), edited for readers outside it: internal task references and process notes were removed and the wording was tightened. Rules, numbers and dates are unchanged. Paths such as `libs/...`, `configs/...` and `data/...` refer to that repository and its data folder, which are not published here; the root README.md lists what this repository contains.

Goal: a retail intraday ML test whose results survive survivorship, look-ahead, corporate-action/redenomination,
bad-tick and cost biases. Most effort goes into the dataset. Evaluation uses an external research library (not
included).

Primary market: Binance USD-M perpetuals (free archive with delisted symbols, exact fees, funding, 24/7), chosen
after a survey of free sources and cross-checked against other venues.

## Language split (each language where it fits best)

| Job | Language | Why |
|---|---|---|
| Archive listing, concurrent download, checksum verify, retry/backoff, per-host rate limits | Rust (tokio + reqwest) | concurrency, memory safety on long unattended runs |
| zip/CSV parse, schema/units normalisation, Parquet write (zstd) | Rust (arrow-rs, parquet, rayon) | throughput, bounded memory on GBs |
| Validation: gaps, dupes, OHLC sanity, outliers vs mark/index/other venues, issue table | Rust | scans every row of every file |
| Bars from trades (dollar/volume/imbalance), resampling, cross-venue consensus | Rust | streaming, path-dependent loops |
| Event-driven execution simulator (positions, fees, spread, slippage, funding accrual, delist exits) | Rust | sequential path-dependent state |
| Fixed-shape panels [T, N] with masks: features, IC / alpha-decay curves over horizons, cost sweeps, bootstrap / Monte Carlo, many small models over CPCV folds x hyper-params via vmap | JAX on GPU (ROCm) | static shapes, vmap, jit |
| Orchestration CLI, small REST sources (macro, sentiment), instrument master, research-library glue (labels, CPCV, DSR, PBO, trial registry), reports | Python | glue code; the research library is Python |

Small keyless REST backfills (Binance fapi public endpoints, a few thousand requests) use Python's stdlib `urllib` instead of the Rust layer: no Rust REST client exists and the volume does not justify one. Decided early (code: quant.data.pipeline.rest_backfill).

## Layout

```
Test/
  pyproject.toml            maturin build backend; [tool.maturin] manifest-path = "libs/core/rust/Cargo.toml",
                            python-source = "python", module-name = "quant.core._qcore"
  libs/core/rust/             THE Rust crate (all Rust lives here)
    Cargo.toml
    src/lib.rs              #[pymodule] _qcore: registers the functions below; nothing else
    src/fetch.rs            list_prefix, fetch_manifest
    src/convert.rs          convert_zip_csv
    src/validate.rs         validate_bars, validate_series
    src/bars.rs             resample_bars, trade_bars, consensus
    src/sim.rs              simulate_targets, simulate_events
    src/util.rs             shared types, errors, time helpers
    tests/                  cargo integration tests
  libs/core/quant/core/
    __init__.py             re-exports _qcore
    cli.py                  `python -m quant <cmd>`
    paths.py                every data path in one place
    sources/                one module per source (binance.py, bybit.py, okx.py, tardis.py, fred.py, ...)
    catalog.py              instrument master, rename/redenomination chains, PIT universe
    costs.py                fee schedule history, spread model calibration (JAX)
    features.py             JAX panel features
    research/               labels, cv, trials, report (research-library glue)
  tests/                    pytest
  data/raw/ data/lake/ data/curated/ data/samples/
  WORKLOG.md DESIGN.md
```

## Rust API (Python-facing, module `quant.core._qcore`)

All timestamps crossing the boundary are int64 nanoseconds since epoch, UTC. Thread pools are capped at 6 threads
(machine rule: cores 0-5). Errors raise Python exceptions with the file/URL in the message; never panic across FFI.

```python
list_prefix(bucket_url: str, prefix: str, max_keys: int = 1000) -> list[dict]
    # S3 ListObjects (follows pagination). dict: key, size, last_modified (ns), etag

fetch_manifest(items: list[dict], dest_root: str, *, concurrency: int = 8, per_host: int = 4,
               verify_checksum: bool = True, retries: int = 5) -> dict
    # items: {url, dest (relative to dest_root), checksum_url | None, size | None}
    # resumable (skip if dest exists with matching size/sha256), atomic (write .part then rename),
    # backoff on 429/5xx, hard stop of a host for 60 s on 418, progress lines to stderr every 10 s.
    # returns {ok, skipped, failed: [{url, error}], bytes}

convert_zip_csv(zip_path: str, out_parquet: str, schema: str) -> dict
    # schema in {"binance_klines", "binance_aggtrades", "binance_funding", "binance_metrics",
    #            "binance_bookticker", "bybit_trades", ...} (registry in convert.rs)
    # detects header/no header, normalises ts unit (ms/us -> ns), writes zstd parquet.
    # returns {rows, ts_min, ts_max, ts_unit_in, had_header}

validate_bars(ts, o, h, l, c, v, interval_ns: int, stuck_limit: int = 30) -> dict
    # numpy arrays in (Python reads parquet with pyarrow; Rust stays free of IO here, GIL released).
    # returns {rows, missing, dupes, unsorted, ohlc_bad, zero_vol, nonpositive, stuck_runs, max_gap_ns,
    #          max_gap_at, issues: list[{rule, ts, n, detail}], flags: np.ndarray[uint32] per row}
validate_series(ts, x, expected_step_ns: int | None, stuck_limit: int, name: str) -> dict
    # same checks for one value series (funding, mark, OI): dupes, unsorted, gaps, stuck runs (V2)

resample_bars(ts, o, h, l, c, v, qv, n, tbv, tbqv, out_interval_ns) -> dict[str, np.ndarray]
trade_bars(ts, price, qty, is_buyer_maker, kind: str, threshold: float) -> dict[str, np.ndarray]
consensus(ts_list, close_list, max_dev_bps) -> dict   # cross-venue median, flags

simulate_targets(target, open_, close, tradable, spread_bps, fee_bps, funding, settle, dollar_vol, sigma,
                 impact_k: float, max_participation: float, equity0: float = 1.0) -> dict
    # All panels float64/bool [T, N], C-contiguous. target[t] = signed fraction of equity per instrument decided
    # at the CLOSE of bar t; it is executed at the OPEN of bar t+1 (never the same bar).
    # Cost per trade (bps of traded notional) = fee_bps[t] + spread_bps[t+1, i]/2
    #   + impact_k * sigma[t, i] * sqrt(traded_notional / dollar_vol[t, i])   (sigma in bps per bar)
    # traded notional is capped at max_participation * dollar_vol[t, i]; the unfilled rest carries over.
    # Positions are held in units, so weights drift with prices; turnover is measured on drifted positions (V5).
    # funding[t, i] = rate charged at bar t (0 when no funding event); cash flow = -rate * position_notional
    #   (a long PAYS when the rate is positive: V1).
    # tradable[t, i] false (filled / missing bar, not listed yet, settled): no trade on that bar, position held (V3);
    #   no NaN ever enters the loop (assert).
    # settle[t, i] true: forced close at close[t, i] * (1 - haircut) where haircut is a param in bps.
    # returns {equity[T], gross_ret[T], fee[T], spread_cost[T], impact_cost[T], funding[T], net_ret[T],
    #          turnover[T], gross_exposure[T], net_exposure[T], pnl_by_instrument[N], n_trades}
simulate_events(entries: [K] (t_idx, i_idx, side, size), close, open_, tradable, pt_mult, sl_mult, vol [T,N],
                max_hold_bars, + the same cost/funding/settle arguments) -> dict
    # triple-barrier execution: barriers checked on bar CLOSE (pt = +pt_mult*vol, sl = -sl_mult*vol from entry);
    # a hit at close t exits at OPEN t+1 (gaps go through barriers: V6); vertical barrier exits at open after
    # max_hold_bars. Returns per-event {entry_t, exit_t, exit_reason, gross_ret, net_ret, costs, funding} plus the
    # same per-bar series as simulate_targets.
```

## Data layout and contracts

- `data/raw/<source>/...`: mirror of the source's own paths. Immutable. Zips may be deleted after conversion when
  the source is re-fetchable; their sha256 stays in `data/raw/<source>/CHECKSUMS.tsv`.
- `data/lake/<source>/<dataset>/<interval>/<SYMBOL>/<YYYY-MM>.parquet`: normalised, one file per source file.
  Column `ts` = bar open time, int64 ns UTC. Prices float64, volumes float64.
- `data/curated/`: the dataset the research reads.
  - `instruments.parquet`: instrument_id, venue, symbol, base, quote, contract, onboard_ts, last_ts, delist_announce_ts,
    multiplier (1000x contracts), successor_id / predecessor_id (renames, swaps), exclude_reason.
  - `bars_<interval>/<YYYY>.parquet`: long panel (ts, instrument_id, OHLCV, quote_vol, taker_buy_quote_vol, trades,
    mark_close, index_close, funding_rate_at_ts, n_venues_ok, issue_flags bitmask).
  - `universe_pit.parquet`: date, instrument_id, eligible, rank, built only from data up to the previous day close.
  - `context_daily.parquet`: macro, sentiment, implied vol, stablecoin supply; each column shifted by its measured
    publication lag.
  - `costs.parquet`: fee schedule history and the calibrated spread/slippage model inputs.
  - `issues.parquet`: every validation issue, with the rule id from the pitfalls catalogue.
  - `MANIFEST.json`: sources, URLs, checksums, row counts, build time, code version.

## Bias controls (each one is a check that runs)

1. Survivorship: universe from the full archive listing incl. delisted symbols; count of delisted in the universe is
   reported per year.
2. Look-ahead: universe, context and cost-model inputs use data up to t-1 only; context columns shifted by publication
   lag; a test that perturbs future data and asserts past features do not change. One documented exception: the cost
   CALIBRATION (cost_model.json, configs/cost_era_mult.json) is fitted once on all spread observations, including later
   years, because costs are charged to results and never read by a signal; it measures what trading actually cost.
   A strategy that uses predicted cost to make decisions (cost-aware sizing or filtering) must use a point-in-time
   fit instead.
3. Redenomination/renames: explicit chains with ratios from the pitfalls catalogue; returns never cross a chain
   without the ratio.
4. Delistings: exit at the last tradeable price after the announcement with a haircut; no trading after the
   announcement in the research.
5. Bad ticks: last-price outliers vs mark/index and other venues are flagged and kept in the data.
6. Costs: point-in-time fee schedule, spread model calibrated on real quotes (bookTicker / free tick samples),
   funding accrued at actual funding times, slippage scaled by size vs volume.
7. Overfitting: every configuration tried is registered in the research library's TrialRegistry; DSR, PBO (CSCV)
   and CPCV paths reported; a final holdout period untouched until the end.

## Rules carried over from the lessons of an earlier project (its strategy section not used)

Each becomes a test or a validator rule.

- V1 Funding sign: a test asserts that a long perp pays when the funding rate is positive, checked against a hand
  computed day; a flipped sign is not detectable from P&L alone (earlier project: ~10 pp CAGR).
- V2 Stuck feeds: a run of identical values longer than a per-dataset limit (funding, mark, OI, close) is an issue
  `stuck_feed`; the bar is flagged, never silently used as information.
- V3 Gaps: never drop a symbol on a missing bar and never pass NaN into the simulator. Missing bar: price carried
  forward with zero volume and flag `filled`, position held, no new entries on filled bars.
- V4 Release time: a context value is usable from its release time, not its observation date. Where the source has no
  historical release timestamps, use a documented conservative lag (ALFRED vintage + 2 days; CFTC COT only if the
  release date is verifiable, otherwise excluded).
- V5 Costs from real OHLCV + quotes, turnover measured on drifted (not target) positions.
- V6 Stops and barriers fill at the next available price after the trigger (gaps go through stops), never at the
  barrier level.
- V7 Every candidate is compared with a gross-matched cash control (same average exposure) and with buy-and-hold of
  the universe; a lower-vol de-grossed copy is not an edge.
- V8 All cross-asset or portfolio-level inputs are lagged one bar; a test shifts each input by -1 bar and asserts the
  result changes only when that input is lagged.
- V9 Holdout: 2025-10-01 onward is untouched until the single final run; nothing is ranked on it.
- V10 Rebalance phase: results that depend on the bar phase are averaged over phases, not reported for the best start.
- V11 DSR sanity: before trusting a DSR, run the same pipeline on N pure-noise trials; DSR of the best noise trial
  must be roughly uniform across seeds. Sharpe, its variance across trials, and n_obs are all in the same per-bar
  units (the earlier project saw DSR stuck at ~1.0 at 150 trials: a units or variance error).
- V12 Binance `metrics` (OI) archive starts 2020-09; it has duplicate and out-of-order rows: dedupe + sort, then
  count missing 5-min slots per day and drop days below a completeness threshold.

## Curated panel contract (supersedes the bars_<interval> line above)

Code: `libs/data/quant/data/panel.py`. Readers: features, research, cost fit.

- `data/curated/bars_5m/<YYYY>.parquet`: long panel sorted by (ts, instrument_id), zstd, 25 columns. One instrument is
  one listing: a ticker that Binance reused has one instrument per listing. An instrument's rows run
  from the first source bar at or after its onboard (floored to 5m) to its end: `listed_end_ts` for an earlier listing
  of a reused ticker (`last_ts_source` = `last_traded_bar`; its last traded bar is `last_ts`), else settle_ts, else the
  5m bar holding the last non-zero-volume 1m bar. Never past the onboard of the next listing of the same symbol. Source
  5m rows of a ticker that fall in no listing are dropped and counted in `panel_stats` `build.dead_period_rows`. This
  removes the flat post-delist tail and the dead period between two listings. No NaN in OHLC ever (V3). Every missing
  float is null, never NaN. The 1000x contracts keep their quoted price; `multiplier` lives in instruments.parquet.
  Row t is the bar opened at ts = t (UTC ns).

  "Usable from" is the first instant a reader may use the value. Bar fields are final at the bar close (t + 5 min),
  so a decision at close t uses rows up to t. Staged-rebuild inputs follow `panel_inputs.py`.

  | column | type, unit | usable from |
  |---|---|---|
  | `ts` | int64, ns UTC, bar open | key |
  | `instrument_id`, `symbol` | str | key |
  | `open high low close` | f64, quote price (USDT) | t + 5 min |
  | `volume` | f64, base units | t + 5 min |
  | `quote_volume`, `taker_buy_quote_volume` | f64, USDT | t + 5 min |
  | `trades` | i64, count | t + 5 min |
  | `mark_close` | f64, USDT; null if no mark bar | t + 5 min |
  | `premium_close` | f64, Binance premium index (dimensionless); null if none | t + 5 min |
  | `funding_rate` | f64, fraction per funding period; positive = long pays; 0 on bars without a settle | t + 5 min (settled at the bar close) |
  | `funding_interval_h` | i8, hours; 0 before the first placed event | t + 5 min |
  | `filled` | bool, no source bar: price carried, volume 0 | t + 5 min |
  | `tradable` | bool, see the rule below | t + 5 min |
  | `issue_flags` | u32, bit set (below) | t + 5 min |
  | `oi_sum_open_interest` | f64, base units, Binance metrics as published; null when the source is 0 (zero-OI rule) | stamp t (`panel_inputs.oi_rows`) |
  | `oi_sum_open_interest_value` | f64, USDT, same stamp; null when the source is 0 (zero-OI rule) | stamp t |
  | `index_close` | f64, USDT; close of the index kline opened at t - 5 min, also across a month boundary (the first bar of a month reads the previous month's last kline, F3) | t (kline closes before t) |
  | `spot_close` | f64, USDT; close of the spot kline opened at t - 5 min, same month rule as `index_close` | t (kline closes before t) |
  | `bybit_funding_rate` | f64, as published by Bybit; null before the first event | latest `usable_from_ts` <= t (asof, backward) |
  | `bybit_oi_1h` | f64, as published by Bybit; null before the first event | latest `usable_from_ts` <= t (asof, backward) |
  | `delist_announced` | bool | t >= earliest `release_ts` of a mapped futures delist notice |

- `data/curated/universe_pit.parquet`: `date` (UTC day D), `instrument_id`, `days_listed`, `median_qv_30d` (median
  daily quote volume over D-30..D-1), `rank`, `eligible` (days_listed >= 30 and median_qv_30d >= 20e6 and rank <= 100,
  excluded instruments never eligible). Uses data up to the end of D-1 only. A test perturbs day D's bars and asserts
  row D does not change.
- `quant.data.panel.load_dense(start, end, fields, ids=None, interval="5m") -> Dense`: `Dense(ts[T] int64, ids[N] list,
  x: dict field -> float32 [T,N], present bool [T,N], tradable bool [T,N], eligible bool [T,N])` where eligible is
  the universe flag of the bar's day broadcast to its bars. Fixed shapes for JAX: T and N are padded to multiples of
  `pad` (default 64) with present=False. This is the only entry point research code uses.
- Summary `data/curated/panel_stats.json`: rows per year, instruments per year, delisted instruments per year,
  eligible count per day (min/median/max), ever-eligible count, delisted-in-universe count per year. `build` holds
  `dead_period_rows`, `stale_run_rows`, `stale_runs`, `tradable_false_rows` (counted after every flag pass),
  `tradable_false_by_bit` (tradable=False rows carrying each panel bit) and `bit_rows` (all rows carrying each bit).
- Publishing: every build and every flag pass writes to `<root>/.staging-<stamp>/`, validates it
  (schema, year coverage, row total against panel_stats, universe present), moves it to `<root>/versions/<stamp>/`,
  and publishes it with ONE atomic symlink swap of `<root>/current`. `bars_5m`, `universe_pit.parquet` and
  `panel_stats.json` are symlinks through `current`. `load_dense` and `iter_dense` resolve `current` once per call, so
  one read sees one version. Retention: after a successful swap, all versions except `current` and the newest other
  version are deleted (about 8.3 GB each); a failed validation changes nothing.
- Limitation (documented, unsolved): a process that opens the legacy symlinks file by file while a swap happens
  can still see two versions in one run. Readers that need one consistent set must call `load_dense` / `iter_dense`,
  or resolve `data/curated/current` once and read from that directory.
- `tradable` is false on: filled bars; bars in a market-wide outage or flat window (`market_window`, `flat_market`);
  bars in a Kraken `outage_confirmed` window (`kraken_outage`); bars in a bad-print consensus window classed
  `refuted` (`consensus_refuted`); the last 30 min before the end of a delisted instrument (`delist_tail`); and
  single-bar prints (`single_bar_print`) and stale runs (`stale_run`). A Kraken `refuted` verdict sets `kraken_refuted`
  as evidence and does not change `tradable`: the 3-venue consensus decides. Premium and mark holes (`premium_hole`,
  `mark_hole`) leave the value null and do not change tradability.
- Single-bar print (`single_bar_print`, redefined; scorecard metric M6). Bar t is a
  candidate when (1) it is off: a real bar (volume > 0, not filled) with a mark whose |close / mark_close - 1| is more
  than 50 bps; (2) it is a run of one: neither neighbour (t-5 min and t+5 min, same instrument) is off, so a neighbour
  without a mark is not off and does not block the print; (3) its close is not within 50 bps of either neighbour's mark
  (a lagging mark explains the print). A candidate gets the bit and `tradable = false` (price kept) when no other
  untradable bit is set on the bar: the base condition, over filled, market_window, kraken_outage, delist_tail,
  flat_market, consensus_refuted and stale_run. A bar already untradable for another reason gets no single-print bit.
  A candidate inside a `confirmed` consensus window is exempt (another venue saw the move; switch
  `SINGLE_PRINT_CONFIRMED_EXEMPT`, on). A sustained divergence (two or more consecutive off bars) is not a candidate and stays tradable.
  Calibration on the inputs at that stage, dev span (ts < 2025-10-01): 32,189 candidates under the current definition before
  the exemption, the same count a separate script produced when the rule was defined; 25,721 with the exemption, which
  is the published rule.
  CAUSALITY: the rule reads bar t+5 min, so it is a DATA-CLEANING flag. It decides whether a fill at print t is
  believable; it is not a feature input. No feature may read `issue_flags` or `single_bar_print`
  (`tests/bias_checks/test_panel_rebuild_r5.py` enforces this by a source scan). A `disputed` window does not exempt a bar.
- Stale run (`stale_run`, added with the data fixes). A run of at least 288 consecutive 5m bars (one UTC day) of one
  instrument with volume 0 and an unchanged close (close equal to the previous bar's close) gets the bit and
  `tradable = false`. Prices are kept. Runs are found on each listing's grid before the span build; a gap row is a
  volume-0 bar carrying the previous close, so a run across a gap is one run. The frozen tails the audit found were the
  dead periods of earlier listings of reused tickers; with listing segments those rows are dead and dropped, so the bit
  covers the frozen stretches inside a live listing.
- Zero open interest (zero-OI rule). A zero `sum_open_interest` or `sum_open_interest_value` on a live perpetual
  is a bad value, not a reading: that column is null on the bar (`panel_inputs.oi_rows`). A bar with no OI stamp is
  null as well.
- Excluded instruments (`exclude_reason` in `instruments.parquet`, built by `quant.data.catalog`): the panel builds
  only rows with a null reason. `never_traded` marks a PENDING_TRADING perpetual with no REST klines (REST code -1122)
  and no archive 1m or 5m file (GAIBUSDT, listed 2025-11-20, never traded as of the snapshot).
- `issue_flags` bits (`PANEL_BITS` in panel.py). Bits 0-9 are the validate bits (quant.flag_bits); 10-24 are panel bits:

  | bit | value | meaning | tradable |
  |---|---|---|---|
  | `filled` | 1<<10 | no source bar, price carried | false |
  | `market_window` | 1<<11 | market-wide flat/outage window | false |
  | `kraken_outage` | 1<<12 | Kraken outage_confirmed window | false |
  | `kraken_refuted` | 1<<13 | Kraken refuted window (evidence only) | unchanged |
  | `delist_tail` | 1<<14 | last 30 min of a delisted instrument | false |
  | `spike_window` | 1<<15 | suspicious spike window (review queue, see below) | unchanged |
  | `mark_divergence_window` | 1<<16 | suspicious mark divergence window (review queue, see below) | unchanged |
  | `funding_fallback` | 1<<17 | funding placed on an earlier bar | unchanged |
  | `ohlc_repaired` | 1<<18 | source OHLC NaN or non-finite, carried | unchanged |
  | `flat_market` | 1<<19 | market-wide flat window (q-score M4) | false |
  | `premium_hole` | 1<<20 | premium_close NaN on a live bar | unchanged |
  | `mark_hole` | 1<<21 | mark_close NaN on a live bar | unchanged |
  | `consensus_refuted` | 1<<22 | consensus 'refuted' window | false |
  | `consensus_disputed` | 1<<23 | consensus 'disputed' window (robustness mask) | unchanged |
  | `single_bar_print` | 1<<24 | one-bar print > 50 bps from mark (definition above; data-cleaning; reads t+5 min) | false |
  | `stale_run` | 1<<25 | >= 288 consecutive bars with volume 0 and an unchanged close (price kept) | false |

- Consensus (`lake/verify_consensus.parquet`, built by `sources/bybit.py` `build_consensus`): each window's class from
  the decided Kraken, Bybit and OKX verdicts. `disputed` = at least one venue confirms and one refutes; `confirmed` =
  a venue confirms, none refutes; `refuted` = a venue refutes, none confirms; `undecided` = no venue decided. A window
  with symbol `MARKET` applies to every symbol. `refuted` and `disputed` set bits; `confirmed` windows exempt
  single-bar prints (`SINGLE_PRINT_CONFIRMED_EXEMPT`, on).
- REST fill rule (`lake/rest/`): REST never fills bars. Premium: the archive `premiumIndexKlines/5m` row wins; REST
  `rest/premiumIndexKlines/5m` rows fill only the slots the archive lacks (`panel_inputs.rest_fill`). Klines 1m: REST
  `rest/klines/1m` minutes count toward the end estimate only for a symbol that has archive 1m; a symbol without
  archive 1m keeps the 5m end rule. Mark: the archive `markPriceKlines/5m` row wins; REST `rest/markPriceKlines/5m`
  fills only the missing slots, as for premium (`panel._rest_series`; days the archive never published, e.g.
  2026-10-18 for 22 symbols; panel_stats `rest_mark_rows`). Index: archive only. Funding: the monthly archive, then
  `rest/funding` for the events after the archive's last one (`panel._read_funding`); the panel's last bar never
  carries the settlement at its close (the data edge).
- `spike_window` and `mark_divergence_window` come from `quality/suspicious_windows.parquet`, the scorecard's review
  queue: the top `QUEUE_CAP` (2,000) windows by magnitude, rewritten on every scorecard run. They are review flags, not
  complete detectors, and their counts move between builds (round 4: 311 mark-divergence windows, round 5: 31).
  Research must not filter on them. The tradable rule does not use them.
- `aux/` (inside each version directory): `context_ext_daily.parquet` (`usable_from`), `tick_history.parquet`
  (`capture_ts`) and `announcements.parquet` (`release_ts`), copied unchanged. A row is usable at bar t only when its
  publish column is <= t (`panel_inputs.visible_rows`, `AUX_USABLE`). They are not per-bar columns.

## Cost-model alignment
`simulate_targets` charges `spread_bps[t+1, i]/2` for the fill at open t+1 after a decision at close t. So the
array passed as `spread_bps` must be execution-aligned: row k is predicted from bars <= k-1 only.
`quant.simulation.costmodel.predict_spread_bps(dense)` returns it already aligned that way (row 0 = the conservative cap), and
callers never shift it again. Size multipliers are keyed by a point-in-time band of `median_qv_30d` from
universe_pit (built from data up to D-1), never by a symbol list, so every instrument gets a fitted multiplier.

## Full-span scaling and smoke-run fixes (from the blinded smoke run)
Full 2020-2025 dense arrays do not fit 6 GB even for 20 names, so research streams by time chunk:
- `quant.data.panel.iter_dense(start, end, fields, chunk="year", halo_bars=8640, ids=None) -> Iterator[Dense]`: yields
  calendar-year chunks; each carries `halo_bars` real bars before its first owned bar (marked `owned=False` via a new
  `Dense.owned bool[T]`), same padded fixed shapes, the same id axis `ids` for all chunks (union of instruments
  present in [start,end) unless ids is given). Padded rows: present=False, and their ts repeat the last real ts, so
  ts never crosses `end` (fixes a smoke-run bug). `Dense.n_t` = number of real rows.
- Features/decay/models compute on each chunk and keep only owned rows. Models train on sampled decision rows
  collected across chunks (small), then predict chunk by chunk.
- `simulate_targets` / `simulate_events` gain optional state carry: `pos0` (f64[N] initial positions in units,
  default zeros), `equity0` (already exists), and return `pos_final` f64[N] (+ open events state for
  simulate_events, as an opaque dict passed back as `state0`). Running chunk by chunk with carry must equal one full
  run exactly (test).
- Capital: the retail baseline is equity0 = 100,000 USD (RESEARCH change log); sensitivity rows 10,000 and
  1,000,000. Every run reports its fill ratio (filled / target notional) and participation-breach share.
- Negative equity is never silent: the sim raises with the bar, instrument and cost components (as now), and a
  research run that raises is a bug to root-cause, never caught and skipped.
