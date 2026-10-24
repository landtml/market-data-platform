# Pre-registration: retail intraday ML test on Binance USD-M perpetuals

> From the private research repository (snapshot 2026-10-24), edited for readers outside it: internal task references and process notes were removed and the wording was tightened. Rules, numbers and dates are unchanged. Paths such as `configs/...` and `data/...` refer to that repository and its data folder, which are not published here; the root README.md lists what this repository contains.

Written 2026-10-19, before any research data was loaded. Changes after this point are logged at the bottom with the
reason, so the number of decisions made after seeing results stays visible.

## Question

After realistic retail costs, is there a cross-sectional or time-series signal on Binance USD-M perpetuals that
survives purged combinatorial cross-validation, deflation for every configuration tried, and an untouched holdout?
And at which holding period does net edge per unit time peak?

"No edge survives" is a valid and reportable result.

## Data (from the curated dataset; see DESIGN.md)

- Instruments: every Binance USD-M crypto perpetual in the archive, including delisted ones. Excluded: TradFi perps,
  index contracts, stablecoin-vs-stablecoin pairs, quarterly contracts.
- Universe on day D, computed only from data up to the end of D-1:
  listed for at least 30 days, and trailing 30-day median daily quote volume at least $20M, and among the top 100 by
  that measure. Delisted instruments stay in until settlement, as on the exchange.
- Base bars: 5 minutes, built from 1-minute klines. Funding at the recorded funding timestamps. Mark and index
  prices for bad-print checks. Open interest and ratios from the archive `metrics` (from 2020-09).
- Context: context_daily.parquet (already lagged to its release time).

## Periods

- Development: 2020-01-01 to 2025-09-30. All selection happens here, inside CPCV.
- Holdout: 2025-10-01 to the last complete day. It is used once, for at most 3 configurations declared in advance in
  the change log below. Nothing is ranked on it.

## Stage 1: the alpha-decay map (JAX, GPU)

For each feature family, compute the daily cross-sectional Spearman IC against the forward residual return (net of the
equal-weight universe return) at horizons h in {15m, 1h, 4h, 12h, 1d, 3d}. Report the IC with block-bootstrap CIs
per horizon, the turnover implied at each horizon, and the break-even one-way cost in bps (IC x cross-sectional
dispersion x sizing / turnover). This is the "estimate alpha(h) and pick h" step. The horizon is a result here, not
a knob.

Feature families: short-horizon reversal and momentum (returns over 5m to 30d); taker-flow imbalance; funding rate
level and change; basis (premium index); open-interest change; realised volatility and its change; distance from
rolling VWAP; residual return after BTC beta; Amihud illiquidity; time of day / day of week; context conditioners
(DVOL, Fear & Greed, stablecoin supply change, rates). All causal, computed on bar close, used from the next bar.

## Stage 2: the model

- Target: the forward residual return at the horizons Stage 1 shows above break-even. If none is above break-even,
  Stage 2 still runs on the best two horizons to measure how far short they fall.
- Models: (a) ridge on ranked features; (b) LightGBM; (c) a small JAX MLP trained with vmap across CPCV folds. Each
  model x horizon x feature-set is one trial in the TrialRegistry.
- Portfolio: dollar-neutral long-short on the top and bottom quintile of the prediction, volatility-scaled, gross
  exposure 1.0, rebalanced at the horizon, phase-averaged over rebalance offsets (V10).
- Execution: Rust simulate_targets with full costs.
- Alternative design (separate trials): time-series events from the CUSUM filter, triple-barrier labels with
  volatility-scaled barriers, a meta-label model, executed with simulate_events (barriers fill at the next open).

## Costs (point-in-time)

- Fees: Binance VIP0 USD-M at each date (taker 5 bps per side as baseline; the history is in costs.parquet).
- Spread: a model calibrated on real quotes (archive bookTicker 2023-05 to 2024-04; checked out of period on free
  tick samples) as a function of high-low range, volume and volatility.
- Impact: k * sigma * sqrt(participation), with participation capped at 5% of bar dollar volume.
- Funding: actual rates at the actual times.
- Baseline is the taker scenario. Maker execution is reported only as a sensitivity, never used for selection.

## Overfitting controls

- CPCV: 10 groups, 2 test groups (45 splits, 9 paths), purge by each label's end time t1, embargo of 1 day.
- Every configuration evaluated goes into the TrialRegistry of an external research library (not included). DSR uses the effective number of trials from ONC
  clustering of the trial return series. PBO by CSCV over the trial matrix.
- A pure-noise run with the same pipeline and trial count first (V11): the best noise trial's DSR must look uniform.
- Controls: a gross-matched cash control (V7) and equal-weight buy-and-hold of the universe.
- Plateau rule: a configuration counts only if its neighbours in horizon (one step either side) also have positive
  net Sharpe.

## Success criteria (fixed now)

A configuration is called an edge only if all of these hold:
1. CPCV mean net Sharpe > 0 and DSR >= 0.95 at the effective trial count;
2. PBO < 0.3 across the trial matrix;
3. plateau rule holds;
4. holdout net Sharpe > 0 after costs, and the holdout PSR against zero is at least 0.8.

Everything else is reported as "no edge at retail cost", with the gap in bps per trade.

## Change log

- 2026-10-19 21:58 UTC (before any research data was loaded; clarification, not a result-driven change):
  BUSD-, USDC- and other non-USDT-margined perps are duplicates of the USDT perp on the same base, so they stay
  out of the universe to avoid counting one asset twice. Exception: a base that never had a USDT-margined perp keeps
  its non-USDT contract, so collapsed coins listed only as BUSD perps are not lost (survivorship). Funding events at
  or after an instrument's end (settle or last traded bar) are not charged: no position can exist then.
- 2026-10-20 23:15 UTC (before any performance number was seen; the smoke run was blinded): account size fixed at
  equity0 = 100,000 USD as the retail baseline, with 10,000 and 1,000,000 as sensitivity rows. Reason: the 5%
  participation cap makes invested gross depend on account size, so the size has to be declared, not tuned.
- 2026-10-20 12:15 UTC (before any research result was seen; the decay dry run is sealed): trial budget fixed at
  K_max = 100 configurations across Stage 2 and the event design together, all registered in the TrialRegistry,
  failed and abandoned ones included. Expected use: 3 models x up to 6 horizons x 3 feature sets (all families; price
  and flow only; without context conditioners) = 54, plus event-design trials. Reason: the V11 noise check and the
  DSR deflation must be run at the trial count actually used, so it has to be declared before Stage 2, not counted
  after.
- 2026-10-20 12:55 UTC (before any performance number was seen; smoke runs are sealed): cost model = pooled fit
  over all spread observations with a per-band multiplier m_b >= 1 so the charged spread covers the actual one at
  least 65% of the time in both out-of-sample directions (the reverse direction under-covered at 0.35-0.57). Spread
  floor stays at 0.5 bps. Every reported configuration also gets cost sensitivity rows at 1.5x and 2x the spread
  and impact components (fees and funding unchanged). Selection uses the 1x baseline only.
- 2026-10-20 13:05 UTC (before any research result was seen; method fix found by the V11 noise run): the effective
  trial count merges two trials only when their return correlation is both significant (standard error corrected
  for the heteroscedastic per-bar variance) and |r| >= 0.3. Reason: at T = 604,800 bars a significance-only gate
  merged independent noise trials at |r| < 0.007, which shrank N_eff and inflated the noise DSR (median 0.63 at K=6).
  The report also gives N_eff at |r| floors of 0.2 and 0.5, and DSR at the raw trial count as a conservative bound.
- 2026-10-20 13:15 UTC (before any research result was seen): bad-print robustness. The cross-venue check cannot
  verify all 417k unverified suspicious windows (~300 h at Kraken's public rate), so (a) a random sample stratified
  by year estimates the population bad-print rate with a CI, and (b) every reported configuration is rerun with all
  unverified suspicious windows on eligible names masked (non-tradable, and features from those bars set to NaN).
  A configuration whose CPCV net Sharpe falls by more than half under that mask is reported as bad-print dependent,
  not as an edge. Baseline masking stays: only refuted windows are non-tradable.
- 2026-10-20 13:25 UTC (before any research result was seen; found by the event-rate data diagnostics): triple-barrier
  scaling. Profit-take and stop-loss are k * sigma_bar * sqrt(h_bars), where sigma_bar is the causal EWMA per-bar
  volatility and h_bars the vertical-barrier horizon (from the Stage 1 horizons), with k = 1 as baseline and k = 2 as
  one variant within the trial budget. Reason: barriers at one bar's sigma resolved almost every label within ~3 bars
  (vertical exits ~6e-6), which makes the label a few-bar noise outcome dominated by costs.
- 2026-10-20 13:35 UTC (before any research result was seen; from the universe breadth check): minimum breadth. The
  cross-sectional design takes positions only on days with at least 30 eligible instruments (at least 6 names per
  quintile leg); on other days it holds cash. Measured on universe_pit: 2020 median 10 eligible (min 2), 30 first
  reached 2020-09-11 with 76 later days below it; from 2021-01-24 never below 50. Data before that still feeds
  feature warm-up, Stage 1 ICs (reported with their daily breadth) and the time-series event design.
- 2026-10-20 14:00 UTC (before any research result was seen; found by the event-rate data diagnostics): CUSUM
  threshold. The symmetric CUSUM filter for horizon h uses threshold sigma_bar * sqrt(h_bars) (the same scale as the
  barriers), floored at 3 price ticks divided by price. Reason: a per-bar-sigma threshold fired a median 72 events per
  instrument-day (one every 4 bars; average label uniqueness 0.017 at h = 1 day), so the event design was dense
  sampling, and low-priced names (NIL, SOLV, USTC, XEM, XPL) triggered on tick bounces.
- 2026-10-20 14:15 UTC (before any research result was seen; from the V11 noise runs): calibration note. At the real
  shapes, pure-noise best-of-K DSR has median ~0.51 and 0% of seeds above 0.95 (K = 6 and 54), so the DSR >= 0.95
  gate is conservative (false-positive rate below nominal); power at a planted net Sharpe of 2/yr is ~46-48%. The gate
  stays as registered. In addition, every reported configuration gets an empirical p-value against the V11 noise
  distribution at the same K and shapes (share of noise seeds whose best-of-K DSR is at least the configuration's).
- 2026-10-21 10:50 UTC (before any Stage 1 or Stage 2 result was seen; my decision, for a retail account): rebalancing
  cadence variants. Besides "rebalanced at the horizon", the cross-sectional portfolio is also run at a fixed cadence
  c in {1h, 4h, 1d} for every c >= h, holding the mean of the predictions made since the last rebalance (no new
  forecasts, no refit). Scope: the all-families feature set and, per model, the two horizons that Stage 2 already
  runs; each (model, horizon, cadence) pair is one registered trial (at most 3 models x 2 horizons x 2 extra cadences
  = 12), inside K_max = 100. The plateau rule also requires the neighbouring cadence (one step either side, where it
  exists) to have positive net Sharpe. Reason: at retail cost, turnover often decides the result, and a slower cadence
  is the cheapest way to cut it; declaring the cadences now stops them from becoming a tuned knob later.
- 2026-10-21 11:10 UTC (before any Stage 1 or Stage 2 result was seen; from the all-years spread check): era
  multipliers. Quoted spreads exist only from 2023-05, and on the aggTrades effective-spread proxy (2020-2025-09,
  doubled to full-spread units; calibrated on 2023-06 and 2024-02, Spearman 0.83-0.89 in the bands below 500M USD)
  the fit3 model covers the actual spread in only 35% / 55% / 61% of 5m buckets in 2020 / 2021 / 2022, below the
  registered 65%. So the charged spread is fit3 x m_b x e_{year,b}, with e from the same 65% rule in both directions
  of an odd/even-month split (era_mult.json): 50m-500m 1.63 / 1.35 / 1.19 / 1.08 for
  2020-2023, 10m-50m 2.20 / 1.62 / 1.07 / 1.29, lt10m 1.18 in 2023; e = 1 for 2024-2025 and for the >= 500M band.
  A band with no proxy rows in a year (lt10m 2020-2022, 2025) takes the largest e of that year. fit3 itself is
  unchanged (refits fit4 and fit-ay failed the out-of-sample gate). The proxy reads harsher than quoted in 2023 (fit3
  coverage 0.61 vs 0.70), so the error is on the conservative side. Reason: costs must meet the registered coverage
  in every year the research uses, not only in the years the quotes cover.
- 2026-10-22 18:30 UTC (before any Stage 1 or Stage 2 result was seen; from the data audit): data version and cost
  refit. The research runs on the panel version published after the audit's fixes (listing segments for reused
  tickers, OHLC from traded minutes, index month boundary, single-bar print with mark-lag exclusion and confirmed-window
  exemption, stale_run, delist-notice mapping); its version stamp is recorded with every trial. fit3's input file no
  longer exists, so the spread model is refitted with the fit3 procedure on the current spread_obs_5m.parquet and the
  rebuilt panel, must pass the same out-of-sample gate that promoted fit3, and the era multipliers are re-derived with
  the registered 65% rule (same odd/even split). If the refit fails the gate, fit3's parameters are kept with the
  re-derived era multipliers and the failure is reported. Fees: taker 4 bps to 2023-06-21 (measured), 5 bps from
  2023-06-22 (the earliest date the evidence allows; measured by 2024-02-20). Reason: every cost and data input must be
  rebuildable from files on disk.
- 2026-10-22 20:40 UTC (before any Stage 1 or Stage 2 result was seen): outcome of the entry above. Data version
  20261022T174452075435Z (after the audit, the re-audit and the closure of the remaining scorecard metrics). Spread
  refit (fit3 procedure, current spread_obs_5m.parquet, input sha256 recorded in refit_inputs.sha256): gate part (a), coverage after scaling
  >= 0.65 in every band and both directions, PASS (min 0.650005); part (b), overall out-of-sample median log error no
  worse than fit3, FAIL (forward 0.4291 -> 0.4603, reverse 0.3790 -> 0.3821; same cause as fit4: ETH and SOL 2024-02
  joined the evaluation set). As registered, fit3's parameters stay (data/curated/cost_model.json unchanged). Era
  multipliers were re-derived with the 65% rule on the new panel; the largest change is 0.003
  (configs/cost_era_mult.json; the previous table is kept as cost_era_mult_prev.json). The refit stays staged as
  cost_model_refit.json, the model rebuildable from files on disk; fit3 itself is not.
