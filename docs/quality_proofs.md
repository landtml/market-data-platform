# Quality proofs: metrics proven unfixable from free sources

> From the private research repository (snapshot 2026-10-24), edited for readers outside it: internal task references and process notes were removed and the wording was tightened. Rules, numbers and dates are unchanged. Paths such as `configs/...` and `data/...` refer to that repository and its data folder, which are not published here; the root README.md lists what this repository contains.

The quality target (set 2026-10-21): every scorecard metric is green, or proven unfixable from free sources with
evidence on disk. A proof is registered in `configs/quality_proofs.json` with the largest value it covers
(`max_value`). `quant.data.quality.apply_proofs` shows such a metric as `proven` only while its value stays at or
below `max_value` and this file exists. A regression reopens the metric.

## M3a: 29 one-minute bars missing for AERGOUSDT on 2025-04-16

Value proven: 29 missing 1m bars (hard rule `missing`), all AERGOUSDT 2025-04 (scorecard round 6,
data/curated/quality/scorecard_20261022T181757Z.json, detail.top20_symbol_months).

Evidence, measured:
- The monthly and daily archive (data.binance.vision) has no rows for AERGOUSDT 2025-04-16 00:00 to 10:59, 660 minutes
  (gap-fill report, "REST refused").
- The REST endpoint refuses the symbol: HTTP 400 `-1121 Invalid symbol` (gap-fill report, probe). The contract
  is delisted, so no REST source exists.
- 631 of the 660 minutes were filled from AERGOUSDTSETTLED, whose series is identical where both exist
  (2026-10-21, data/lake/binance_um/rest/klines/1m/AERGOUSDT/2025-04.parquet). The 29 remaining minutes are in
  neither series.
- The panel marks the affected window: AERGOUSDT:20240901 has a stale_run of 5,783 bars from 2025-03-27 to the
  2025-04-16 reopen, all untradable (panel_stats, re-audit values).

No other free source carries Binance USD-M 1m bars for a delisted contract. Effect on research: none, because the bars
are inside an untradable stale run.

## M3b: soft-rule rates in the 1m data are the exchange's own record

Value proven: 9,800 per 1M bars (zero_vol 5,954, stuck 134, spike 3,712; round 6).

These counts measure market states in the exchange's own 1m archive:
- zero_vol is a minute with no trade, stuck is 30 or more identical closes in a row, and spike is the validator's
  heuristic flag.
- The lake 1m is the archive itself: 431 of 431 archive files match their sha256 checksums (audit). Panel bars equal
  the exchange's 5m klines on 17,280 of 17,280 sampled bars (re-audit), holdout included.
- "Fixing" them would mean changing true exchange data. The panel handles them instead:
  - stale_run makes frozen runs untradable;
  - single_bar_print and the 3-venue consensus bits handle isolated prints;
  - flat_market and market_window cover exchange halts.
- The rate halved, 20,308 to 9,800, once the dead periods of reused tickers (CTK, CVC, MDT...) stopped being scored
  (listing-aware live ranges). What remains is thin-coin trading.

The amber thresholds (zero_vol 1,000, stuck 100, spike 50 per 1M) were set before the archive was audited. They
describe a liquid market. This proof replaces them as the pass criterion. A rise above 9,800 per 1M
reopens M3b.
