# Data report, 2026-10-22: audit A, fixes, re-audit, ultra-premium closure

> Copied unchanged from the private research repository (snapshot 2026-10-24), except this note. Paths such as `libs/...`, `data/...`, `infra/swarm/...` and `data/samples/...` refer to that repository and its data folder, which are not published here. What this repository does contain is listed in the root README.md.

Panel version: `data/curated/versions/20261022T174452075435Z` (the `current` link). 170,259,866 five-minute bars,
696 Binance USD-M USDT-margined crypto perpetuals (delisted contracts included), 2020-01 to 2026-10-21 23:55 UTC.
The holdout (from 2025-10-01) received only mechanical checks: no returns or statistics were computed on it.

## Verdict

The data is ready for research. Scorecard round 7 (`data/curated/quality/scorecard_latest.json`): 14 green, 2 proven
unfixable from free sources, 1 waived by owner rule, 0 amber, 0 red. Full test suite: 765 passed, 0 failed.

What that means for the backtest-versus-reality gap: on a fresh random sample, the panel's bars equal the exchange's
own 5m klines on 17,280 of 17,280 bars (open, high, low, close, volume, trades), holdout and split listings included.
Funding equals the exchange on 2,574 of 2,574 settlements. Data error is therefore not a measurable source of gap on
the sampled fields. The remaining gap sits in execution: the spread model, impact and fees (see "Costs").

## What was checked (audit A, seven read-only workers)

- **Files:** 156,335 parquet files open; 431/431 archive sha256 match; schema identical across years and versions;
  0 duplicate rows; 0 grid holes.
- **Values:** the exchange matched on close, volume, trades, mark, premium, funding (1,629/1,629) and open interest
  (1,440/1,440). The universe re-derives on 10 of 10 days.
- **Defects:** audit A also found the defects listed below (F1-F12).

## What was fixed

| Fix | Problem | Result |
|---|---|---|
| F1 listing segments | A ticker Binance reused for a new coin (LIT, CVC, CTK, ICP, TLM, BNX, MAVIA, CVX, SLP, AIA, PUMP, MDT) carried the old coin's history under the new id, including years of frozen prices | one instrument per listing; the old listing ends at its last traded bar; 17.0M rows outside any listing dropped and counted |
| MDT (found by the rebuild) | MDTUSDT reused on 2026-10-21 for a TradFi perpetual (Medtronic, about $95); the catalog read a 10-19 exchange snapshot | snapshot 10-22; the crypto listing ends 2024-05-16; an earlier listing no longer inherits the new listing's contract type |
| F2 OHLC from traded minutes | 5m open/high/low used minutes without trades | equals the exchange's 5m kline 17,280/17,280 (before: 17,229) |
| F3 month boundary | index/spot close at a month's first bar was null | uses the previous month's last kline |
| F4 single-bar prints | over-flagged | mark-lag exclusion plus exemption inside 3-venue confirmed windows: 25,721 dev bars |
| stale_run | frozen zero-volume runs were tradable | >= 288 frozen bars are untradable: 2 runs remain (TLM 437, AERGO 5,783 bars) |
| F5-F6 downloads | data stopped 2026-09-30; October funding was archive-only (zeros) | extended to 2026-10-21; REST funding tail merged; a 2026-10-18 mark-price day the archive never published was filled from the REST endpoint |
| F7 delist notices | CRV notice mapped to the wrong contract; 25 multi-id notices never fired | 76 notices mapped correctly |
| F8 | GAIBUSDT never traded | excluded |
| F9 | stats and NaN | NaN -> null; tradable counts recomputed after every flag pass |
| F10 OKX | 849 OKX days marked ok without bars | re-fetched |
| F11 fees | fee change date unknown | taker 4 bps to 2023-06-21, 5 bps from 2023-06-22 (measured window) |
| F12 | one corrupt archive minute per date on 4 dates in 2023 | exchange-side; documented, values kept |

## Re-audit (wave R, seven workers)

Every audit-A finding was re-measured as before -> after. Results:
- Exchange truth is exact on a new seed.
- Schema identical; NaN 0; duplicates 0; OHLC violations 0.
- Listing bounds: 0 violations.
- The universe re-derives on 10 of 10 days.
- Single-bar prints on the dev span equal 25,721.
- Survivorship clean: 12 earlier listings enter the universe on their own dates.

Every row-level change between the audited version and this one traces back to a fix.

## Ultra-premium closure (wave U)

| Metric | Round 5 | Round 7 | How |
|---|---|---|---|
| M4 market-wide flat windows | red | green | 7 windows measured as Binance USD-M trading halts (all major perps zero trades while spot traded) and catalogued (P108-P114); 8 of 9 explained |
| M9 open-interest days | red | green 0.62% | 91% of "incomplete" days were outside any listing; scored inside listings only. 1,728 in-listing days have no archive file and the REST endpoint serves 29 days: unfixable, below threshold |
| M15/M16 mark/premium holes | amber | green 0 | REST fill of 2026-10-18; dead periods of reused tickers not scored |
| M10 mapping | amber | green | a new October listing has daily-archive data |
| M3a hard rules | red | proven | 29 AERGO minutes on 2025-04-16 exist in no free source (archive empty, REST refuses the delisted symbol); inside an untradable stale run |
| M3b soft-rule rates | amber | proven | 9,800 per 1M bars of zero-volume, stuck and spike minutes are the exchange's own record of thin trading, handled by panel bits |

Proofs: `configs/quality_proofs.json`, `docs/quality_proofs.md`. A proof holds only while the value stays at or below
its proven level and the evidence file exists, so a regression reopens the metric.

## Costs

- **Spread model:** the refit (fit3 procedure) failed the registered out-of-sample gate, as fit4 did before it. fit3
  stays, and the era multipliers were re-derived (largest change 0.003). fit3's original input file no longer exists,
  so the reproducible refit is kept staged (`data/samples/w-cost2/`). Spread quotes exist only from 2023-05; earlier
  years use the era multipliers on an effective-spread proxy, deliberately conservative.
- **Fees:** fees after 2024 are assumed from the last measured schedule; the archive and live endpoints return
  nothing to measure them.

## Known limitations (documented, not defects)

- 29 AERGO minutes (M3a) and 1,728 open-interest days (M9) do not exist in any free source.
- The panel's last bar (2026-10-21 23:55) never carries the funding settlement at its close.
- `spike_window` and `mark_divergence_window` are review-queue flags (top 2,000 windows); research must not filter on
  them.
- ELIZAOS is absent from Binance, so the AI16Z rename chain stays unlinked.
- The full scorecard needs `MemoryMax=13G`; 10G kills it.

## Where to find things

- Ledger, with every ruling: `infra/swarm/ledger.md`.
- Worker reports: `infra/swarm/reports/` (a-* audit, w-* fixes, r-* re-audit, u-* closure).
- Scorecards: `data/curated/quality/`.
- Contracts: `docs/DESIGN.md`. Pre-registration: `docs/RESEARCH.md`.
