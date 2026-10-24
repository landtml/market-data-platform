# Data licences and terms: status per source

> From the private research repository (snapshot 2026-10-24), edited for readers outside it: internal task references and process notes were removed and the wording was tightened. Licence terms, quotes and dates are unchanged. Paths such as `data/...` refer to that repository's data folder, which is not published here; the root README.md lists what this repository contains.
>
> Status note (2026-10-24): this table was written on 2026-10-19. Since then the lake gained OKX 5m bars (`okx/bars_5m`) and Bybit funding and open interest (`bybit_deriv/`), used only to cross-check Binance prints and as panel columns. Their terms are still marked "unclear" below; no OKX, Bybit or other exchange data is included in this repository.

Last updated: 2026-10-19. Terms were read from the saved pages under data/raw/_terms/ (clause files) and
data/raw/dukascopy/_terms/. Quotes are verbatim; the full text is in each clause file.

## Scope

The dataset is for my private, non-commercial research. It is never redistributed: no publishing, no
sharing of raw or curated files, no feeds, no sale. Derived Binance data inherits CC BY-NC-SA 4.0 (Binance Vision,
terms 3.1 and 4.5), so any derived Binance output keeps that licence and attribution if it ever leaves the private
store.

Status column: `used` = in a curated file or a live pipeline; `quarantined` = moved to data/quarantine/, not used;
`not used` = sample or probe only, or the code path is not run.

## Sources

| Source | Status in project | Verdict | Conditions | Required attribution | Clause file |
|---|---|---|---|---|---|
| Binance archive (data.binance.vision) | used (data/raw/binance, instruments, data/lake/binance_um) | allowed-with-conditions | CC BY-NC-SA 4.0, non-commercial only (3.1); personal backtesting and research (4.1); no feeds, sale or commercial use (4.2, 4.3); identical licence on any redistributed derivative (4.5); posted rate limits and download protocols (7.1, 7.2) | "Data: Binance Vision (data.binance.vision), CC BY-NC-SA 4.0." | data/raw/_terms/binance_clauses.txt |
| Binance REST (fapi exchangeInfo, catalog) | used (fapi symbol snapshot, catalog.py) | unclear (binance.com ToS unreachable, HTTP 202) | keep calls within weight limits (X-MBX-USED-WEIGHT-1M); do not pull the announcement CMS JSON; the ToS still has to be read in a browser | Binance, as above | data/raw/_terms/binance_clauses.txt |
| CoinMarketCap | quarantined (data/quarantine/cmc/: 512 snapshots and mcap_rank_weekly.parquet) | forbidden-without-consent | ToS: "Use or introduce to the Service any data mining, crawling, "scraping", robot or similar automated or data gathering or extraction method ... (unless expressly authorized by CMC)". Fetcher disabled (RuntimeError). Further data only with CMC written authorisation, or its API under its own terms | none (not used) | data/raw/_terms/cmc_clauses.txt |
| Deribit (DVOL) | quarantined (data/quarantine/deribit/; DVOL columns removed from context_daily) | forbidden-without-consent | Art. 11.1(h): "conduct any systematic or automated data collection activities (including without limitation scraping, data mining, data extraction and data harvesting) on our systems". Clause 4.6 limits market data to personal use. Read from the 2021 PDF; current ToS unreachable. Written consent from Deribit is needed before any pull | none (not used) | data/raw/_terms/deribit_clauses.txt |
| Coinbase Exchange (candles, trades) | quarantined (data/quarantine/coinbase/; 9 files from data/samples/s-xvenue) | forbidden-without-consent for automated recording; AI/ML use forbidden | CDP ToS: "strictly prohibited from recording data or content accessed via the CDP Tools through the use of any automated programs ... or any other method of screen scraping". Market Data Terms bar developing, training or improving any AI or ML model with Market Data. Never train models on Coinbase data. Note: data/samples/s-xvenue/btc_20240305_venues.csv still has a coinbase_btcusd column (not moved; decision open) | none (not used) | data/raw/_terms/coinbase_clauses.txt |
| Bybit (archive and REST) | not used (sample probes in data/samples/s-xvenue only) | unclear (ToS and API terms return HTTP 403 everywhere) | no bulk pulls until the ToS has been read and saved | none (not used) | data/raw/_terms/bybit_clauses.txt |
| OKX (REST and trades archive) | not used (sample pages in data/samples/s-xvenue only) | unclear (API agreement 9.4 limits market data to "your own personal, non-commercial trading and account management purposes"; scraping "at a rate or scale that" exceeds personal trading need) | no bulk pull; small rate-limited test only after the terms are accepted; ask OKX in writing whether research counts | none (not used) | data/raw/_terms/okx_clauses.txt |
| Kraken (public trades and OHLC) | not used (sample probes only) | allowed-with-conditions | personal use of public endpoint data; non-personal commercial use needs Kraken's prior permission; about 1 call per second per IP; main ToS (404) not read | none stated in the read text; cite Kraken if displayed | data/raw/_terms/kraken_clauses.txt |
| Tardis.dev (free datasets) | not used (day probes returned HTTP 403; spread code plans it) | allowed-with-conditions | personal, research or internal use by the customer only (Permitted Use); no redistribution or dataset, feed or backtesting product for others (9.2); derived output only as charts, papers, or candles of 10 min or longer (9.3); own model training allowed, not sold (9.5); Coinbase-sourced data barred from AI/ML use (cl. 23) | "Data: Tardis.dev" (recommended; not required by the read clauses) | data/raw/_terms/tardis_clauses.txt |
| Coin Metrics (community API) | used (context_daily: AdrActCnt, SplyCur, FlowInExUSD, FlowOutExUSD for BTC and ETH) | unclear | community data is CC BY-NC 4.0 (no key, 10 requests per 6 s); owner Talos's site ToS 7(e) bars robots and crawlers on "the Services"; api terms page redirects to marketing. Stay under 10 requests per 6 s; non-commercial only | "Coin Metrics Community Data, CC BY-NC 4.0" | data/raw/_terms/coinmetrics_clauses.txt |
| DefiLlama (stablecoin charts) | used (context_daily: stable_usdt, stable_usdc, stable_total) | allowed-with-conditions | personal non-commercial licence (7); only the official public API may be called programmatically (8); no site scraping or robots; no republishing or resale; liquidated damages up to USD 100,000 per breach (14) | none stated; cite DefiLlama if shown | data/raw/_terms/defillama_clauses.txt |
| alternative.me (Fear and Greed) | used (context_daily: fng_value) | allowed-with-conditions | commercial use allowed with attribution; no impersonation or confusingly similar service | "Source: alternative.me Crypto Fear and Greed Index", next to every display of the data | data/raw/_terms/altme_clauses.txt |
| FRED / ALFRED (St. Louis Fed) | used (context_daily: 10 FRED series incl. WALCL); ALFRED used for spot checks only (not in the build) | allowed-with-conditions | personal, non-commercial use; cite FRED and keep copyright notices; use the API, not scraping. Copyrighted series (notes contain "Copyright") are for personal use only; anything else needs the data owner's permission. Copyrighted series in use: SP500, NASDAQCOM, VIXCLS, BAMLH0A0HYM2. The FRED API needs a registered key (conflicts with the keyless rule; decision open) | "Source: Federal Reserve Bank of St. Louis, FRED; accessed via FRED", plus each series' copyright notice as printed | data/raw/_terms/fred_clauses.txt |
| Dukascopy (historical tick and bar download) | not used (terms captured under data/raw/dukascopy/_terms; no data pulled) | forbidden-without-consent for automated access (as quoted) | "You shall not use or attempt to use any "scraper," "robot," "bot," "spider," "data mining" ... without the prior express written consent of DUKASCOPY." Downloads only for personal, non-commercial use; no recirculation or redistribution | none (not used) | data/raw/dukascopy/_terms/quoted_clauses.txt |

## Open questions (as of 2026-10-19)

1. Binance API ToS: binance.com ToS and API terms return HTTP 202 with an empty body, so they have to be read in a browser; until then, REST calls stay few and within the weight limits.
2. Bybit ToS: every URL returns HTTP 403. The page has to be saved from a browser into data/raw/_terms/.
3. OKX API agreement 9.4: does "market data consumption" (3.3) cover research, or only personal trading? This needs a written answer from OKX if it matters.
4. Coin Metrics vs Talos ToS: does Talos's robot clause (7(e)) reach api.coinmetrics.io, or only the Talos site? The CC BY-NC 4.0 community grant is in place now.
5. Quarantined data: data/quarantine/cmc/, data/quarantine/deribit/ and data/quarantine/coinbase/ are kept; nothing has been deleted.
6. Coinbase in the venue sample: data/samples/s-xvenue/btc_20240305_venues.csv still carries coinbase_btcusd. That column or the file is to be removed (only the Coinbase files were moved).
7. FRED API key: whether to register one (the keyless rule versus the API ToS).
