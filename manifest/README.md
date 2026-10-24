# manifest/

Machine-generated descriptions of the private data, built by `tools/build_manifest.py` from parquet file footers
only (row counts, schemas, row-group min/max statistics). No data page was read and no market data is stored here.
Generated 2026-10-24 with pyarrow 26.0.0.

| File | Scope | What it pins |
|---|---|---|
| `manifest.json` | the lake (`data/lake`): normalised per-venue parquet | per partition: files, rows, bytes, symbol count, schema hashes, time range, sha256 of the sorted (path, size) listing; whole-lake listing sha256 `58a72a8c...` |
| `manifest_files.tsv.gz` | the lake | one line per file: relative path, bytes, rows (154,690 lines) |
| `curated_panel_manifest.json` | the published research panel version `20261022T174452075435Z` | the 5-minute panel (170,259,866 rows, 7 yearly files), universe and auxiliary tables |
| `data_folder_manifest.json` | the whole private `data/` folder, sealed research results excluded | used only to reconcile the headline figure (see below) |

## How it was produced

```bash
python tools/build_manifest.py <private>/data/lake --out manifest/manifest.json
python tools/build_manifest.py <private>/data/curated/versions/20261022T174452075435Z \
    --out manifest/curated_panel_manifest.json --no-listing
python tools/build_manifest.py <private>/data --exclude research/sealed \
    --out manifest/data_folder_manifest.json --no-listing
```

The lake run reads 154,690 footers in about 15 seconds on 8 cores. `python tools/verify.py <lake>` recomputes it
and reports any difference; the three runs above were verified against their files right after writing them.

## Partitions and categories

A partition is a dataset folder, for example `binance_um/klines/1m`, whose files are
`<SYMBOL>/<YYYY-MM>.parquet`. The rule is in the docstring of `tools/build_manifest.py`. Each partition gets a
category:

- `market_data`: per-symbol series (klines, funding, mark, premium and index prices, open interest, OKX 5m bars).
- `table`: single tables (consolidated Bybit funding and open interest, symbol maps, cross-venue verdict tables).
- `auxiliary`: intermediate part files, issue logs and verification bookkeeping. Some of these duplicate rows held
  in a `table` (for example `bybit_deriv/_parts` and `bybit_deriv/funding`), so they are kept out of "primary".

`primary` = `market_data` + `table`. Time ranges come from the `ts` column (int64 ns, UTC; bar open time) or, for
verification tables, `start_ts`. Symbol counts are distinct per-symbol folders or files, so a ticker listed twice by
the exchange counts once here.

## True totals and the headline figure

| Scope | Files | Rows |
|---|---:|---:|
| Lake, all parquet | 154,690 | 2,157,688,483 |
| Lake, primary (no intermediate copies) | 150,689 | 2,139,150,678 |
| Lake, per-symbol market data | 150,678 | 2,122,404,687 |
| Curated panel version (bars + universe + aux) | 11 | 173,628,168 |
| Whole `data/` folder, sealed results excluded | 160,770 | 2,537,964,196 |

The figure "about 2.5 billion rows in 160,000+ files" matches the whole `data/` folder. That count includes the previous
version of the curated panel, kept for rollback (173.6M rows, same row count as the current one), about 3,300 scratch files of the build workers
(26.9M rows) and the intermediate copies above. The defensible figure for the data lake is **2.16 billion rows in
154,690 parquet files**, of which 2.12 billion rows are per-symbol market data.
