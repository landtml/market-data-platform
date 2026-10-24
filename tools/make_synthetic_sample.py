#!/usr/bin/env python3
"""Write a small synthetic lake with the real lake's layout and column schemas, plus planted defects.

    python tools/make_synthetic_sample.py --out sample/lake

No exchange data is used or redistributed (docs/DATA_LICENSES.md): prices are a seeded random walk for three made-up
tickers (SYNTHAUSDT, SYNTHBUSDT, SYNTHCUSDT). Each monthly file holds the first two days of its month so the sample
stays under 2 MB. The column names and types match the real partitions exactly; tools/verify.py --sample checks that
against manifest/manifest.json.

Planted defects (only in SYNTHCUSDT 1m, 2024-02), written to sample/EXPECTED_DEFECTS.json so the quality scan in
tools/verify.py must find exactly these and nothing else:
  * one duplicated minute, one 7-minute gap, one bar with high below close, one stuck run of 40 identical closes.

Deterministic for a given seed and pyarrow version. Needs numpy and pyarrow.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

MIN_NS = 60_000_000_000
SYMBOLS = {"SYNTHAUSDT": 100.0, "SYNTHBUSDT": 2.5, "SYNTHCUSDT": 0.04}
MONTHS = {"2024-01": 1704067200, "2024-02": 1706745600}  # UTC epoch seconds of the month start
DAYS = 2

KLINE_1M = pa.schema([("ts", pa.int64()), ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()),
                      ("close", pa.float64()), ("volume", pa.float64()), ("close_ts", pa.int64()),
                      ("quote_volume", pa.float64()), ("count", pa.int64()), ("taker_buy_volume", pa.float64()),
                      ("taker_buy_quote_volume", pa.float64()), ("flags", pa.uint32())])
KLINE_5M = pa.schema([("ts", pa.int64()), ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()),
                      ("close", pa.float64()), ("volume", pa.float64()), ("quote_volume", pa.float64()),
                      ("count", pa.int64()), ("taker_buy_volume", pa.float64()), ("taker_buy_quote_volume", pa.float64()),
                      ("n_src", pa.int64()), ("complete", pa.bool_()), ("filled", pa.bool_())])
FUNDING = pa.schema([("ts", pa.int64()), ("funding_interval_hours", pa.int64()), ("last_funding_rate", pa.float64()),
                     ("flags", pa.uint32())])


def one_minute(rng: np.random.Generator, p0: float, start_s: int, n: int) -> dict[str, np.ndarray]:
    ts = (np.int64(start_s) * 1_000_000_000) + np.arange(n, dtype=np.int64) * MIN_NS
    r = rng.normal(0.0, 8e-4, n)
    close = p0 * np.exp(np.cumsum(r))
    open_ = np.concatenate([[p0], close[:-1]])
    wig = np.abs(rng.normal(0.0, 4e-4, n))
    high = np.maximum(open_, close) * (1 + wig)
    low = np.minimum(open_, close) * (1 - wig)
    count = rng.poisson(40, n).astype(np.int64)
    count[rng.random(n) < 0.02] = 0  # thin minutes with no trade: placeholder bar at the previous close
    idle = count == 0
    open_[idle] = high[idle] = low[idle] = close[idle] = np.concatenate([[p0], close[:-1]])[idle]
    volume = np.where(idle, 0.0, rng.gamma(2.0, 50.0 / p0 * 100, n))
    quote = volume * close
    tb = volume * rng.uniform(0.3, 0.7, n)
    return {"ts": ts, "open": open_, "high": high, "low": low, "close": close, "volume": volume,
            "close_ts": ts + MIN_NS - 1_000_000, "quote_volume": quote, "count": count, "taker_buy_volume": tb,
            "taker_buy_quote_volume": tb * close, "flags": np.zeros(n, dtype=np.uint32)}


def plant_defects(c: dict[str, np.ndarray]) -> tuple[dict[str, np.ndarray], dict]:
    """Duplicate minute 100, drop minutes 500-506 (gap of 7), high below close at row 900, 40 stuck closes from 1500."""
    n = len(c["ts"])
    for i in range(1500, 1540):
        for k in ("open", "high", "low", "close"):
            c[k][i] = c["close"][1499]
    c["high"][900] = c["close"][900] * 0.999
    keep = np.ones(n, dtype=bool)
    keep[500:507] = False
    idx = np.flatnonzero(keep)
    idx = np.insert(idx, np.searchsorted(idx, 100) + 1, 100)  # minute 100 appears twice
    out = {k: v[idx] for k, v in c.items()}
    gap_at = int(c["ts"][499])
    expected = {"dupe": [int(c["ts"][100])], "gap": [{"after_ts": gap_at, "missing_minutes": 7}],
                "ohlc_bad": [int(c["ts"][900])], "stuck": [{"start_ts": int(c["ts"][1499]), "run": 41}]}
    return out, expected


def resample_5m(c: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """5m bars from 1m, open/high/low/close from traded minutes only (the rule the real pipeline uses)."""
    ts5 = c["ts"] - c["ts"] % (5 * MIN_NS)
    buckets, start = np.unique(ts5, return_index=True)
    end = np.append(start[1:], len(ts5))
    rows = {k: [] for k in KLINE_5M.names}
    for b, s, e in zip(buckets, start, end):
        traded = c["count"][s:e] > 0
        src = np.arange(s, e)[traded] if traded.any() else np.arange(s, e)
        rows["ts"].append(int(b))
        rows["open"].append(c["open"][src[0]])
        rows["high"].append(c["high"][src].max())
        rows["low"].append(c["low"][src].min())
        rows["close"].append(c["close"][src[-1]])
        for k in ("volume", "quote_volume", "taker_buy_volume", "taker_buy_quote_volume"):
            rows[k].append(c[k][s:e].sum())
        rows["count"].append(int(c["count"][s:e].sum()))
        rows["n_src"].append(int(e - s))
        rows["complete"].append(bool(e - s == 5))
        rows["filled"].append(False)
    return {k: np.asarray(v) for k, v in rows.items()}


def write(path: Path, cols: dict, schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({f.name: pa.array(cols[f.name], type=f.type) for f in schema}, schema=schema)
    pq.write_table(table, path, compression="zstd", write_statistics=True)


def make(out: Path, seed: int = 20261024) -> dict:
    rng = np.random.default_rng(seed)
    expected: dict = {}
    for sym, p0 in SYMBOLS.items():
        funding_ts, funding_rate = [], []
        for month, start_s in MONTHS.items():
            c = one_minute(rng, p0, start_s, DAYS * 1440)
            p0 = float(c["close"][-1])
            if sym == "SYNTHCUSDT" and month == "2024-02":
                c, expected[f"binance_um/klines/1m/{sym}/{month}.parquet"] = plant_defects(c)
            write(out / f"binance_um/klines/1m/{sym}/{month}.parquet", c, KLINE_1M)
            if not (sym == "SYNTHCUSDT" and month == "2024-02"):
                write(out / f"binance_um/klines/5m/{sym}/{month}.parquet", resample_5m(c), KLINE_5M)
            for h in range(0, DAYS * 24, 8):
                funding_ts.append((start_s + h * 3600) * 1_000_000_000)
                funding_rate.append(round(float(rng.normal(1e-4, 5e-5)), 8))
        n = len(funding_ts)
        write(out / f"binance_um/funding/{sym}.parquet",
              {"ts": np.asarray(funding_ts, dtype=np.int64), "funding_interval_hours": np.full(n, 8, dtype=np.int64),
               "last_funding_rate": np.asarray(funding_rate), "flags": np.zeros(n, dtype=np.uint32)}, FUNDING)
    return expected


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=Path("sample/lake"))
    ap.add_argument("--seed", type=int, default=20261024)
    a = ap.parse_args(argv)
    expected = make(a.out, a.seed)
    (a.out.parent / "EXPECTED_DEFECTS.json").write_text(json.dumps(expected, indent=1) + "\n")
    print(f"wrote {sum(1 for _ in a.out.rglob('*.parquet'))} files under {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
