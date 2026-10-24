"""Context features: look-ahead, shuffle (future raw changes leave the past unchanged) and schema tests.

Synthetic tests run from tmp files and always run. Real-data tests read data/raw/context and
data/curated/context_daily.parquet and skip when those are absent.
"""

from __future__ import annotations

import csv
import datetime as dt
import glob
import json
import random
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant.core import paths
from quant.data.sources import context as ctx

RAW = paths.RAW / "context"
CURATED = paths.CURATED / "context_daily.parquet"
CARD = paths.CURATED / "context_sources.json"
REAL = RAW.is_dir() and CURATED.exists() and CARD.exists()
needs_real = pytest.mark.skipif(not REAL, reason="real context raw/curated not built yet")

FEATURE = {f.column: f for f in ctx.FEATURES}


# ------------------------------------------------------------- independent raw readers (no context.py loaders)

def _raw_fred(sid: str, root: Path = RAW) -> dict[dt.date, float]:
    out = {}
    with (root / "fred" / f"{sid}.csv").open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            v = r[sid]
            if v not in ("", "."):
                out[dt.date.fromisoformat(r["observation_date"])] = float(v)
    return out


def _raw_cm(asset: str, metric: str, root: Path = RAW) -> dict[dt.date, float]:
    out = {}
    for p in sorted(glob.glob(str(root / "coinmetrics" / f"{asset}_page*.json"))):
        for r in json.loads(Path(p).read_text())["data"]:
            if r.get(metric) is not None:
                out[dt.date.fromisoformat(r["time"][:10])] = float(r[metric])
    return out


def _raw_for(column: str, root: Path = RAW) -> dict[dt.date, float]:
    if column.startswith("fred_"):
        return _raw_fred(column[len("fred_"):].upper(), root)
    if column.startswith("cm_"):
        _, asset, metric = column.split("_", 2)
        names = {"adractcnt": "AdrActCnt", "splycur": "SplyCur", "flowinexusd": "FlowInExUSD",
                 "flowoutexusd": "FlowOutExUSD"}
        return _raw_cm(asset, names[metric], root)
    raise KeyError(column)


def _avail(x: dt.date, lag: int, unit: str) -> dt.date:
    """First UTC day on which an observation dated x is public (calendar days, or Mon-Fri business days)."""
    off = pd.offsets.BDay(lag) if unit == "bday" else pd.Timedelta(days=lag)
    return (pd.Timestamp(x) + off).date()


def _brute_expect(obs: dict[dt.date, float], day: dt.date, lag: int, unit: str = "day") -> tuple[float, float]:
    """Newest observation X with X + lag (in unit) <= day, by linear search. Returns (value, age) or (nan, nan)."""
    best = None
    for x in obs:
        if _avail(x, lag, unit) <= day and (best is None or x > best):
            best = x
    if best is None or (day - best).days > ctx.MAX_AGE_DAYS:
        return float("nan"), float("nan")
    return obs[best], float((day - best).days)


def _curated() -> pd.DataFrame:
    df = pq.read_table(CURATED).to_pandas()
    df.index = pd.DatetimeIndex(pd.to_datetime(df.pop("date")))
    return df


# ------------------------------------------------------------- synthetic raw fixtures

def _write_synthetic(raw: Path, rng: random.Random, *, perturb_after: dt.date | None = None) -> None:
    """Small deterministic raw set: FRED DGS2 (daily), WALCL (weekly), CM BTC flows, FNG, DefiLlama USDT.
    perturb_after: if set, every observation dated after it gets a different value (the 'future changed' case)."""
    start = dt.date(2019, 1, 1)
    days = [start + dt.timedelta(days=i) for i in range(900)]

    def v(day: dt.date, base: float) -> float:
        r = random.Random(f"{day}-{base}-{perturb_after if perturb_after and day > perturb_after else 0}")
        return round(base + r.random(), 4)

    (raw / "fred").mkdir(parents=True, exist_ok=True)
    with (raw / "fred" / "DGS2.csv").open("w", encoding="utf-8") as fh:
        fh.write("observation_date,DGS2\n")
        for d in days:
            if d.weekday() < 5:
                val = "." if d.day == 1 else f"{v(d, 2.0):.2f}"
                fh.write(f"{d.isoformat()},{val}\n")
    with (raw / "fred" / "WALCL.csv").open("w", encoding="utf-8") as fh:
        fh.write("observation_date,WALCL\n")
        for d in days:
            if d.weekday() == 2:
                fh.write(f"{d.isoformat()},{int(v(d, 7_000_000) * 100)}\n")
    (raw / "coinmetrics").mkdir(parents=True, exist_ok=True)
    rows = [{"time": f"{d.isoformat()}T00:00:00.000000000Z", "FlowInExUSD": f"{v(d, 1e9):.2f}"}
            for d in days if d.day % 5 != 0]
    (raw / "coinmetrics" / "btc_page0000.json").write_text(json.dumps({"data": rows}))
    (raw / "alternative_me").mkdir(parents=True, exist_ok=True)
    fng = [{"value": str(int(v(d, 40))), "timestamp": str(int(dt.datetime(d.year, d.month, d.day,
                                                                           tzinfo=dt.timezone.utc).timestamp()))}
           for d in days]
    (raw / "alternative_me" / "fng.json").write_text(json.dumps({"data": fng}))


def _synthetic_frame(raw: Path) -> pd.DataFrame:
    frame, _ = ctx.build_frame(raw, start=pd.Timestamp("2019-01-01"), end=dt.date(2021, 9, 30))
    return frame


# ------------------------------------------------------------- (a) look-ahead

def test_lookahead_synthetic(tmp_path):
    raw = tmp_path / "raw"
    _write_synthetic(raw, random.Random(1))
    frame = _synthetic_frame(raw)
    picks = {"fred_dgs2": 1, "fred_walcl": 2, "cm_btc_flowinexusd": 2}
    checked = 0
    for col, lag in picks.items():
        unit = FEATURE[col].lag_unit
        obs = _raw_for(col, raw)
        for d in frame.index[::7][:120]:
            day = d.date()
            want_v, want_age = _brute_expect(obs, day, lag, unit)
            got_v, got_age = frame.at[d, col], frame.at[d, f"{col}__age_days"]
            assert (np.isnan(want_v) and np.isnan(got_v)) or got_v == pytest.approx(want_v), (col, day)
            assert (np.isnan(want_age) and np.isnan(got_age)) or got_age == want_age, (col, day)
            checked += 1
    assert checked > 300


@needs_real
def test_lookahead_real_three_columns():
    """DGS2 (lag 1 business day), WALCL (lag 2 days, weekly), BTC exchange inflow (Coin Metrics, lag 2 days)."""
    cur = _curated()
    rng = random.Random(7)
    lags = {"fred_dgs2": (1, "bday"), "fred_walcl": (2, "day"), "cm_btc_flowinexusd": (2, "day")}
    assert all((FEATURE[c].lag_days, FEATURE[c].lag_unit) == v for c, v in lags.items())
    have = set(cur.index.date)
    for col, (lag, unit) in lags.items():
        obs = _raw_for(col)
        dates = sorted(have)
        # random days, plus the day before and the day of each publication date (the day before must not show X)
        days = rng.sample(dates, 250)
        for x in rng.sample(sorted(obs), 40):
            a = _avail(x, lag, unit)
            days += [a - dt.timedelta(days=1), a]
        for day in days:
            if day not in have:
                continue
            row = cur.loc[pd.Timestamp(day)]
            want_v, want_age = _brute_expect(obs, day, lag, unit)
            if np.isnan(want_v):
                assert np.isnan(row[col]), (col, day)
            else:
                assert row[col] == pytest.approx(want_v, rel=0, abs=1e-9), (col, day)
                assert row[f"{col}__age_days"] == want_age, (col, day)
                assert row[f"{col}__age_days"] >= lag, (col, day)


# ------------------------------------------------------------- (b) shuffle: changing raw after X leaves rows <= X

def test_shuffle_synthetic(tmp_path):
    raw_a = tmp_path / "a"
    raw_b = tmp_path / "b"
    _write_synthetic(raw_a, random.Random(1))
    cutoff = dt.date(2020, 6, 30)
    _write_synthetic(raw_b, random.Random(1), perturb_after=cutoff)
    fa = _synthetic_frame(raw_a)
    fb = _synthetic_frame(raw_b)
    past = fa.index <= pd.Timestamp(cutoff)
    future = ~past
    assert past.sum() > 300 and future.sum() > 200
    pd.testing.assert_frame_equal(fa.loc[past], fb.loc[past], check_exact=True)
    assert not np.allclose(fa.loc[future, "fred_dgs2"].fillna(0), fb.loc[future, "fred_dgs2"].fillna(0))


@needs_real
def test_shuffle_real_raw(monkeypatch):
    """Perturb every real raw observation after X (in memory, same loaders) and check rows <= X are unchanged."""
    cur = _curated()
    end = cur.index[-1].date()
    cutoff = pd.Timestamp("2024-06-30")
    base, _ = ctx.build_frame(RAW, end=end)
    rng = np.random.default_rng(3)

    def wrap(loader):
        def f(raw_root, *args):
            obs, info = loader(raw_root, *args)
            future = obs.index > cutoff
            vals = obs.to_numpy(float).copy()
            vals[future] = vals[future] * rng.uniform(0.5, 1.5, future.sum()) + rng.normal(0, 1, future.sum())
            return pd.Series(vals, index=obs.index), info
        return f

    for key, loader in list(ctx._LOADERS.items()):
        monkeypatch.setitem(ctx._LOADERS, key, wrap(loader))
    shuffled, _ = ctx.build_frame(RAW, end=end)
    past = base.index <= cutoff
    pd.testing.assert_frame_equal(base.loc[past], shuffled.loc[past], check_exact=True)
    assert (base.loc[~past, "fred_dgs2"].fillna(-1) != shuffled.loc[~past, "fred_dgs2"].fillna(-1)).any()


# ------------------------------------------------------------- (c) schema, dtypes, dates, card

def test_schema_synthetic(tmp_path):
    raw = tmp_path / "raw"
    _write_synthetic(raw, random.Random(2))
    out = tmp_path / "ctx.parquet"
    frame = _synthetic_frame(raw)
    arrays = {"date": pa.array(frame.index.date, type=pa.date32())}
    for c in frame.columns:
        arrays[c] = pa.array(frame[c].to_numpy(float), type=pa.float64(), from_pandas=True)
    pq.write_table(pa.table(arrays), out)
    t = pq.read_table(out)
    assert t.schema.field("date").type == pa.date32()
    for f in t.schema:
        if f.name != "date":
            assert f.type == pa.float64(), f.name
    df = t.to_pandas()
    assert df["date"].is_unique
    assert (np.diff(pd.to_datetime(df["date"]).values).astype("timedelta64[D]") == np.timedelta64(1, "D")).all()


@needs_real
def test_schema_real():
    t = pq.read_table(CURATED)
    names = t.schema.names
    assert names[0] == "date" and t.schema.field("date").type == pa.date32()
    feats = [n for n in names if n != "date" and not n.endswith("__age_days")]
    assert len(feats) == len(ctx.FEATURES)
    for n in feats:
        assert n + "__age_days" in names, n
        assert t.schema.field(n).type == pa.float64(), n
        assert t.schema.field(n + "__age_days").type == pa.float64(), n
    df = t.to_pandas()
    assert df["date"].is_unique
    days = pd.to_datetime(df["date"])
    assert (days.diff().dropna() == pd.Timedelta(days=1)).all(), "one row per UTC day, no gaps"
    for n in feats:
        val, age = df[n], df[n + "__age_days"]
        assert (val.isna() == age.isna()).all(), f"{n}: value and age null-patterns differ"
        ok = age.dropna()
        assert (ok >= FEATURE[n].lag_days).all(), f"{n}: age below its lag"
        assert (ok <= ctx.MAX_AGE_DAYS).all(), f"{n}: age above cap"


@needs_real
def test_card_matches_frame():
    card = json.loads(CARD.read_text())
    assert set(card["columns"]) == {f.column for f in ctx.FEATURES}
    need = {"source", "url_pattern", "lag_days", "lag_rule", "revisions", "first_date", "last_date", "n_missing"}
    for name, c in card["columns"].items():
        assert need <= set(c), name
    assert "cftc_cot" in {e["source"] for e in card["exclusions"]}
    assert card["alfred_used"]["build"] == "no"
    df = _curated()
    for name, c in card["columns"].items():
        assert c["n_rows_no_value"] == int(df[name].isna().sum()), name
