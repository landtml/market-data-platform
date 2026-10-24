"""Tests for quant.core._qcore resample_bars / trade_bars / consensus (bars.rs).

- resample_bars is checked against a pandas reference on random klines and on one real BTCUSDT 1m day.
- trade_bars is checked against finlab-ml's dollar_bars / volume_bars / tick_bars on the same trades: same bar count,
  same closes and volumes (exact), same bar-close labels.
- consensus is checked on crafted venues with one bad print and with missing bars.

Real data: one Binance USD-M 1m kline day and one aggTrades day (data.binance.vision), read straight from the zip
files under data/samples/b-bars/real. The tests skip when those files are absent.
"""

from __future__ import annotations

import importlib.util
import os
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from finlab import bars as fb


def _load_core():
    """quant.core._qcore, or a private build when QCORE_BARS_SO points at one (used before the crate merge)."""
    so = os.environ.get("QCORE_BARS_SO")
    if so:
        spec = importlib.util.spec_from_file_location("_qcore", so)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    from quant.core import _qcore

    return _qcore


core = _load_core()

MIN = 60_000_000_000
HOUR = 60 * MIN
DAY = 24 * HOUR
START = 1_709_596_800_000_000_000  # 2024-03-05 00:00:00 UTC, minute- and day-aligned
ROOT = Path(__file__).resolve().parents[2] / "data" / "samples" / "b-bars" / "real"
KLINES_ZIP = ROOT / "BTCUSDT-1m-2024-03-05.zip"
AGG_ZIP = ROOT / "BTCUSDT-aggTrades-2024-02-04.zip"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _read_zip_csv(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as z:
        with z.open(z.namelist()[0]) as f:
            return pd.read_csv(f)


def random_klines(n_min: int, seed: int, drop_frac: float = 0.0):
    """Random 1m OHLCV rows on the minute grid with some minutes dropped (gaps). Sorted, unique, consistent OHLC."""
    rng = np.random.default_rng(seed)
    keep = np.flatnonzero(rng.random(n_min) >= drop_frac)
    ts = START + keep.astype(np.int64) * MIN
    m = keep.size
    steps = rng.normal(0.0, 1e-3, size=m)
    close = 100.0 * np.exp(np.cumsum(steps))
    open_ = np.concatenate(([100.0], close))[:-1]
    spread = np.abs(rng.normal(0.0, 5e-4, size=(2, m))) * close
    high = np.maximum(open_, close) + spread[0]
    low = np.minimum(open_, close) - spread[1]
    vol = rng.gamma(2.0, 50.0, size=m)
    qv = vol * close
    n = rng.integers(1, 500, size=m).astype(np.int64)
    tbv = vol * rng.random(m)
    tbqv = tbv * close
    return dict(ts=ts, o=open_, h=high, l=low, c=close, v=vol, qv=qv, n=n, tbv=tbv, tbqv=tbqv)


def pandas_resample(k: dict, out_ns: int) -> dict:
    """Reference: pandas resample over the same rows; empty buckets become filled bars (V3)."""
    idx = pd.to_datetime(k["ts"], unit="ns", utc=True)
    df = pd.DataFrame(
        {x: k[x] for x in ("o", "h", "l", "c", "v", "qv", "n", "tbv", "tbqv")}, index=idx
    )
    freq = pd.Timedelta(out_ns, unit="ns")
    g = df.resample(freq, origin="epoch", closed="left", label="left")
    agg = g.agg(
        o=("o", "first"),
        h=("h", "max"),
        l=("l", "min"),
        c=("c", "last"),
        v=("v", "sum"),
        qv=("qv", "sum"),
        n=("n", "sum"),
        tbv=("tbv", "sum"),
        tbqv=("tbqv", "sum"),
        n_src=("o", "count"),
    )
    empty = agg["n_src"].to_numpy() == 0
    agg["c"] = agg["c"].ffill()
    for col in ("o", "h", "l"):
        agg.loc[empty, col] = agg.loc[empty, "c"]
    expected = out_ns // MIN
    return dict(
        ts=agg.index.asi8.astype(np.int64),
        o=agg["o"].to_numpy(),
        h=agg["h"].to_numpy(),
        l=agg["l"].to_numpy(),
        c=agg["c"].to_numpy(),
        v=agg["v"].to_numpy(),
        qv=agg["qv"].to_numpy(),
        n=agg["n"].to_numpy().astype(np.int64),
        tbv=agg["tbv"].to_numpy(),
        tbqv=agg["tbqv"].to_numpy(),
        n_src=agg["n_src"].to_numpy().astype(np.int64),
        complete=agg["n_src"].to_numpy() == expected,
        filled=empty,
    )


def run_resample(k: dict, out_ns: int) -> dict:
    return core.resample_bars(
        k["ts"], k["o"], k["h"], k["l"], k["c"], k["v"], k["qv"], k["n"], k["tbv"], k["tbqv"], out_ns
    )


def assert_resample_equal(got: dict, ref: dict, rtol: float = 1e-12) -> None:
    np.testing.assert_array_equal(got["ts"], ref["ts"])
    for col in ("o", "h", "l", "c"):
        np.testing.assert_array_equal(got[col], ref[col], err_msg=col)
    for col in ("v", "qv", "tbv", "tbqv"):
        np.testing.assert_allclose(got[col], ref[col], rtol=rtol, atol=0.0, err_msg=col)
    np.testing.assert_array_equal(got["n"], ref["n"], err_msg="n")
    np.testing.assert_array_equal(got["n_src"], ref["n_src"], err_msg="n_src")
    np.testing.assert_array_equal(got["complete"], ref["complete"], err_msg="complete")
    np.testing.assert_array_equal(got["filled"], ref["filled"], err_msg="filled")


def btc_klines() -> dict:
    df = _read_zip_csv(KLINES_ZIP)
    ts = df["open_time"].to_numpy(np.int64) * 1_000_000
    return dict(
        ts=ts,
        o=df["open"].to_numpy(np.float64),
        h=df["high"].to_numpy(np.float64),
        l=df["low"].to_numpy(np.float64),
        c=df["close"].to_numpy(np.float64),
        v=df["volume"].to_numpy(np.float64),
        qv=df["quote_volume"].to_numpy(np.float64),
        n=df["count"].to_numpy(np.int64),
        tbv=df["taker_buy_volume"].to_numpy(np.float64),
        tbqv=df["taker_buy_quote_volume"].to_numpy(np.float64),
    )


def take(k: dict, keep: np.ndarray) -> dict:
    return {x: np.ascontiguousarray(a[keep]) for x, a in k.items()}


def btc_trades():
    df = _read_zip_csv(AGG_ZIP)
    df = df.sort_values(["transact_time", "agg_trade_id"], kind="stable").reset_index(drop=True)
    buyer_maker = df["is_buyer_maker"]
    assert buyer_maker.dtype == bool, buyer_maker.dtype
    return (
        df["transact_time"].to_numpy(np.int64) * 1_000_000,
        np.ascontiguousarray(df["price"].to_numpy(np.float64)),
        np.ascontiguousarray(df["quantity"].to_numpy(np.float64)),
        np.ascontiguousarray(buyer_maker.to_numpy(bool)),
    )


def synthetic_trades(n: int, seed: int):
    rng = np.random.default_rng(seed)
    ts = np.cumsum(rng.integers(0, 3, size=n)).astype(np.int64) * 1_000_000 + START
    price = 30_000.0 * np.exp(np.cumsum(rng.normal(0.0, 2e-4, size=n)))
    qty = rng.gamma(0.7, 0.4, size=n)
    qty[rng.random(n) < 0.05] = 0.0  # zero-size trades exist on real tapes too
    bm = rng.random(n) < 0.5
    return ts, price, qty, bm


def assert_finlab_equal(ours: dict, fdf: pd.DataFrame, ts: np.ndarray, volumes: bool = True) -> None:
    """Exact equality with finlab. ``volumes=False`` for tick bars, where finlab got no volumes and reports NaN."""
    ends = fdf.index.to_numpy(np.int64)  # RangeIndex input: the index holds the closing trade's position
    assert len(ours["ts"]) == len(fdf), f"bar count {len(ours['ts'])} != finlab {len(fdf)}"
    np.testing.assert_array_equal(ours["ts"], ts[ends], err_msg="bar-close labels")
    np.testing.assert_array_equal(ours["c"], fdf["close"].to_numpy(), err_msg="close")
    np.testing.assert_array_equal(ours["o"], fdf["open"].to_numpy(), err_msg="open")
    np.testing.assert_array_equal(ours["h"], fdf["high"].to_numpy(), err_msg="high")
    np.testing.assert_array_equal(ours["l"], fdf["low"].to_numpy(), err_msg="low")
    np.testing.assert_array_equal(ours["n_trades"], fdf["ticks"].to_numpy(), err_msg="ticks")
    if volumes:
        np.testing.assert_array_equal(ours["volume"], fdf["volume"].to_numpy(), err_msg="volume")
        np.testing.assert_array_equal(ours["dollar_volume"], fdf["dollar_value"].to_numpy(), err_msg="dollar")


# --------------------------------------------------------------------------- #
# resample_bars
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("out_ns", [MIN, 5 * MIN, 15 * MIN, HOUR, 4 * HOUR, DAY])
def test_resample_random_matches_pandas(out_ns):
    k = random_klines(3 * 1440, seed=11, drop_frac=0.07)
    got = run_resample(k, out_ns)
    assert_resample_equal(got, pandas_resample(k, out_ns))


@pytest.mark.parametrize("out_ns,drop", [(5 * MIN, 0.9), (HOUR, 0.995)])
def test_resample_random_heavy_gaps_matches_pandas(out_ns, drop):
    k = random_klines(3 * 1440, seed=12, drop_frac=drop)
    got = run_resample(k, out_ns)
    assert got["filled"].sum() > 0
    assert_resample_equal(got, pandas_resample(k, out_ns))


def test_resample_random_full_grid_is_complete():
    k = random_klines(1440, seed=3)
    got = run_resample(k, HOUR)
    assert len(got["ts"]) == 24
    assert got["complete"].all() and not got["filled"].any()
    np.testing.assert_array_equal(got["n_src"], np.full(24, 60))


@pytest.mark.parametrize("out_ns", [5 * MIN, HOUR, 4 * HOUR, DAY])
def test_resample_real_btc_1m_day(out_ns):
    if not KLINES_ZIP.exists():
        pytest.skip("real kline sample absent")
    k = btc_klines()
    got = run_resample(k, out_ns)
    assert_resample_equal(got, pandas_resample(k, out_ns))
    assert got["complete"].all(), "a full UTC day of 1m bars must fill every bucket"
    assert not got["filled"].any()


def test_resample_real_btc_1m_day_with_gaps():
    if not KLINES_ZIP.exists():
        pytest.skip("real kline sample absent")
    k = btc_klines()
    rng = np.random.default_rng(5)
    minute = (k["ts"] - k["ts"][0]) // MIN
    keep = np.ones(k["ts"].size, dtype=bool)
    keep[rng.choice(k["ts"].size, size=40, replace=False)] = False
    keep[(minute >= 100) & (minute < 105)] = False  # one whole 5m bucket
    keep[(minute >= 300) & (minute < 360)] = False  # one whole hour
    k = take(k, keep)
    for out_ns in (5 * MIN, HOUR):
        got = run_resample(k, out_ns)
        ref = pandas_resample(k, out_ns)
        assert_resample_equal(got, ref)
        assert got["filled"].any()
        assert not got["complete"][got["filled"]].any()


def test_resample_gap_is_filled_not_dropped():
    # 5m buckets: [0,5) has minutes 0-2; [5,10) is empty; [10,15) has minute 10.
    k = {
        "ts": np.array([START, START + MIN, START + 2 * MIN, START + 10 * MIN], dtype=np.int64),
        "o": np.array([1.0, 2.0, 3.0, 9.0]),
        "h": np.array([1.5, 2.5, 3.5, 9.5]),
        "l": np.array([0.5, 1.5, 2.5, 8.5]),
        "c": np.array([2.0, 3.0, 4.0, 9.0]),
        "v": np.array([1.0, 1.0, 1.0, 5.0]),
        "qv": np.array([2.0, 3.0, 4.0, 45.0]),
        "n": np.array([10, 20, 30, 50], dtype=np.int64),
        "tbv": np.array([0.5, 0.5, 0.5, 2.0]),
        "tbqv": np.array([1.0, 1.0, 1.0, 18.0]),
    }
    got = run_resample(k, 5 * MIN)
    assert got["ts"].tolist() == [START, START + 5 * MIN, START + 10 * MIN]
    assert got["filled"].tolist() == [False, True, False]
    assert got["complete"].tolist() == [False, False, False]
    assert got["n_src"].tolist() == [3, 0, 1]
    filled = got["filled"]
    assert got["o"][filled][0] == got["h"][filled][0] == got["l"][filled][0] == got["c"][filled][0] == 4.0
    assert got["v"][filled][0] == 0.0 and got["n"][filled][0] == 0 and got["tbqv"][filled][0] == 0.0
    assert got["c"][0] == 4.0 and got["v"][0] == 3.0 and got["n"][0] == 60 and got["h"][0] == 3.5
    for col in got.values():
        if isinstance(col, np.ndarray) and col.dtype.kind == "f":
            assert np.isfinite(col).all(), "V3: never NaN"


def test_resample_rejects_bad_input():
    k = random_klines(20, seed=1)
    bad_order = {x: a[::-1].copy() for x, a in k.items()}
    with pytest.raises(ValueError, match="strictly increasing"):
        run_resample(bad_order, 5 * MIN)
    dupe = {x: a.copy() for x, a in k.items()}
    dupe["ts"][3] = dupe["ts"][2]
    with pytest.raises(ValueError, match="strictly increasing"):
        run_resample(dupe, 5 * MIN)
    with pytest.raises(ValueError, match="minute-aligned"):
        run_resample({**k, "ts": k["ts"] + 1}, 5 * MIN)
    with pytest.raises(ValueError, match="multiple of 60 s"):
        run_resample(k, 90_000_000_000)
    nan_in = {x: a.copy() for x, a in k.items()}
    nan_in["c"][4] = np.nan
    with pytest.raises(ValueError, match="finite"):
        run_resample(nan_in, 5 * MIN)
    with pytest.raises(ValueError, match="rows"):
        run_resample({**k, "v": k["v"][:-1]}, 5 * MIN)


def test_resample_empty_input():
    k = random_klines(0, seed=1)
    got = run_resample(k, 5 * MIN)
    assert all(len(v) == 0 for v in got.values())


# --------------------------------------------------------------------------- #
# trade_bars against finlab-ml
# --------------------------------------------------------------------------- #


def test_trade_bars_dollar_equals_finlab_real_aggtrades():
    if not AGG_ZIP.exists():
        pytest.skip("real aggTrades sample absent")
    ts, p, q, bm = btc_trades()
    thr = float(np.sum(p * q) / 400.0)  # about 400 bars for the day
    ours = core.trade_bars(ts, p, q, bm, "dollar", thr)
    fdf = fb.dollar_bars(p, q, thr)
    assert len(fdf) > 300
    assert_finlab_equal(ours, fdf, ts)


def test_trade_bars_volume_equals_finlab_real_aggtrades():
    if not AGG_ZIP.exists():
        pytest.skip("real aggTrades sample absent")
    ts, p, q, bm = btc_trades()
    thr = float(np.sum(q) / 400.0)
    ours = core.trade_bars(ts, p, q, bm, "volume", thr)
    fdf = fb.volume_bars(p, q, thr)
    assert len(fdf) > 300
    assert_finlab_equal(ours, fdf, ts)


def test_trade_bars_tick_equals_finlab_real_aggtrades():
    if not AGG_ZIP.exists():
        pytest.skip("real aggTrades sample absent")
    ts, p, q, bm = btc_trades()
    ours = core.trade_bars(ts, p, q, bm, "tick", 1000.0)
    fdf = fb.tick_bars(p, 1000)
    assert len(fdf) == p.size // 1000
    assert_finlab_equal(ours, fdf, ts, volumes=False)


@pytest.mark.parametrize("seed,n", [(1, 5_000), (2, 50_000), (3, 20_000)])
def test_trade_bars_equals_finlab_synthetic(seed, n):
    ts, p, q, bm = synthetic_trades(n, seed)
    for kind, thr, ref in (
        ("dollar", 7_500.0, lambda: fb.dollar_bars(p, q, 7_500.0)),
        ("volume", 25.0, lambda: fb.volume_bars(p, q, 25.0)),
        ("tick", 37, lambda: fb.tick_bars(p, 37)),
    ):
        ours = core.trade_bars(ts, p, q, bm, kind, float(thr))
        assert_finlab_equal(ours, ref(), ts, volumes=kind != "tick")


def test_trade_bars_fields_and_tail():
    # volume threshold 4: bar 1 = trades 0..3 (volume 1+2+0+1 = 4); bar 2 = trades 4..5 (3+1 = 4); trade 6 is the tail.
    ts = np.array([START + i for i in range(7)], dtype=np.int64)
    p = np.array([10.0, 11.0, 9.0, 12.0, 12.0, 8.0, 8.0])
    q = np.array([1.0, 2.0, 0.0, 1.0, 3.0, 1.0, 1.0])
    bm = np.array([False, True, False, False, False, True, False])
    out = core.trade_bars(ts, p, q, bm, "volume", 4.0)
    assert out["ts"].tolist() == [START + 3, START + 5]
    assert out["o"].tolist() == [10.0, 12.0]
    assert out["h"].tolist() == [12.0, 12.0]
    assert out["l"].tolist() == [9.0, 8.0]
    assert out["c"].tolist() == [12.0, 8.0]
    assert out["volume"].tolist() == [4.0, 4.0]
    assert out["n_trades"].tolist() == [4, 2]
    # taker buys (is_buyer_maker false): bar 1 has trades 0, 2, 3 -> 1 + 0 + 1; bar 2 has trades 4, 5 -> 3
    assert out["buy_volume"].tolist() == [2.0, 3.0]
    assert out["dollar_volume"][0] == pytest.approx(10 * 1 + 11 * 2 + 9 * 0 + 12 * 1)
    assert out["vwap"][0] == pytest.approx(44.0 / 4.0)
    assert out["tail_trades"] == 1


def test_trade_bars_rejects_bad_input():
    ts, p, q, bm = synthetic_trades(10, 1)
    with pytest.raises(ValueError, match="kind must be"):
        core.trade_bars(ts, p, q, bm, "imbalance", 5.0)
    with pytest.raises(ValueError, match="threshold"):
        core.trade_bars(ts, p, q, bm, "tick", 0.0)
    with pytest.raises(ValueError, match="non-decreasing"):
        core.trade_bars(ts[::-1].copy(), p, q, bm, "tick", 2.0)
    bad_p = p.copy()
    bad_p[2] = 0.0
    with pytest.raises(ValueError, match="price"):
        core.trade_bars(ts, bad_p, q, bm, "tick", 2.0)


# --------------------------------------------------------------------------- #
# consensus
# --------------------------------------------------------------------------- #


def _venues_with_bad_print():
    """Three venues on a 10-minute grid. Venue C misses minutes 3 and 7; venue B prints 101 at minute 4 (bad)."""
    t = START + np.arange(10, dtype=np.int64) * MIN
    a = np.full(10, 100.0)
    b = np.full(10, 100.0)
    b[4] = 101.0
    keep_c = np.array([i for i in range(10) if i not in (3, 7)])
    c = np.full(keep_c.size, 100.0)
    return [t, t, t[keep_c]], [a, b, c]


def test_consensus_three_venues_flags_bad_print():
    ts_list, close_list = _venues_with_bad_print()
    out = core.consensus(ts_list, close_list, 50.0)
    assert out["ts"].tolist() == [START + i * MIN for i in range(10)]
    assert out["n_venues"].tolist() == [3, 3, 3, 2, 3, 3, 3, 2, 3, 3]
    assert out["median"][4] == 100.0
    flags = [np.asarray(x) for x in out["outlier"]]
    devs = [np.asarray(x) for x in out["dev_bps"]]
    # Only venue B at minute 4 is flagged: deviation 100 bps against a 50 bps limit.
    assert flags[1][4]
    assert devs[1][4] == pytest.approx(100.0)
    assert devs[0][4] == 0.0 and devs[2][4] == 0.0
    assert not flags[0][4] and not flags[2][4]
    assert int(sum(f.sum() for f in flags)) == 1
    assert out["max_dev_bps"][4] == pytest.approx(100.0)
    # Venue C is absent on minutes 3 and 7: NaN deviation there, never flagged.
    assert np.isnan(devs[2][3]) and np.isnan(devs[2][7])
    assert not flags[2][3] and not flags[2][7]


def test_consensus_two_venues_flags_both():
    t = START + np.arange(4, dtype=np.int64) * MIN
    a = np.array([100.0, 100.0, 100.0, 100.0])
    b = np.array([100.0, 100.6, 100.0, 100.0])
    out = core.consensus([t, t], [a, b], 50.0)
    # Two venues: median = mean = 100.3; gap 0.6 is 59.8 bps, above the 50 bps limit: both flagged.
    assert out["n_venues"].tolist() == [2, 2, 2, 2]
    assert out["median"][1] == pytest.approx(100.3)
    assert np.asarray(out["outlier"][0])[1] and np.asarray(out["outlier"][1])[1]
    assert not np.asarray(out["outlier"][0])[0]
    assert np.asarray(out["dev_bps"][0])[1] == pytest.approx(out["dev_bps"][1][1])
    assert np.asarray(out["dev_bps"][0])[1] == pytest.approx(0.6 / 100.3 * 1e4)
    out_loose = core.consensus([t, t], [a, b], 100.0)
    assert not np.asarray(out_loose["outlier"][0]).any()


def test_consensus_union_grid_and_single_venue():
    t1 = START + np.array([0, 2], dtype=np.int64) * MIN
    t2 = START + np.array([1], dtype=np.int64) * MIN
    out = core.consensus([t1, t2], [np.array([5.0, 6.0]), np.array([5.5])], 10.0)
    assert out["ts"].tolist() == [START, START + MIN, START + 2 * MIN]
    assert out["n_venues"].tolist() == [1, 1, 1]
    assert out["max_dev_bps"].tolist() == [0.0, 0.0, 0.0]
    assert not any(np.asarray(x).any() for x in out["outlier"])


def test_consensus_rejects_bad_input():
    t = START + np.arange(3, dtype=np.int64) * MIN
    with pytest.raises(ValueError):
        core.consensus([t], [np.array([1.0, 2.0])], 10.0)
    with pytest.raises(ValueError):
        core.consensus([t, t], [np.array([1.0, 2.0, 3.0])], 10.0)
    with pytest.raises(ValueError):
        core.consensus([t[::-1].copy()], [np.array([1.0, 2.0, 3.0])], 10.0)
    with pytest.raises(ValueError):
        core.consensus([t], [np.array([1.0, np.nan, 3.0])], 10.0)
