"""Tests for quant.data.panel on a synthetic lake (no network, no real data)."""

from __future__ import annotations

import datetime as dt
import json
import shutil
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from quant.data import panel
from quant.data.panel import (
    BAR_NS,
    DAY_NS,
    MIN_NS,
    PANEL_BITS,
    HoldoutError,
    build_panel,
    load_dense,
)


def _ns(y, m, d, hh=0, mm=0, ss=0, ms=0) -> int:
    t = dt.datetime(y, m, d, hh, mm, ss, tzinfo=dt.UTC)
    return int(t.timestamp()) * 1_000_000_000 + ms * 1_000_000


D0 = _ns(2024, 1, 1)
N_DAYS = 40
FEB1 = _ns(2024, 2, 1)
MAR1 = _ns(2024, 3, 1)
GRID5 = D0 + np.arange(N_DAYS * 288, dtype=np.int64) * BAR_NS
SETTLE_B = _ns(2024, 1, 31, 12)
CUT_C = _ns(2024, 1, 26, 6, 2)  # last non-zero 1m bar of C

KLINE5_COLS = {
    "ts": pl.Int64, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
    "volume": pl.Float64, "quote_volume": pl.Float64, "count": pl.Int64, "taker_buy_volume": pl.Float64,
    "taker_buy_quote_volume": pl.Float64, "n_src": pl.Int64, "complete": pl.Boolean, "filled": pl.Boolean,
}


def _day_qv(mult: float) -> np.ndarray:
    """Daily quote volume pattern, varying by day so medians are sensitive to single days."""
    d = np.arange(N_DAYS)
    return mult * 1e6 * (1 + (d * 7919 % 40)) / 288.0  # per bar


def _quote_for(ts: np.ndarray, mult: float) -> np.ndarray:
    day = (ts - D0) // DAY_NS
    return _day_qv(mult)[day]


def _write_by_month(path_root: Path, sym: str, ts: np.ndarray, quote: np.ndarray, *, price: float = 100.0,
                    nan_rows: tuple = (), drop_rows: np.ndarray | None = None, zero_after: int | None = None,
                    kind: str = "klines/5m") -> None:
    n = ts.size
    keep = np.ones(n, dtype=bool) if drop_rows is None else ~drop_rows
    close = np.full(n, price)
    open_ = close.copy()
    high = close + 1.0
    low = close - 1.0
    for i in nan_rows:
        open_[i] = np.nan
    vol = quote / price
    if zero_after is not None:
        vol = np.where(ts >= zero_after, 0.0, vol)
        quote = np.where(ts >= zero_after, 0.0, quote)
    frame = pl.DataFrame({
        "ts": ts, "open": open_, "high": high, "low": low, "close": close,
        "volume": vol, "quote_volume": quote, "count": np.full(n, 10, dtype=np.int64),
        "taker_buy_volume": vol * 0.5, "taker_buy_quote_volume": quote * 0.5,
        "n_src": np.full(n, 5, dtype=np.int64), "complete": np.ones(n, dtype=bool),
        "filled": np.zeros(n, dtype=bool),
    }, schema=KLINE5_COLS).filter(pl.Series(keep))
    for ym, lo, hi in (("2024-01", D0, FEB1), ("2024-02", FEB1, MAR1)):
        part = frame.filter((pl.col("ts") >= lo) & (pl.col("ts") < hi))
        if part.height == 0:
            continue
        out = path_root / kind / sym / f"{ym}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        part.write_parquet(out)


def _write_series(root: Path, kind: str, sym: str, ts: np.ndarray, close: np.ndarray, flags: np.ndarray | None = None):
    flags = np.zeros(ts.size, dtype=np.uint32) if flags is None else flags
    frame = pl.DataFrame({"ts": ts, "open": close, "high": close, "low": close, "close": close,
                          "close_ts": ts + BAR_NS - 1, "flags": flags.astype(np.uint32)})
    for ym, lo, hi in (("2024-01", D0, FEB1), ("2024-02", FEB1, MAR1)):
        part = frame.filter((pl.col("ts") >= lo) & (pl.col("ts") < hi))
        if part.height:
            out = root / kind / sym / f"{ym}.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            part.write_parquet(out)


def _make_lake(tmp: Path) -> dict:
    """Four instruments. A: live, with funding, mark, premium and a gap. B: settled. C: no settle, 1m end.
    D: small volume, never eligible. Returns the input paths."""
    lake = tmp / "lake" / "binance_um"
    cur = tmp / "curated"
    cur.mkdir(parents=True, exist_ok=True)
    onboard = _ns(2023, 12, 1)

    # A: bars everywhere, a missing bar, a NaN open, mark and premium series
    a_ts = GRID5.copy()
    a_quote = _quote_for(a_ts, 1.0)
    gap = (a_ts == _ns(2024, 1, 3, 12))
    nan_idx = int(np.flatnonzero(a_ts == _ns(2024, 1, 4, 6))[0])
    _write_by_month(lake, "AAAUSDT", a_ts, a_quote, drop_rows=gap, nan_rows=(nan_idx,))
    _write_series(lake, "markPriceKlines/5m", "AAAUSDT", a_ts, np.full(a_ts.size, 100.5))
    _write_series(lake, "premiumIndexKlines/5m", "AAAUSDT", a_ts, np.full(a_ts.size, 0.001))

    # B: settles at SETTLE_B; bars continue to the end of the data
    _write_by_month(lake, "BBBUSDT", GRID5, _quote_for(GRID5, 1.7))
    # C: no settle; 5m volume and 1m volume stop at CUT_C, so the end is the 5m bar 06:00 + 5m
    c_quote = np.where(GRID5 < CUT_C, _quote_for(GRID5, 0.9), 0.0)
    _write_by_month(lake, "CCCUSDT", GRID5, c_quote, zero_after=_ns(2024, 1, 26, 6, 5))
    m1 = D0 + np.arange(N_DAYS * 1440, dtype=np.int64) * MIN_NS
    m1_vol = np.where(m1 <= CUT_C, 1.0, 0.0)
    for ym, lo, hi in (("2024-01", D0, FEB1), ("2024-02", FEB1, MAR1)):
        sel = (m1 >= lo) & (m1 < hi)
        out = lake / "klines" / "1m" / "CCCUSDT" / f"{ym}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"ts": m1[sel], "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
                      "volume": m1_vol[sel], "close_ts": m1[sel] + MIN_NS - 1, "quote_volume": m1_vol[sel] * 100,
                      "count": np.ones(sel.sum(), dtype=np.int64), "taker_buy_volume": 0.0,
                      "taker_buy_quote_volume": 0.0, "flags": np.zeros(sel.sum(), dtype=np.uint32)}).write_parquet(out)
    # D: volume too small for the universe
    _write_by_month(lake, "DDDUSDT", GRID5, np.full(GRID5.size, 1e4 / 288))

    # funding for A (sign kept as recorded) and B (settle + one event after the end)
    fa_ts = np.array([
        _ns(2023, 12, 31),  # before the first bar: not placed
        _ns(2024, 1, 5, 0, 0, 0, 2),  # +2 ms jitter -> bar 2024-01-04 23:55
        _ns(2024, 1, 5, 8, 0, 0, 2),  # +2 ms -> bar 07:55
        _ns(2024, 1, 5, 15, 59, 59, 997),  # -3 ms -> bar 15:55
        _ns(2024, 1, 3, 12, 5),  # bar 12:00 is the gap row: placed on the filled row, no fallback
    ], dtype=np.int64)
    fa_rate = np.array([0.00005, 0.0001, 0.0002, -0.0003, 0.00007])
    _write_funding(lake, "AAAUSDT", fa_ts, fa_rate)
    fb_ts = np.array([SETTLE_B, _ns(2024, 2, 1)], dtype=np.int64)  # second is after the settle: fallback
    _write_funding(lake, "BBBUSDT", fb_ts, np.array([0.0001, 0.00003]))

    # instruments
    inst = pl.DataFrame({
        "instrument_id": ["binance_um:AAAUSDT:20231201", "binance_um:BBBUSDT:20231201",
                          "binance_um:CCCUSDT:20231201", "binance_um:DDDUSDT:20231201",
                          "binance_um:EXCUSDT:20231201"],
        "symbol": ["AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT", "EXCUSDT"],
        "base": ["AAA", "BBB", "CCC", "DDD", "EXC"], "quote": ["USDT"] * 5,
        "contract_type": ["PERPETUAL"] * 5,
        "exclude_reason": [None, None, None, None, "tradifi"],
        "onboard_ts": [onboard] * 5,
        "status_now": ["TRADING", "SETTLING", "ABSENT", "TRADING", "TRADING"],
        "last_ts": [None, SETTLE_B, None, None, None],
        "first_archive_month": ["2024-01"] * 5,
        "last_archive_month": ["2024-02"] * 5,
    })
    inst_path = cur / "instruments.parquet"
    inst.write_parquet(inst_path)

    win = pl.DataFrame({
        "symbol": ["MARKET", "AAAUSDT"],
        "start_ts": [_ns(2024, 1, 6, 10), _ns(2024, 1, 8)],
        "end_ts": [_ns(2024, 1, 6, 10, 15), _ns(2024, 1, 8, 0, 5)],
        "reason": ["market_flat", "spike"],
        "magnitude_bps": [10000.0, 500.0],
    })
    win_path = cur / "quality" / "suspicious_windows.parquet"
    win_path.parent.mkdir(parents=True, exist_ok=True)
    win.write_parquet(win_path)

    ver_path = tmp / "kraken" / "verdicts.parquet"
    ver_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "window_id": ["MARKET|a", "AAAUSDT|refuted", "AAAUSDT|flip", "AAAUSDT|flip", "AAAUSDT|confirmed"],
        "symbol": ["MARKET", "AAAUSDT", "AAAUSDT", "AAAUSDT", "AAAUSDT"],
        "start_ts": [_ns(2024, 1, 6, 10), _ns(2024, 1, 7), _ns(2024, 1, 9), _ns(2024, 1, 9), _ns(2024, 1, 9, 1)],
        "end_ts": [_ns(2024, 1, 6, 10, 15), _ns(2024, 1, 7, 0, 10), _ns(2024, 1, 9, 0, 10), _ns(2024, 1, 9, 0, 10),
                   _ns(2024, 1, 9, 1, 5)],
        "verdict": ["outage_confirmed", "refuted", "refuted", "inconclusive", "confirmed"],
    }).write_parquet(ver_path)
    return {"lake": lake, "instruments": inst_path, "windows": win_path, "verdicts": ver_path}


def _write_funding(lake: Path, sym: str, ts: np.ndarray, rate: np.ndarray) -> None:
    out = lake / "funding" / f"{sym}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"ts": ts, "funding_interval_hours": np.full(ts.size, 8, dtype=np.int64),
                  "last_funding_rate": rate, "flags": np.zeros(ts.size, dtype=np.uint32)}).write_parquet(out)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("panel")
    inputs = _make_lake(tmp)
    out = tmp / "out"
    stats = build_panel(out, progress=None, **inputs)
    return {"tmp": tmp, "inputs": inputs, "out": out, "stats": stats}


def _bars(out: Path, sym: str | None = None) -> pl.DataFrame:
    df = pl.read_parquet(out / "bars_5m" / "2024.parquet")
    return df if sym is None else df.filter(pl.col("symbol") == sym)


def _row(df: pl.DataFrame, ts: int) -> dict:
    r = df.filter(pl.col("ts") == ts)
    assert r.height == 1, f"expected one row at {ts}, got {r.height}"
    return r.row(0, named=True)


# ---------------------------------------------------------------- tests

def test_funding_placement_hand_built_day(built):
    a = _bars(built["out"], "AAAUSDT")
    # rate on the bar whose close is F: open F-5m, sign as recorded
    assert _row(a, _ns(2024, 1, 4, 23, 55))["funding_rate"] == pytest.approx(0.0001)
    assert _row(a, _ns(2024, 1, 5, 7, 55))["funding_rate"] == pytest.approx(0.0002)
    assert _row(a, _ns(2024, 1, 5, 15, 55))["funding_rate"] == pytest.approx(-0.0003)
    # the gap row at 12:00 of Jan 3 gets the event at 12:05 (the 12:00 bar exists as a filled row)
    g = _row(a, _ns(2024, 1, 3, 12))
    assert g["funding_rate"] == pytest.approx(0.00007)
    assert g["filled"] is True
    # every other bar is zero, and the event before the first bar is not placed
    placed_ts = {_ns(2024, 1, 4, 23, 55), _ns(2024, 1, 5, 7, 55), _ns(2024, 1, 5, 15, 55), _ns(2024, 1, 3, 12)}
    other = a.filter(~pl.col("ts").is_in(list(placed_ts)))
    assert other["funding_rate"].abs().sum() == 0.0
    assert a["funding_rate"].sum() == pytest.approx(0.0001 + 0.0002 - 0.0003 + 0.00007)
    # interval in effect: 0 before the first placed event, 8 after
    assert _row(a, _ns(2024, 1, 3, 11, 55))["funding_interval_h"] == 0  # before the first placed event (12:00)
    assert _row(a, _ns(2024, 1, 3, 12))["funding_interval_h"] == 8
    stats = built["stats"]["funding"]
    # all instruments: A has 5 events, B has 2. One is before A's first bar, one is after B's settle
    assert stats["events"] == 7 and stats["not_placed"] == 2 and stats["after_end"] == 1
    assert stats["max_round_ms"] == pytest.approx(3.0)
    assert stats["fallback"] == 0


def test_funding_after_end_not_placed(built):
    b = _bars(built["out"], "BBBUSDT")
    last = _row(b, _ns(2024, 1, 31, 11, 55))
    assert last["ts"] == b["ts"].max()  # B trimmed at its settle
    # the event after the settle is not placed (it would otherwise be summed onto the last bar)
    assert last["funding_rate"] == pytest.approx(0.0001)
    assert last["issue_flags"] & PANEL_BITS["funding_fallback"] == 0
    assert built["stats"]["funding"]["bar_collisions"] == 0


def test_no_nan_in_ohlc_and_gap_row_is_filled(built):
    df = _bars(built["out"])
    for c in ("open", "high", "low", "close"):
        assert df[c].null_count() == 0
        assert not df[c].is_nan().any(), c
    a = _bars(built["out"], "AAAUSDT")
    gap = _row(a, _ns(2024, 1, 3, 12))
    assert gap["filled"] is True and gap["tradable"] is False
    assert gap["close"] == pytest.approx(_row(a, _ns(2024, 1, 3, 11, 55))["close"])  # carried forward
    nanbar = _row(a, _ns(2024, 1, 4, 6))
    assert nanbar["issue_flags"] & PANEL_BITS["ohlc_repaired"]
    assert nanbar["filled"] is True and nanbar["open"] == pytest.approx(100.0)


def test_delisted_tail_trimmed(built):
    b = _bars(built["out"], "BBBUSDT")
    assert b["ts"].max() == _ns(2024, 1, 31, 11, 55)  # bars open before settle_ts only
    assert b["ts"].min() == D0
    c = _bars(built["out"], "CCCUSDT")
    assert c["ts"].max() == _ns(2024, 1, 26, 6, 0)  # 1m end: 06:02 -> 5m bar 06:00 + 5m = 06:05
    tail = c.filter(pl.col("ts") >= _ns(2024, 1, 26, 5, 35))
    assert tail.height == 6
    assert tail["tradable"].sum() == 0
    assert (tail["issue_flags"].to_numpy() & PANEL_BITS["delist_tail"] != 0).all()
    before = _row(c, _ns(2024, 1, 26, 5, 30))
    assert before["issue_flags"] & PANEL_BITS["delist_tail"] == 0


def test_windows_inclusive_and_kraken_rules(built):
    a = _bars(built["out"], "AAAUSDT")
    for mm in (0, 5, 10, 15):  # market_flat + outage_confirmed, inclusive ends
        r = _row(a, _ns(2024, 1, 6, 10, mm))
        assert r["tradable"] is False
        assert r["issue_flags"] & PANEL_BITS["market_window"]
        assert r["issue_flags"] & PANEL_BITS["kraken_outage"]
    assert _row(a, _ns(2024, 1, 6, 10, 20))["tradable"] is True
    # refuted windows are NOT applied by the build (see test_refuted_pass_is_separate_and_idempotent)
    for mm in (0, 5, 10):
        r = _row(a, _ns(2024, 1, 7, 0, mm))
        assert r["tradable"] is True and r["issue_flags"] & PANEL_BITS["kraken_refuted"] == 0
    assert _row(a, _ns(2024, 1, 7, 0, 15))["tradable"] is True
    # last verdict for a window wins: the 'inconclusive' overwrite leaves those bars tradable
    assert _row(a, _ns(2024, 1, 9, 0, 5))["tradable"] is True
    # spike windows flag but do not change tradable
    s = _row(a, _ns(2024, 1, 8, 0, 0))
    assert s["tradable"] is True and s["issue_flags"] & PANEL_BITS["spike_window"]


def test_universe_lookahead(built, tmp_path):
    """Perturb day D (instrument A's quote volume x1000). Row D of the universe must not change; row D+1 may."""
    inputs = built["inputs"]
    base_u = pl.read_parquet(built["out"] / "universe_pit.parquet")
    D = _ns(2024, 1, 20)
    lake2 = tmp_path / "lake2"
    lake = inputs["lake"]
    import shutil
    shutil.copytree(lake, lake2 / "binance_um")
    ts_all = GRID5
    day = (ts_all - D0) // DAY_NS
    d_idx = (D - D0) // DAY_NS
    quote = _quote_for(ts_all, 1.0)
    quote = np.where(day == d_idx, quote * 1000.0, quote)
    _write_by_month(lake2 / "binance_um", "AAAUSDT", ts_all, quote, drop_rows=(ts_all == _ns(2024, 1, 3, 12)),
                    nan_rows=(int(np.flatnonzero(ts_all == _ns(2024, 1, 4, 6))[0]),))
    out2 = tmp_path / "out2"
    build_panel(out2, progress=None, lake=lake2 / "binance_um", instruments=inputs["instruments"],
                windows=inputs["windows"], verdicts=inputs["verdicts"])
    pert_u = pl.read_parquet(out2 / "universe_pit.parquet")
    row_d = base_u.filter(pl.col("date") == dt.date(2024, 1, 20))
    row_d2 = pert_u.filter(pl.col("date") == dt.date(2024, 1, 20))
    assert row_d.equals(row_d2), "universe row D changed when day D's bars changed"
    a_next = base_u.filter((pl.col("date") == dt.date(2024, 1, 21)) & (pl.col("instrument_id").str.starts_with("binance_um:AAA")))
    a_next2 = pert_u.filter((pl.col("date") == dt.date(2024, 1, 21)) & (pl.col("instrument_id").str.starts_with("binance_um:AAA")))
    assert a_next["median_qv_30d"][0] != a_next2["median_qv_30d"][0], "row D+1 should see day D"


def test_universe_rules(built):
    u = pl.read_parquet(built["out"] / "universe_pit.parquet")
    a = u.filter(pl.col("instrument_id") == "binance_um:AAAUSDT:20231201")
    # days_listed counts from onboard to D-1
    first = a.sort("date").row(0, named=True)
    assert first["days_listed"] == (dt.date(2024, 1, 1) - dt.date(2023, 12, 1)).days
    # 30-day median is over D-30..D-1 and eligible needs 30 days listed and >= 20M
    r = a.filter(pl.col("date") == dt.date(2024, 2, 5)).row(0, named=True)
    assert r["days_listed"] >= 30
    assert r["eligible"] == bool(r["median_qv_30d"] >= 20e6 and r["rank"] is not None and r["rank"] <= 100)
    # D (tiny volume) and the excluded instrument are never eligible
    assert u.filter(pl.col("instrument_id").str.starts_with("binance_um:DDD"))["eligible"].sum() == 0
    assert u.filter(pl.col("instrument_id").str.starts_with("binance_um:EXC")).height == 0
    # no instrument is ranked after its settle or after it stopped trading
    b = u.filter(pl.col("instrument_id").str.starts_with("binance_um:BBB") & (pl.col("date") > dt.date(2024, 2, 1)))
    assert b["rank"].null_count() == b.height
    assert u.filter(pl.col("eligible")).height > 0


def test_load_dense_padded_shapes_and_holdout_guard(built):
    out = built["out"]
    d = load_dense("2024-01-02", "2024-01-03", ["close", "funding_rate", "trades"], root=out, pad=64)
    assert d.ts.shape[0] % 64 == 0 and d.ts.shape[0] == 320  # 288 real bars padded to 320
    assert len(d.ids) % 64 == 0 and len(d.ids) == 64
    assert d.n_t == 288 and d.n_n == 4
    assert d.x["close"].dtype == np.float32 and d.x["funding_rate"].shape == (320, 64)
    assert d.present.shape == (320, 64) and d.tradable.shape == (320, 64) and d.eligible.shape == (320, 64)
    assert not d.present[288:, :].any() and not d.present[:, 4:].any()
    assert d.ids[4:] == [None] * 60
    assert d.ids[0] == "binance_um:AAAUSDT:20231201"
    assert np.isnan(d.x["close"][288:, :]).all()
    # the bar of 2024-01-02 00:00 for A
    col = d.ids.index("binance_um:AAAUSDT:20231201")
    assert d.present[0, col] and d.x["close"][0, col] == pytest.approx(100.0)
    with pytest.raises(HoldoutError):
        load_dense("2025-09-30", "2025-10-02", ["close"], root=out)
    # end exactly at the holdout boundary is fine; holdout=True permits the read
    load_dense("2025-09-30", "2025-10-01", ["close"], root=out)
    d_h = load_dense("2025-10-01", "2025-10-02", ["close"], root=out, holdout=True)
    assert not d_h.present.any()


def test_build_twice_identical(built, tmp_path):
    out2 = tmp_path / "again"
    inp = built["inputs"]
    build_panel(out2, progress=None, lake=inp["lake"], instruments=inp["instruments"], windows=inp["windows"],
                verdicts=inp["verdicts"])
    assert (built["out"] / "bars_5m" / "2024.parquet").read_bytes() == (out2 / "bars_5m" / "2024.parquet").read_bytes()
    # Table.equals treats NaN as unequal, so compare the data with NaN mapped to None
    from polars.testing import assert_frame_equal
    for rel in ("bars_5m/2024.parquet", "universe_pit.parquet"):
        t1 = pl.read_parquet(built["out"] / rel).fill_nan(None)
        t2 = pl.read_parquet(out2 / rel).fill_nan(None)
        assert_frame_equal(t1, t2, check_exact=True)  # prints the first differing column on failure


def test_schema_and_sort_order(built):
    t = pq.read_table(built["out"] / "bars_5m" / "2024.parquet")
    assert t.schema.names == panel.BARS_SCHEMA.names
    assert t.schema.field("issue_flags").type == pa.uint32()
    ts = t["ts"].to_numpy()
    assert (np.diff(ts) >= 0).all()
    stats = json.loads((built["out"] / "panel_stats.json").read_text())
    assert stats["rows"] == t.num_rows
    assert stats["years"]["2024"]["instruments"] == 4


def test_margin_exception_admits_only_bases_without_usdt_perp(tmp_path):
    """Lead ruling: margin_* perps are out unless their base never had a USDT PERPETUAL (1000x prefix normalised)."""
    rows = [
        # (instrument_id, symbol, base, quote, contract_type, exclude_reason)
        ("binance_um:AAAUSDT:20231201", "AAAUSDT", "AAA", "USDT", "PERPETUAL", None),
        ("binance_um:AAABUSD:20231201", "AAABUSD", "AAA", "BUSD", "PERPETUAL", "margin_busd"),
        ("binance_um:ZZZBUSD:20231201", "ZZZBUSD", "ZZZ", "BUSD", "PERPETUAL", "margin_busd"),
        ("binance_um:QQQUSDT:20231201", "QQQUSDT", "QQQ", "USDT", "PERPETUAL", None),
        ("binance_um:1000QQQUSDC:20231201", "1000QQQUSDC", "1000QQQ", "USDC", "PERPETUAL", "margin_usdc"),
        ("binance_um:WWWUSDC:20231201", "WWWUSDC", "WWW", "USDC", "PERPETUAL", "margin_usdc"),
        ("binance_um:WWWQ:20231201", "WWWQ", "WWW", "QUARTER", "QUARTERLY:2024-12-27", "quarterly"),
        ("binance_um:UUUU:20231201", "UUUU", "UUU", "U", "PERPETUAL", "margin_u"),  # never admitted (lead ruling)
    ]
    cat = pl.DataFrame({
        "instrument_id": [r[0] for r in rows], "symbol": [r[1] for r in rows], "base": [r[2] for r in rows],
        "quote": [r[3] for r in rows], "contract_type": [r[4] for r in rows], "exclude_reason": [r[5] for r in rows],
        "onboard_ts": [_ns(2023, 12, 1)] * len(rows), "status_now": ["TRADING"] * len(rows),
        "last_ts": [None] * len(rows), "first_archive_month": ["2024-01"] * len(rows),
        "last_archive_month": ["2024-02"] * len(rows),
    })
    p = tmp_path / "instruments.parquet"
    cat.write_parquet(p)
    admitted = panel.margin_exception_ids(cat)
    # AAA and QQQ (1000 prefix removed) have USDT perps, so their margin copies stay out. ZZZ and WWW never had one
    assert admitted == ["binance_um:ZZZBUSD:20231201", "binance_um:WWWUSDC:20231201"]
    inst = panel._read_instruments(p, None)
    assert sorted(inst["symbol"].to_list()) == ["AAAUSDT", "QQQUSDT", "WWWUSDC", "ZZZBUSD"]


def test_margin_exception_bars_and_universe(built, tmp_path):
    """An admitted margin perp gets bars in bars_5m and universe rows; a rejected one gets neither."""
    inp = built["inputs"]
    lake = tmp_path / "lake"
    shutil.copytree(inp["lake"], lake)
    _write_by_month(lake, "ZZZBUSD", GRID5, _quote_for(GRID5, 1.2))
    _write_by_month(lake, "AAABUSD", GRID5, _quote_for(GRID5, 1.2))
    cat = pl.read_parquet(inp["instruments"])
    extra = pl.DataFrame({
        "instrument_id": ["binance_um:ZZZBUSD:20231201", "binance_um:AAABUSD:20231201"],
        "symbol": ["ZZZBUSD", "AAABUSD"], "base": ["ZZZ", "AAA"], "quote": ["BUSD", "BUSD"],
        "contract_type": ["PERPETUAL", "PERPETUAL"], "exclude_reason": ["margin_busd", "margin_busd"],
    })
    rest = {c: None for c in cat.columns if c not in extra.columns}
    extra = extra.with_columns([pl.lit(None, dtype=cat.schema[c]).alias(c) for c in rest]).select(cat.columns)
    extra = extra.with_columns(
        pl.Series("onboard_ts", [_ns(2023, 12, 1)] * 2, dtype=pl.Int64),
        pl.Series("status_now", ["TRADING", "TRADING"]),
        pl.Series("first_archive_month", ["2024-01", "2024-01"]),
        pl.Series("last_archive_month", ["2024-02", "2024-02"]),
    )
    inst_path = tmp_path / "instruments.parquet"
    pl.concat([cat, extra], how="diagonal_relaxed").write_parquet(inst_path)
    out = tmp_path / "out"
    build_panel(out, progress=None, lake=lake, instruments=inst_path, windows=inp["windows"],
                verdicts=inp["verdicts"])
    bars = pl.read_parquet(out / "bars_5m" / "2024.parquet")
    ids = set(bars["instrument_id"].unique().to_list())
    assert "binance_um:ZZZBUSD:20231201" in ids
    assert "binance_um:AAABUSD:20231201" not in ids
    u = pl.read_parquet(out / "universe_pit.parquet")
    assert u.filter(pl.col("instrument_id") == "binance_um:AAABUSD:20231201").height == 0
    assert u.filter(pl.col("instrument_id") == "binance_um:ZZZBUSD:20231201").height > 0


# ---------------------------------------------------------------- real smoke panel (read-only), BUG-2 and iter_dense

SMOKE = Path(__file__).resolve().parents[2] / "data" / "samples" / "b-smoke" / "panel"
BAR5 = 5 * 60 * 1_000_000_000


@pytest.fixture(scope="module")
def smoke():
    if not (SMOKE / "bars_5m" / "2024.parquet").exists():
        pytest.skip("smoke panel not present")
    return SMOKE


def test_padding_never_crosses_end_real_panel(smoke):
    """BUG-2: a window whose length is not a multiple of 64 bars. Padded ts repeat the last real ts."""
    start, end = _ns(2024, 12, 25), _ns(2025, 1, 7)  # 3744 bars, 3744 % 64 == 32
    d = panel.load_dense(start, end, ["close"], root=smoke)
    assert d.n_t == 3744 and d.ts.shape[0] == 3776
    assert d.ts[0] == start
    assert d.ts[d.n_t - 1] == end - BAR5
    assert (d.ts[d.n_t:] == end - BAR5).all()  # padded ts repeat the last real ts, never past end
    assert d.ts.max() <= end - BAR5
    assert not d.owned[d.n_t:].any() and d.owned[:d.n_t].all()
    assert not d.present[d.n_t:].any() and not d.tradable[d.n_t:].any() and not d.eligible[d.n_t:].any()
    assert np.isnan(d.x["close"][d.n_t:]).all()


def test_padding_window_touching_holdout_boundary(smoke):
    """A window ending exactly at 2025-10-01 must not trip the guard through its padding (BUG-2 repro shape)."""
    d = panel.load_dense(_ns(2025, 9, 28), _ns(2025, 10, 1), ["close"], root=smoke)
    assert d.ts.max() == _ns(2025, 9, 30, 23, 55)
    assert d.ts.shape[0] % 64 == 0 and d.n_t == 864


def test_iter_owned_rows_concat_equals_load_dense(smoke):
    start, end, halo = _ns(2024, 12, 25), _ns(2025, 1, 7), 288
    fields = ["close", "volume", "funding_rate"]
    chunks = list(panel.iter_dense(start, end, fields, halo_bars=halo, root=smoke))
    assert len(chunks) == 2  # 2024 and 2025 parts
    full = panel.load_dense(start, end, fields, root=smoke)
    assert all(c.ids == full.ids for c in chunks)  # one shared id axis
    assert all(c.ts.shape == chunks[0].ts.shape for c in chunks)  # one padded shape
    owned_ts = np.concatenate([c.ts[c.owned] for c in chunks])
    np.testing.assert_array_equal(owned_ts, full.ts[: full.n_t])
    for f in fields:
        got = np.concatenate([c.x[f][c.owned] for c in chunks])
        np.testing.assert_array_equal(got, full.x[f][: full.n_t])
    for name in ("present", "tradable", "eligible"):
        got = np.concatenate([getattr(c, name)[c.owned] for c in chunks])
        np.testing.assert_array_equal(got, getattr(full, name)[: full.n_t])
    assert sum(int(c.owned.sum()) for c in chunks) == full.n_t


def test_halo_rows_equal_previous_chunk_tail(smoke):
    start, end, halo = _ns(2024, 12, 25), _ns(2025, 1, 7), 288
    fields = ["close", "volume"]
    c0, c1 = list(panel.iter_dense(start, end, fields, halo_bars=halo, root=smoke))[:2]
    assert not c1.owned[:halo].any() and c1.owned[halo: halo + 5].all()
    prev_owned = np.flatnonzero(c0.owned)
    tail = prev_owned[-halo:]
    np.testing.assert_array_equal(c1.ts[:halo], c0.ts[tail])
    for f in fields:
        np.testing.assert_array_equal(c1.x[f][:halo], c0.x[f][tail])
    np.testing.assert_array_equal(c1.present[:halo], c0.present[tail])
    np.testing.assert_array_equal(c1.eligible[:halo], c0.eligible[tail])


def test_iter_padding_never_crosses_end(smoke):
    start, end = _ns(2024, 12, 25), _ns(2025, 1, 7)
    for c in panel.iter_dense(start, end, ["close"], halo_bars=288, root=smoke):
        assert c.ts.max() <= end - BAR5
        assert not c.owned[c.n_t:].any()
        assert not c.present[c.n_t:].any()


def test_flag_pass_is_separate_and_idempotent(built, tmp_path):
    """apply_flags: refuted bars, flat_market windows, premium/mark holes and M3a, from its own pass, rerunnable."""
    inp = built["inputs"]
    out = tmp_path / "out_flags"
    build_panel(out, progress=None, lake=inp["lake"], instruments=inp["instruments"], windows=inp["windows"],
                verdicts=inp["verdicts"])
    a = _bars(out, "AAAUSDT")
    assert _row(a, _ns(2024, 1, 7, 0, 0))["tradable"] is True  # build alone does not apply refuted
    stats = json.loads((out / "panel_stats.json").read_text())
    assert stats["refuted_pass"] is None and stats["verdicts"]["mtime_utc"]
    sc = tmp_path / "scorecard.json"  # a hermetic M4 window: Jan 6 10:00 to 10:05, every symbol
    sc.write_text(json.dumps({"metrics": [{"id": "M4", "detail": {"market_windows": [
        {"start": _ns(2024, 1, 6, 10), "end": _ns(2024, 1, 6, 10, 5)}]}}]}))
    fp = panel.apply_flags(out, verdicts=inp["verdicts"], scorecard=sc)
    assert fp["refuted_bars"] == 3  # the Kraken evidence bit is set on 3 bars
    assert fp["flat_market_bars"] == 8  # 2 bars x 4 live symbols
    assert fp["base_inconsistent"] == 0
    a = _bars(out, "AAAUSDT")
    for mm in (0, 5, 10):  # Kraken refuted alone (no consensus file here): flag set, tradable unchanged
        r = _row(a, _ns(2024, 1, 7, 0, mm))
        assert r["tradable"] is True and r["issue_flags"] & PANEL_BITS["kraken_refuted"]
    assert _row(a, _ns(2024, 1, 7, 0, 15))["tradable"] is True
    assert _row(a, _ns(2024, 1, 9, 0, 5))["tradable"] is True  # its last verdict is 'inconclusive'
    fl = _row(a, _ns(2024, 1, 6, 10, 5))
    assert fl["tradable"] is False and fl["issue_flags"] & PANEL_BITS["flat_market"]
    # M3a: the gap row (missing source bar inside the live range) is filled and not tradable
    gap = _row(a, _ns(2024, 1, 3, 12))
    assert gap["filled"] is True and gap["tradable"] is False
    assert fp["filled_tradable_violations"] == 0 and fp["filled_bars"] >= 1
    # holes: BBB has no premium or mark series at all, so its bars are holes; the value stays NaN, tradability holds
    b = _bars(out, "BBBUSDT")
    assert b["premium_close"].is_nan().all() and b["mark_close"].is_nan().all()
    assert (b["issue_flags"].to_numpy() & PANEL_BITS["premium_hole"] != 0).all()
    assert b.filter(pl.col("tradable")).height > 0
    # second run: nothing changes
    fp2 = panel.apply_flags(out, verdicts=inp["verdicts"], scorecard=sc)
    assert fp2["tradable_changed_to_false"] == 0 and fp2["tradable_changed_to_true"] == 0
    assert fp2["refuted_bars"] == 3 and fp2["flat_market_bars"] == 8
    stats = json.loads((out / "panel_stats.json").read_text())
    assert stats["flag_pass"]["flat_market_bars"] == 8 and stats["years"]["2024"]["nontradable_rows"] > 0


def _scale_prices(src: Path, dst: Path, factor: float) -> None:
    """Copy a synthetic lake and multiply its 5m OHLC by factor (a second version with a visible difference)."""
    shutil.copytree(src, dst)
    for f in (dst / "klines" / "5m").rglob("*.parquet"):
        df = pl.read_parquet(f)
        df = df.with_columns([pl.col(c) * factor for c in ("open", "high", "low", "close")])
        df.write_parquet(f)


def test_reader_sees_old_or_new_set_never_a_mix(tmp_path):
    """Staging + one atomic current swap: a reader during a rebuild sees a whole version, never a mix."""
    import threading

    inp = _make_lake(tmp_path / "v1")
    root = tmp_path / "curated"
    build_panel(root, progress=None, lake=inp["lake"], instruments=inp["instruments"], windows=inp["windows"],
                verdicts=inp["verdicts"])
    lake2 = tmp_path / "lake2"
    _scale_prices(inp["lake"], lake2, 2.0)
    samples: list[set] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            d = panel.load_dense("2024-01-02", "2024-01-03", ["close"], root=root)
            vals = d.x["close"][d.present]
            samples.append(set(np.round(vals.astype(np.float64), 3).tolist()))

    th = threading.Thread(target=reader)
    th.start()
    try:
        build_panel(root, progress=None, lake=lake2, instruments=inp["instruments"], windows=inp["windows"],
                    verdicts=inp["verdicts"])
    finally:
        stop.set()
        th.join()
    assert samples, "reader never ran"
    for s_ in samples:
        assert len(s_) == 1, f"mixed versions in one read: {s_}"
        assert s_ in ({100.0}, {200.0})
    final = panel.load_dense("2024-01-02", "2024-01-03", ["close"], root=root)
    assert set(np.round(final.x["close"][final.present].astype(np.float64), 3).tolist()) == {200.0}
    assert not [p for p in root.iterdir() if p.name.startswith(".staging-")]  # nothing left staged
    assert len(list((root / "versions").iterdir())) == 2  # the old version is kept, not deleted


def test_flag_pass_publishes_a_new_version_and_keeps_the_old(tmp_path):
    inp = _make_lake(tmp_path / "v1")
    root = tmp_path / "curated"
    build_panel(root, progress=None, lake=inp["lake"], instruments=inp["instruments"], windows=inp["windows"],
                verdicts=inp["verdicts"])
    old = (root / "current").resolve()
    old_bytes = (old / "bars_5m" / "2024.parquet").read_bytes()
    sc = tmp_path / "scorecard.json"
    sc.write_text(json.dumps({"metrics": [{"id": "M4", "detail": {"market_windows": [
        {"start": _ns(2024, 1, 6, 10), "end": _ns(2024, 1, 6, 10, 5)}]}}]}))
    panel.apply_flags(root, verdicts=inp["verdicts"], scorecard=sc)
    new = (root / "current").resolve()
    assert new != old and new.parent == old.parent
    assert (old / "bars_5m" / "2024.parquet").read_bytes() == old_bytes  # old version untouched
    assert not [p for p in root.iterdir() if p.name.startswith(".staging-")]
    d = panel.load_dense("2024-01-06 10:00", "2024-01-06 10:10", ["close"], root=root)
    col = d.ids.index("binance_um:AAAUSDT:20231201")
    assert not d.tradable[:2, col].any()  # new version carries the flat_market mask


def test_month_chunks_equal_year_chunks_real_slice():
    """chunk='month' on a real 3-month slice (crosses a year boundary): owned rows equal the year chunks and one load."""
    curated = Path(__file__).resolve().parents[2] / "data" / "curated"
    if not (curated / "current").exists():
        pytest.skip("curated panel not present")
    start, end = "2024-11-01", "2025-02-01"
    fields = ["close", "funding_rate"]
    months = list(panel.iter_dense(start, end, fields, chunk="month", halo_bars=8640, root=curated))
    years = list(panel.iter_dense(start, end, fields, chunk="year", halo_bars=8640, root=curated))
    assert len(months) == 3 and len(years) == 2
    assert all(c.ids == months[0].ids for c in months + years)
    assert all(c.ts.shape == months[0].ts.shape for c in months)  # one padded shape for all month chunks
    full = panel.load_dense(start, end, fields, root=curated)

    def owned_concat(chunks, name):
        return np.concatenate([getattr(c, name)[c.owned] if name in ("ts", "present", "tradable", "eligible")
                               else c.x[name][c.owned] for c in chunks])

    for name in ("ts", "present", "tradable", "eligible", "close", "funding_rate"):
        m = owned_concat(months, name)
        y = owned_concat(years, name)
        np.testing.assert_array_equal(m, y, err_msg=name)
        if name == "ts":
            np.testing.assert_array_equal(m, full.ts[: full.n_t])
    assert sum(int(c.owned.sum()) for c in months) == full.n_t


def test_retention_keeps_current_and_one_previous(tmp_path):
    """After each successful swap only current and the newest other version remain; a failed validation keeps all."""
    inp = _make_lake(tmp_path / "v1")
    root = tmp_path / "curated"
    for _ in range(4):
        build_panel(root, progress=None, lake=inp["lake"], instruments=inp["instruments"],
                    windows=inp["windows"], verdicts=inp["verdicts"])
    versions = sorted(d.name for d in (root / "versions").iterdir())
    assert len(versions) == 2
    cur = (root / "current").resolve().name
    assert cur in versions
    assert panel.load_dense("2024-01-02", "2024-01-03", ["close"], root=root).n_t == 288  # current still readable
    # a staged set that fails validation: no swap, no prune
    bad = root / ".staging-bad"
    bad.mkdir()
    (bad / "bars_5m").mkdir()
    (bad / "panel_stats.json").write_text(json.dumps({"rows": 1, "years": {}}))
    before = sorted(d.name for d in (root / "versions").iterdir())
    with pytest.raises(RuntimeError):
        panel._publish(root, bad, "20990101T000000000000Z")
    assert sorted(d.name for d in (root / "versions").iterdir()) == before
    assert (root / "current").resolve().name == cur


def test_prune_keeps_previous_when_root_is_relative(tmp_path, monkeypatch):
    """Regression: a relative --out (data/curated) once kept the current version as 'previous' and deleted the real one."""
    monkeypatch.chdir(tmp_path)
    root = Path("curated")
    for name in ("20260101T000000000000Z", "20260102T000000000000Z", "20260103T000000000000Z"):
        (root / "versions" / name).mkdir(parents=True)
    (root / "current").symlink_to(Path("versions") / "20260103T000000000000Z")
    removed = panel._prune_versions(root)
    assert removed == ["20260101T000000000000Z"]
    assert sorted(d.name for d in (root / "versions").iterdir()) == ["20260102T000000000000Z", "20260103T000000000000Z"]


# ---------------------------------------------------------------- F1: listing segments (fix_contract F1)
#
# XYZUSDT is one ticker with two listings. The earlier listing traded D0 .. D0+10d and was then left alone; the
# dead period D0+10d .. D0+20d is zero volume in the lake; the new listing onboards at D0+20d. Each listing is its
# own instrument; the dead rows belong to no instrument and are counted in build.dead_period_rows.

REUSE_DEAD_FROM = D0 + 10 * DAY_NS
REUSE_DEAD_TO = D0 + 20 * DAY_NS
REUSE_A = "binance_um:XYZUSDT:20231202"
REUSE_B = "binance_um:XYZUSDT:20240121"


def _write_reused_lake(tmp: Path) -> dict:
    lake = tmp / "lake" / "binance_um"
    cur = tmp / "curated"
    cur.mkdir(parents=True, exist_ok=True)
    quote = np.where((GRID5 >= REUSE_DEAD_FROM) & (GRID5 < REUSE_DEAD_TO), 0.0, _quote_for(GRID5, 1.0))
    _write_by_month(lake, "XYZUSDT", GRID5, quote)
    inst = pl.DataFrame({
        "instrument_id": [REUSE_A, REUSE_B],
        "symbol": ["XYZUSDT", "XYZUSDT"], "base": ["XYZ", "XYZ"], "quote": ["USDT", "USDT"],
        "contract_type": ["PERPETUAL", "PERPETUAL"], "exclude_reason": [None, None],
        "onboard_ts": [D0 - 30 * DAY_NS, REUSE_DEAD_TO],
        "onboard_ts_source": ["exchangeInfo", "exchangeInfo"],
        "status_now": ["ABSENT", "TRADING"],
        "last_ts": [REUSE_DEAD_FROM - BAR_NS, None],
        "last_ts_source": ["last_traded_bar", None],
        "listed_end_ts": [REUSE_DEAD_FROM, None],
        "symbol_reused": [True, True],
        "first_archive_month": ["2024-01", "2024-01"], "last_archive_month": ["2024-02", "2024-02"],
    })
    inst_path = cur / "instruments.parquet"
    inst.write_parquet(inst_path)
    win_path = cur / "quality" / "suspicious_windows.parquet"
    win_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(schema={"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64, "reason": pl.Utf8,
                         "magnitude_bps": pl.Float64}).write_parquet(win_path)
    return {"lake": lake, "instruments": inst_path, "windows": win_path,
            "verdicts": tmp / "no_verdicts.parquet"}


@pytest.fixture(scope="module")
def reused(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("reused")
    inputs = _write_reused_lake(tmp)
    out = tmp / "out"
    stats = build_panel(out, progress=None, **inputs)
    return {"out": out, "stats": stats}


def test_reused_ticker_rows_belong_to_their_own_listing(reused):
    x = _bars(reused["out"], "XYZUSDT")
    assert set(x["instrument_id"].unique().to_list()) == {REUSE_A, REUSE_B}
    a = x.filter(pl.col("instrument_id") == REUSE_A)["ts"]
    b = x.filter(pl.col("instrument_id") == REUSE_B)["ts"]
    assert a.min() == D0 and a.max() == REUSE_DEAD_FROM - BAR_NS  # from the first bar to its last traded bar
    assert b.min() == REUSE_DEAD_TO  # the new listing starts at its own onboard, not at the ticker's first bar


def test_no_ticker_timestamp_belongs_to_two_instruments(reused):
    x = _bars(reused["out"], "XYZUSDT")
    assert x.select(["ts"]).unique().height == x.height  # one row per (ticker, ts): ts is unique within the ticker


def test_dead_period_rows_are_dropped_and_counted(reused):
    x = _bars(reused["out"], "XYZUSDT")
    dead = (x["ts"] >= REUSE_DEAD_FROM) & (x["ts"] < REUSE_DEAD_TO)
    assert int(dead.sum()) == 0
    assert reused["stats"]["build"]["dead_period_rows"] == 10 * 288  # ten days of zero-volume 5m bars in no listing
    assert reused["stats"]["build"]["dead_period_rows"] > 0


def test_earlier_listing_never_runs_past_the_next_onboard(tmp_path):
    """A listing end later than the next listing's onboard is cut at that onboard (fix_contract F1)."""
    inputs = _write_reused_lake(tmp_path)
    inst = pl.read_parquet(inputs["instruments"])
    inst = inst.with_columns(pl.when(pl.col("instrument_id") == REUSE_A)
                             .then(pl.lit(D0 + 25 * DAY_NS)).otherwise(pl.col("listed_end_ts")).alias("listed_end_ts"))
    inst.write_parquet(inputs["instruments"])
    stats = build_panel(tmp_path / "out", progress=None, **inputs)
    x = _bars(tmp_path / "out", "XYZUSDT")
    a = x.filter(pl.col("instrument_id") == REUSE_A)["ts"]
    assert a.max() < REUSE_DEAD_TO
    assert x.select(["ts"]).unique().height == x.height
    assert stats["build"]["dead_period_rows"] == 0  # the cut listing covers its own dead bars: none are dead


def test_funding_rest_tail_after_archive(tmp_path):
    # the archive is monthly: events after its last one come from lake/rest/funding; earlier REST rows never override
    _write_funding(tmp_path, "AAAUSDT", np.array([_ns(2024, 1, 31, 16)]), np.array([0.0001]))
    rest = tmp_path / "rest" / "funding"
    rest.mkdir(parents=True)
    pl.DataFrame({"ts": [_ns(2024, 1, 31, 16), _ns(2024, 2, 1, 0)], "funding_interval_hours": [8, 8],
                  "last_funding_rate": [0.9, 0.0002], "source": ["rest", "rest"]}).write_parquet(rest / "AAAUSDT.parquet")
    t = panel._read_funding(tmp_path, "AAAUSDT")
    assert t["ts"].to_list() == [_ns(2024, 1, 31, 16), _ns(2024, 2, 1, 0)]
    assert t["last_funding_rate"].to_list() == [0.0001, 0.0002]
    assert panel._read_funding(tmp_path, "ZZZUSDT") is None


def test_mark_rest_overlay_fills_archive_holes_only(tmp_path):
    # wave R: the archive has no daily mark zip for some symbols on 2026-10-18; lake/rest/markPriceKlines fills it.
    # Archive rows win where both exist; REST adds only the missing keys.
    ts = _ns(2024, 1, 2) + BAR_NS * np.arange(6)
    _write_series(tmp_path, "markPriceKlines/5m", "AAAUSDT", ts[[0, 1, 4, 5]], np.array([1.0, 1.0, 1.0, 1.0]))
    _write_series(tmp_path / "rest", "markPriceKlines/5m", "AAAUSDT", ts[1:5], np.array([9.0, 2.0, 2.0, 9.0]))
    active = [{"symbol": "AAAUSDT", "instrument_id": "binance_um:AAAUSDT:20240101"}]
    arch = panel._read_month_series(tmp_path / "markPriceKlines/5m/AAAUSDT/2024-01.parquet", active[0]["instrument_id"],
                                    "mark_close")
    rest = panel._rest_series(tmp_path, active, "2024-01", "markPriceKlines", "mark_close")
    out, added = panel.pin.rest_fill(arch, rest)
    out = out.sort("ts")
    assert added == 2
    assert out["ts"].to_list() == ts.tolist()
    assert out["mark_close"].to_list() == [1.0, 1.0, 2.0, 2.0, 1.0, 1.0]
    assert out.columns == arch.columns
