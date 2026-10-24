"""Fix wave F panel rules (infra/swarm/briefs/fix_contract.md): stale runs, index and spot at the month boundary,
single-bar print as defined by w-r5b2, and the panel_stats counts.

Each rule has a test that fails on the pre-fix code. The F1 segment tests are in tests/unit/test_panel.py.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest
from quant.data import panel
from quant.data.panel import BAR_NS, BARS_SCHEMA, BARS_WRITE_OPTS, PANEL_BITS, SINGLE_PRINT_BPS, build_panel

T0 = int(dt.datetime(2024, 1, 1, tzinfo=dt.UTC).timestamp()) * 1_000_000_000
FEB1 = int(dt.datetime(2024, 2, 1, tzinfo=dt.UTC).timestamp()) * 1_000_000_000
N_BARS = 40 * 288  # 40 days from 2024-01-01: January and February 2024 (Feb 1 is bar 31 * 288)
JAN_LAST = (FEB1 - BAR_NS - T0) // BAR_NS  # index of the 2024-01-31 23:55 bar
STALE = PANEL_BITS.get("stale_run", 1 << 25)  # the new bit (fix wave F): red before the fix
SP = PANEL_BITS["single_bar_print"]
THR = SINGLE_PRINT_BPS / 1e4
DEV = 1 + 2 * THR  # a close 100 bps off its mark


def _ts(i: int) -> int:
    return T0 + i * BAR_NS


def _write_month_files(root: Path, sub: str, sym: str, ts: np.ndarray, cols: dict) -> None:
    """Split a series into month files: root/<sub>/<sym>/<YYYY-MM>.parquet (the layout the panel reads)."""
    for ym, lo, hi in (("2024-01", T0, FEB1), ("2024-02", FEB1, _ts(N_BARS))):
        sel = (ts >= lo) & (ts < hi)
        if not sel.any():
            continue
        out = root / sub / sym / f"{ym}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"ts": ts[sel], **{k: v[sel] for k, v in cols.items()}}).write_parquet(out)


def _kline_cols(close: np.ndarray, volume: np.ndarray) -> dict:
    n = close.size
    return {"open": close, "high": close, "low": close, "close": close,
            "volume": volume, "quote_volume": volume * close, "count": np.ones(n, dtype=np.int64),
            "taker_buy_volume": volume * 0.5, "taker_buy_quote_volume": volume * close * 0.5,
            "n_src": np.full(n, 5, dtype=np.int64), "complete": np.ones(n, dtype=bool),
            "filled": np.zeros(n, dtype=bool)}


def _instrument(iid: str, sym: str) -> dict:
    return {"instrument_id": iid, "symbol": sym, "base": sym[:-4], "quote": "USDT", "contract_type": "PERPETUAL",
            "exclude_reason": None, "onboard_ts": T0 - 30 * 86_400_000_000_000, "onboard_ts_source": "exchangeInfo",
            "status_now": "TRADING", "last_ts": None, "last_ts_source": None, "listed_end_ts": None,
            "symbol_reused": False, "first_archive_month": "2024-01", "last_archive_month": "2024-02"}


def _build_lake(tmp: Path) -> dict:
    """IDX: every bar traded; index and spot klines distinct per bar; mark NaN on one bar.
    STL: a run of 300 zero-volume bars with a frozen close inside (bars 300..599).
    SHT: a run of 287 (one bar short of the rule)."""
    lake = tmp / "lake" / "binance_um"
    cur = tmp / "curated"
    cur.mkdir(parents=True, exist_ok=True)
    ts = np.array([_ts(i) for i in range(N_BARS)], dtype=np.int64)
    idx = np.arange(N_BARS, dtype=np.float64)

    close = 100.0 + idx * 0.01
    _write_month_files(lake, "klines/5m", "IDXUSDT", ts, _kline_cols(close, np.ones(N_BARS)))
    _write_month_files(lake, "indexPriceKlines/5m", "IDXUSDT", ts, {"close": 1000.0 + idx})
    _write_month_files(lake.parent, "binance_spot/klines_5m", "IDXUSDT", ts, {"close": 2000.0 + idx})
    mark = np.full(N_BARS, 100.0)
    mark[5] = np.nan
    _write_month_files(lake, "markPriceKlines/5m", "IDXUSDT", ts, {"close": mark, "flags": np.zeros(N_BARS, np.uint32)})

    for sym, z0, z1 in (("STLUSDT", 300, 600), ("SHTUSDT", 300, 587)):
        c = 100.0 + idx * 0.01
        vol = np.ones(N_BARS)
        vol[z0:z1] = 0.0
        c[z0:z1] = c[z0 - 1]  # frozen close inside the zero-volume run
        _write_month_files(lake, "klines/5m", sym, ts, _kline_cols(c, vol))

    inst = pl.DataFrame([_instrument("binance_um:IDXUSDT:20231202", "IDXUSDT"),
                         _instrument("binance_um:STLUSDT:20231202", "STLUSDT"),
                         _instrument("binance_um:SHTUSDT:20231202", "SHTUSDT")])
    inst_path = cur / "instruments.parquet"
    inst.write_parquet(inst_path)
    win = cur / "quality" / "suspicious_windows.parquet"
    win.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(schema={"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64, "reason": pl.Utf8,
                         "magnitude_bps": pl.Float64}).write_parquet(win)
    return {"lake": lake, "instruments": inst_path, "windows": win, "verdicts": tmp / "no_verdicts.parquet"}


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("panel_f")
    inputs = _build_lake(tmp)
    out = tmp / "out"
    stats = build_panel(out, progress=None, **inputs)
    return {"out": out, "stats": stats, "inputs": inputs}


def _sym(out: Path, sym: str) -> pl.DataFrame:
    return pl.read_parquet(out / "bars_5m" / "2024.parquet").filter(pl.col("symbol") == sym).sort("ts")


def _at(df: pl.DataFrame, i: int) -> dict:
    r = df.filter(pl.col("ts") == _ts(i))
    assert r.height == 1
    return r.row(0, named=True)


# ---------------------------------------------------------------- stale runs

def test_stale_threshold_is_288_bars_and_is_pure():
    assert panel.STALE_MIN_BARS == 288
    ts = np.array([_ts(i) for i in range(400)], dtype=np.int64)
    close = np.ones(400)
    vol = np.ones(400)
    vol[10:297] = 0.0  # 287 zero-volume bars
    assert panel.stale_runs(ts, vol, close) == []
    vol[297] = 0.0  # 288
    assert panel.stale_runs(ts, vol, close) == [(_ts(10), _ts(297))]


def test_stale_run_is_broken_by_a_price_change_or_a_traded_bar():
    ts = np.array([_ts(i) for i in range(400)], dtype=np.int64)
    vol = np.zeros(400)
    vol[0] = 1.0
    vol[200] = 1.0  # traded bar: splits the zero-volume stretch into two runs of 199 bars
    assert panel.stale_runs(ts, vol, np.ones(400)) == []
    close = np.ones(400)
    close[150:] = 2.0  # a changed close inside the zero-volume stretch: the run is not frozen
    vol2 = np.zeros(400)
    vol2[0] = 1.0
    assert panel.stale_runs(ts, vol2, close) == []


def test_stale_run_bit_marks_the_frozen_bars_only(built):
    s = _sym(built["out"], "STLUSDT")
    flags = s["issue_flags"].to_numpy()
    stale_idx = np.flatnonzero(flags & STALE)
    assert stale_idx.size == 300
    assert s.filter(pl.col("ts") == _ts(300))["tradable"][0] is False
    assert s.filter(pl.col("ts") == _ts(599))["tradable"][0] is False
    assert _at(s, 299)["issue_flags"] & STALE == 0 and _at(s, 600)["issue_flags"] & STALE == 0
    assert _at(s, 450)["close"] == pytest.approx(_at(s, 299)["close"])  # price kept


def test_stale_run_of_287_bars_gets_no_bit(built):
    s = _sym(built["out"], "SHTUSDT")
    assert int((s["issue_flags"].to_numpy() & STALE != 0).sum()) == 0
    assert bool(s["tradable"].all())


def test_stale_bit_is_an_untradable_bit():
    assert panel.UNTRADABLE_MASK & STALE
    assert STALE == 1 << 25 and PANEL_BITS["single_bar_print"] == 1 << 24


def _staged_copy(built: dict, tmp: Path) -> Path:
    """A staging-shaped copy of the built set (bars, stats, universe), for a flag pass."""
    import shutil
    d = tmp / "stage" / "set"
    d.mkdir(parents=True)
    shutil.copytree(built["out"] / "bars_5m", d / "bars_5m")
    for name in ("panel_stats.json", "universe_pit.parquet"):
        shutil.copy(built["out"] / name, d / name)
    return d


def _scorecard(tmp: Path) -> Path:
    sc = tmp / "scorecard.json"
    sc.write_text(json.dumps({"metrics": [{"id": "M4", "detail": {"market_windows": []}}]}))
    return sc


def test_stale_bit_survives_the_flag_pass(built, tmp_path):
    d = _staged_copy(built, tmp_path)
    panel._flag_pass(d, verdicts=tmp_path / "none.parquet", scorecard=_scorecard(tmp_path),
                     consensus=tmp_path / "none_c.parquet")
    s = pl.read_parquet(d / "bars_5m" / "2024.parquet").filter(pl.col("symbol") == "STLUSDT")
    flags = s["issue_flags"].to_numpy()
    assert int((flags & STALE != 0).sum()) == 300
    assert int(((flags & STALE != 0) & s["tradable"].to_numpy()).sum()) == 0


# ---------------------------------------------------------------- index and spot at the month boundary

def test_index_close_on_month_first_joins_the_previous_month_last_kline(built):
    x = _sym(built["out"], "IDXUSDT")
    r = _at(x, JAN_LAST + 1)  # 2024-02-01 00:00
    assert r["index_close"] is not None
    assert r["index_close"] == pytest.approx(1000.0 + JAN_LAST)  # the kline opened at 2024-01-31 23:55


def test_index_close_is_the_kline_opened_one_bar_earlier_on_every_bar(built):
    x = _sym(built["out"], "IDXUSDT")
    ts = x["ts"].to_numpy()
    got = x["index_close"].to_numpy()
    want = 1000.0 + ((ts - T0) // BAR_NS - 1)
    ok = np.arange(ts.size) > 0
    assert np.isfinite(got[ok]).all()
    assert np.allclose(got[ok], want[ok])


def test_spot_close_on_month_first_joins_the_previous_month_last_kline(built):
    x = _sym(built["out"], "IDXUSDT")
    r = _at(x, JAN_LAST + 1)
    assert r["spot_close"] == pytest.approx(2000.0 + JAN_LAST)


# ---------------------------------------------------------------- nulls and panel_stats

def test_no_nan_in_any_float_column_and_missing_mark_is_null(built):
    x = pl.read_parquet(built["out"] / "bars_5m" / "2024.parquet")
    for name, dtype in zip(x.columns, x.dtypes):
        if dtype in (pl.Float32, pl.Float64):
            assert x[name].is_nan().sum() == 0, name
    assert _at(_sym(built["out"], "IDXUSDT"), 5)["mark_close"] is None
    assert _at(_sym(built["out"], "IDXUSDT"), 5)["premium_close"] is None


def test_build_tradable_false_rows_and_bit_table_are_written(built):
    x = pl.read_parquet(built["out"] / "bars_5m" / "2024.parquet")
    st = json.loads((built["out"] / "panel_stats.json").read_text())
    assert st["build"]["tradable_false_rows"] == int((~x["tradable"]).sum())
    table = st["build"]["tradable_false_by_bit"]
    assert set(PANEL_BITS) <= set(table)
    assert table["stale_run"] == 300


def test_flag_pass_refreshes_tradable_false_rows(built, tmp_path):
    d = _staged_copy(built, tmp_path)
    panel._flag_pass(d, verdicts=tmp_path / "none.parquet", scorecard=_scorecard(tmp_path),
                     consensus=tmp_path / "none_c.parquet")
    st = json.loads((d / "panel_stats.json").read_text())
    x = pl.read_parquet(d / "bars_5m" / "2024.parquet")
    assert st["build"]["tradable_false_rows"] == int((~x["tradable"]).sum())
    assert st["build"]["tradable_false_by_bit"]["stale_run"] == 300


# ---------------------------------------------------------------- F4: single_bar_print as w-r5b2 defines it
#
# w-r5b2 (infra/swarm/reports/w-r5b2.md; data/samples/w-r5b2/analyze.py, kind=single_bar, and the 32,189 count
# reproduced in data/samples/a-spike/dev_rules.py): a bar is a single-bar print when
#   real (volume > 0, not filled), has a mark, |close / mark - 1| > 50 bps (off), and it is a run of one off bar
#   (neither contiguous neighbour is off), and its close is NOT within 50 bps of the mark of either neighbour
#   (mark-lag explained), and no other untradable bit is set (the base condition; a-spike found it undocumented).
# The confirmed-window exemption is applied on top (see DESIGN.md).

def _stage_rows(root: Path, rows: list[dict], consensus_rows: list[dict] = ()) -> tuple[Path, Path, Path]:
    out = root / ".staging-test"
    (out / "bars_5m").mkdir(parents=True)
    base = {"open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0, "quote_volume": 1.0,
            "taker_buy_quote_volume": 1.0, "trades": 1, "mark_close": 1.0, "premium_close": 1.0,
            "funding_rate": 0.0, "funding_interval_h": 8, "filled": False, "tradable": True, "issue_flags": 0}
    full = [{**base, **r} for r in rows]
    df = pl.DataFrame(full).sort(["ts", "instrument_id"])
    df = df.with_columns(
        *[pl.lit(None, dtype=pl.Float64).alias(c) for c in ("oi_sum_open_interest", "oi_sum_open_interest_value",
                                                            "index_close", "spot_close", "bybit_funding_rate",
                                                            "bybit_oi_1h")],
        pl.lit(False).alias("delist_announced")).select(BARS_SCHEMA.names)
    w = pq.ParquetWriter(out / "bars_5m" / "2024.parquet", BARS_SCHEMA, **BARS_WRITE_OPTS)
    w.write_table(df.to_arrow().cast(BARS_SCHEMA))
    w.close()
    (out / "panel_stats.json").write_text(json.dumps({
        "years": {"2024": {}}, "rows": df.height, "size": {"lake_bytes_read": {}, "universe_bytes": 0},
        "build": {"gap_filled_rows": 0}}))
    cons = root / "verify_consensus.parquet"
    pl.DataFrame(list(consensus_rows), schema={"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64,
                                               "consensus": pl.Utf8}).write_parquet(cons)
    sc = root / "scorecard.json"
    sc.write_text(json.dumps({"metrics": [{"id": "M4", "detail": {"market_windows": []}}]}))
    return out, cons, sc


def _series(sym: str, n: int, override: dict | None = None) -> list[dict]:
    """n bars of one symbol on the 5m grid: close 1.0, mark 1.0. override[i] replaces bar i's columns."""
    override = override or {}
    rows = []
    for i in range(n):
        r = {"ts": _ts(i), "instrument_id": f"binance_um:{sym}:20231202", "symbol": sym}
        r.update(override.get(i, {}))
        rows.append(r)
    return rows


def _flag(root: Path, rows: list[dict], consensus_rows=()) -> pl.DataFrame:
    out, cons, sc = _stage_rows(root, rows, consensus_rows)
    panel._flag_pass(out, verdicts=root / "none.parquet", scorecard=sc, consensus=cons)
    return pl.read_parquet(out / "bars_5m" / "2024.parquet")


def test_single_print_bit_is_bit_24_and_threshold_is_50_bps():
    assert SP == 1 << 24 and THR == 0.005


def test_mark_lag_explained_bar_is_not_a_single_print(tmp_path):
    # bar 10 prints 2% above its own mark, and the next bar's mark has caught up to that print: a lagging mark
    rows = _series("AAAUSDT", 20, {10: {"close": DEV}, 11: {"close": DEV, "mark_close": DEV}})
    df = _flag(tmp_path, rows)
    r = df.filter(pl.col("ts") == _ts(10)).row(0, named=True)
    assert r["tradable"] is True and r["issue_flags"] & SP == 0


def test_bar_with_another_untradable_bit_gets_no_single_bit(tmp_path):
    # a consensus-refuted bar is untradable already: the base condition leaves it without the single-print bit
    rows = _series("AAAUSDT", 20, {10: {"close": DEV}})
    refuted = [{"symbol": "AAAUSDT", "start_ts": _ts(10), "end_ts": _ts(10), "consensus": "refuted"}]
    df = _flag(tmp_path, rows, refuted)
    r = df.filter(pl.col("ts") == _ts(10)).row(0, named=True)
    assert r["issue_flags"] & PANEL_BITS["consensus_refuted"] and r["issue_flags"] & SP == 0


def _reference_single_print(bars: dict) -> set:
    """Pure-python w-r5b2 definition over contiguous per-symbol series. bars[sym] = list of dicts in time order."""
    hits = set()
    for sym, seq in bars.items():
        n = len(seq)

        def off(b):
            real = b["volume"] > 0 and not b["filled"]
            has_mark = b["mark_close"] is not None and b["mark_close"] > 0
            return bool(real and has_mark and abs(b["close"] / b["mark_close"] - 1.0) > THR)

        offs = [off(b) for b in seq]
        for i in range(n):
            if not offs[i]:
                continue
            if (i > 0 and offs[i - 1]) or (i < n - 1 and offs[i + 1]):
                continue  # a run longer than one bar
            c = seq[i]["close"]
            lag = False
            for j in (i - 1, i + 1):
                if 0 <= j < n and seq[j]["mark_close"] is not None and seq[j]["mark_close"] > 0:
                    lag |= abs(c / seq[j]["mark_close"] - 1.0) <= THR
            if not lag:
                hits.add((sym, i))
    return hits


def test_single_print_matches_the_w_r5b2_definition_on_random_bars(tmp_path):
    rng = np.random.default_rng(20261022)
    n = 150
    rows, ref = [], {}
    for sym in ("AAAUSDT", "BBBUSDT"):
        seq = []
        mark = 1.0
        for i in range(n):
            mark = mark * (1 + rng.normal(0, 0.002))
            close = mark * (1 + (rng.normal(0, 0.012) if rng.random() < 0.15 else rng.normal(0, 0.001)))
            vol = 0.0 if rng.random() < 0.08 else float(rng.uniform(1, 9))
            filled = bool(rng.random() < 0.03)
            mk = None if rng.random() < 0.04 else mark
            b = {"ts": _ts(i), "instrument_id": f"binance_um:{sym}:20231202", "symbol": sym, "close": close,
                 "mark_close": mk, "volume": vol, "filled": filled}
            seq.append(b)
            rows.append({"ts": _ts(i), "instrument_id": b["instrument_id"], "symbol": sym, "close": close,
                         "mark_close": mk, "volume": vol, "filled": filled, "open": close, "high": close,
                         "low": close})
        ref[sym] = seq
    want = _reference_single_print(ref)
    assert 5 < len(want) < n * 2  # the sample has single prints and not everything
    df = _flag(tmp_path, rows)
    got = set()
    for sym in ("AAAUSDT", "BBBUSDT"):
        s = df.filter(pl.col("symbol") == sym).sort("ts")
        for i, f in enumerate(s["issue_flags"].to_list()):
            if f & SP:
                got.add((sym, i))
    assert got == want


# ---------------------------------------------------------------- delist notices with several instruments

def test_multi_instrument_delist_notice_flags_every_listed_instrument():
    """announcements.parquet joins instrument_ids with '|' (w-meta); a notice naming two instruments flags both."""
    from quant.data import panel_inputs as pin
    ann = pl.DataFrame({
        "action": ["delist", "delist", "delist"],
        "market": ["futures", "futures", "futures"],
        "instrument_map_status": ["mapped", "mapped", "mapped"],
        "instrument_ids": ["binance_um:AAAUSDT:20200101|binance_um:BBBUSDT:20200201",
                           "binance_um:CCCUSDT:20200301", "binance_um:DDDUSDT:20200401,binance_um:EEEUSDT:20200501"],
        "release_ts": [1_700_000_000_000_000_000, 1_710_000_000_000_000_000, 1_720_000_000_000_000_000],
    })
    t = pin.delist_table(ann).sort("instrument_id")
    got = dict(zip(t["instrument_id"].to_list(), t["delist_from"].to_list()))
    assert set(got) == {"binance_um:AAAUSDT:20200101", "binance_um:BBBUSDT:20200201", "binance_um:CCCUSDT:20200301",
                        "binance_um:DDDUSDT:20200401", "binance_um:EEEUSDT:20200501"}
    assert got["binance_um:BBBUSDT:20200201"] == 1_700_000_000_000_000_000
    assert got["binance_um:CCCUSDT:20200301"] == 1_710_000_000_000_000_000


def test_delist_flag_turns_on_at_the_notice_for_each_id_of_a_pipe_joined_notice():
    from quant.data import panel_inputs as pin
    ann = pl.DataFrame({
        "action": ["delist"], "market": ["futures"], "instrument_map_status": ["mapped"],
        "instrument_ids": ["binance_um:AAAUSDT:20200101|binance_um:BBBUSDT:20200201"],
        "release_ts": [1_000],
    })
    t = pin.delist_table(ann)
    ts = pl.Series([999, 1_000, 2_000])
    assert pin.delist_column(ts, pl.Series([t["delist_from"][0]] * 3)).to_list() == [False, True, True]
