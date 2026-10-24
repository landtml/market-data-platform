"""Round 5 panel rules: zero open interest is null, single-bar prints are untradable (price kept), REST minutes.

The single_bar_print bit is a data-cleaning flag: it reads bar t+5m to decide whether bar t is tradable. These tests
check the rule, the exemptions, and that no feature module reads the bit.
"""

import json
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
from quant.data import panel, panel_inputs as pin
from quant.data.panel import BARS_SCHEMA, BARS_WRITE_OPTS, PANEL_BITS, SINGLE_PRINT_BPS

BAR = panel.BAR_NS
T0 = 1_704_067_200_000_000_000  # 2024-01-01 00:00 UTC
N = 40
SYMS = ("AAAUSDT", "BBBUSDT")
REPO = Path(__file__).resolve().parents[2]


def _stage(root: Path, overrides: dict, consensus_rows: list[dict]):
    """A staged set of two symbols, N bars each. overrides[(sym, i)] = {col: value} replaces a bar's columns."""
    out = root / ".staging-test"
    (out / "bars_5m").mkdir(parents=True)
    rows = []
    for sym in SYMS:
        for i in range(N):
            r = {"ts": T0 + i * BAR, "instrument_id": f"binance_um:{sym}:20200101", "symbol": sym,
                 "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0, "quote_volume": 1.0,
                 "taker_buy_quote_volume": 1.0, "trades": 1, "mark_close": 1.0, "premium_close": 1.0,
                 "funding_rate": 0.0, "funding_interval_h": 8, "filled": False, "tradable": True,
                 "issue_flags": 0}
            r.update(overrides.get((sym, i), {}))
            rows.append(r)
    df = pl.DataFrame(rows).sort(["ts", "instrument_id"])
    df = df.with_columns(
        *[pl.lit(None, dtype=pl.Float64).alias(c) for c in ("oi_sum_open_interest", "oi_sum_open_interest_value",
                                                            "index_close", "spot_close", "bybit_funding_rate",
                                                            "bybit_oi_1h")],
        pl.lit(False).alias("delist_announced"))
    w = pq.ParquetWriter(out / "bars_5m" / "2024.parquet", BARS_SCHEMA, **BARS_WRITE_OPTS)
    w.write_table(df.to_arrow().cast(BARS_SCHEMA))
    w.close()
    (out / "panel_stats.json").write_text(json.dumps({
        "years": {"2024": {}}, "rows": df.height, "size": {"lake_bytes_read": {}, "universe_bytes": 0},
        "build": {"gap_filled_rows": 0}}))
    cons = root / "verify_consensus.parquet"
    pl.DataFrame(consensus_rows, schema={"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64,
                                         "consensus": pl.Utf8}).write_parquet(cons)
    sc = root / "scorecard.json"
    sc.write_text(json.dumps({"metrics": [{"id": "M4", "detail": {"market_windows": []}}]}))
    return out, cons, sc


def _run(tmp_path, overrides=None, consensus_rows=()):
    out, cons, sc = _stage(tmp_path, overrides or {}, list(consensus_rows))
    panel._flag_pass(out, verdicts=tmp_path / "no_verdicts.parquet", scorecard=sc, consensus=cons)
    return pl.read_parquet(out / "bars_5m" / "2024.parquet").select(
        ["symbol", "ts", "close", "tradable", "issue_flags"])


def _at(df, sym, i):
    r = df.filter((pl.col("symbol") == sym) & (pl.col("ts") == T0 + i * BAR)).row(0, named=True)
    return r


SP = PANEL_BITS["single_bar_print"]
DEV = 1 + 2 * SINGLE_PRINT_BPS / 1e4  # a close 100 bps off its mark (well beyond the 50 bps threshold)


def test_threshold_constant_is_50_bps():
    assert SINGLE_PRINT_BPS == 50.0 and SP == 1 << 24


def test_single_reverting_print_is_untradable_and_price_is_kept(tmp_path):
    df = _run(tmp_path, {("AAAUSDT", 10): {"close": DEV}})
    r = _at(df, "AAAUSDT", 10)
    assert r["tradable"] is False and r["issue_flags"] & SP
    assert r["close"] == DEV  # price never replaced
    assert _at(df, "AAAUSDT", 9)["tradable"] is True and _at(df, "AAAUSDT", 11)["tradable"] is True
    assert _at(df, "BBBUSDT", 10)["tradable"] is True


def test_sustained_divergence_is_kept(tmp_path):
    df = _run(tmp_path, {("AAAUSDT", 10): {"close": DEV}, ("AAAUSDT", 11): {"close": DEV}})
    for i in (10, 11):
        r = _at(df, "AAAUSDT", i)
        assert r["tradable"] is True and r["issue_flags"] & SP == 0


def test_neighbour_outside_threshold_is_not_a_single_print(tmp_path):
    # bar 9 is itself 100 bps off: bar 10 is not a one-bar excursion, and bar 9 has a neighbour off too
    df = _run(tmp_path, {("AAAUSDT", 9): {"close": DEV}, ("AAAUSDT", 10): {"close": DEV}})
    assert _at(df, "AAAUSDT", 10)["tradable"] is True
    assert _at(df, "AAAUSDT", 9)["tradable"] is True


def test_missing_neighbour_mark_does_not_block_the_rule(tmp_path):
    # fix wave F (w-r5b2 definition): a neighbour counts against the print only when it is itself off (> 50 bps from
    # its own mark). A neighbour with no mark is not off, so bar 10 is a single-bar print.
    df = _run(tmp_path, {("AAAUSDT", 10): {"close": DEV}, ("AAAUSDT", 11): {"mark_close": None}})
    assert _at(df, "AAAUSDT", 10)["tradable"] is False


def test_confirmed_window_exempts_the_print(tmp_path):
    # lead ruling 2026-10-22: the exemption is on (the published rule)
    rows = [{"symbol": "AAAUSDT", "start_ts": T0 + 9 * BAR, "end_ts": T0 + 11 * BAR, "consensus": "confirmed"}]
    df = _run(tmp_path / "c", {("AAAUSDT", 10): {"close": DEV}}, rows)
    assert _at(df, "AAAUSDT", 10)["tradable"] is True and _at(df, "AAAUSDT", 10)["issue_flags"] & SP == 0


def test_confirmed_window_does_not_exempt_the_print_when_the_exemption_is_off(tmp_path, monkeypatch):
    monkeypatch.setattr(panel, "SINGLE_PRINT_CONFIRMED_EXEMPT", False)
    rows = [{"symbol": "AAAUSDT", "start_ts": T0 + 9 * BAR, "end_ts": T0 + 11 * BAR, "consensus": "confirmed"}]
    df = _run(tmp_path / "c2", {("AAAUSDT", 10): {"close": DEV}}, rows)
    r = _at(df, "AAAUSDT", 10)
    assert r["tradable"] is False and r["issue_flags"] & SP


def test_disputed_window_does_not_exempt_the_print(tmp_path):
    rows = [{"symbol": "AAAUSDT", "start_ts": T0 + 9 * BAR, "end_ts": T0 + 11 * BAR, "consensus": "disputed"}]
    df = _run(tmp_path / "d", {("AAAUSDT", 10): {"close": DEV}}, rows)
    r = _at(df, "AAAUSDT", 10)
    assert r["tradable"] is False and r["issue_flags"] & SP


def test_filled_and_zero_volume_bars_are_not_candidates(tmp_path):
    df = _run(tmp_path / "f", {("AAAUSDT", 10): {"close": DEV, "filled": True},
                               ("BBBUSDT", 10): {"close": DEV, "volume": 0.0}})
    assert _at(df, "AAAUSDT", 10)["issue_flags"] & SP == 0
    assert _at(df, "BBBUSDT", 10)["issue_flags"] & SP == 0


def test_rule_is_idempotent(tmp_path):
    out, cons, sc = _stage(tmp_path, {("AAAUSDT", 10): {"close": DEV}}, [])
    ver = tmp_path / "no_verdicts.parquet"
    panel._flag_pass(out, verdicts=ver, scorecard=sc, consensus=cons)
    first = pl.read_parquet(out / "bars_5m" / "2024.parquet")
    panel._flag_pass(out, verdicts=ver, scorecard=sc, consensus=cons)
    assert first.equals(pl.read_parquet(out / "bars_5m" / "2024.parquet"))


def test_only_the_untradable_bit_and_tradable_change(tmp_path):
    """Setting the bit moves tradable and nothing else: close, mark and the other bits are unchanged."""
    out, cons, sc = _stage(tmp_path / "x", {("AAAUSDT", 10): {"close": DEV}}, [])
    before = pl.read_parquet(out / "bars_5m" / "2024.parquet")
    panel._flag_pass(out, verdicts=tmp_path / "no_verdicts.parquet", scorecard=sc, consensus=cons)
    after = pl.read_parquet(out / "bars_5m" / "2024.parquet")
    assert before["close"].to_list() == after["close"].to_list()
    assert before["mark_close"].to_list() == after["mark_close"].to_list()
    changed = before.with_columns(pl.col("issue_flags") ^ after["issue_flags"]).filter(pl.col("issue_flags") != 0)
    assert changed.height == 1 and changed["issue_flags"][0] == SP


def test_no_feature_module_reads_the_single_print_bit():
    """Causality: the bit uses bar t+5m, so only the panel builder and the cleaning docs may name it."""
    # quality.py is the scorecard: it reports M6 from the bit (a metric, not a feature input). Strategies and features
    # may not name the bit or issue_flags.
    allowed = {REPO / "libs/data/quant/data/panel.py", REPO / "libs/data/quant/data/panel_inputs.py",
               REPO / "libs/data/quant/data/quality.py"}
    hits = []
    for f in (REPO / "libs").rglob("*.py"):
        if f in allowed or "finlab" in f.parts:
            continue
        text = f.read_text(errors="ignore")
        if "single_bar_print" in text or "SINGLE_BIT" in text or "issue_flags" in text:
            hits.append(str(f))
    assert hits == []


def test_zero_open_interest_is_null_per_column():
    ts = [T0 + i * BAR for i in range(4)]
    m = pl.DataFrame({"ts": ts, "sum_open_interest": [0.0, 2.0, 0.0, 4.0],
                      "sum_open_interest_value": [5.0, 0.0, 0.0, 9.0]})
    rows = pin.oi_rows(m, "X").sort("ts")
    assert rows["oi_sum_open_interest"].to_list() == [None, 2.0, None, 4.0]
    assert rows["oi_sum_open_interest_value"].to_list() == [5.0, None, None, 9.0]


def test_rest_minutes_under_an_archive_symbol_are_read(tmp_path):
    """AERGOUSDT-style: archive 1m exists, REST 1m for the same symbol holds a later traded minute -> end moves."""
    lake = tmp_path / "lake"
    arch = lake / "klines" / "1m" / "AAAUSDT"
    rest = lake / "rest" / "klines" / "1m" / "AAAUSDT"
    arch.mkdir(parents=True)
    rest.mkdir(parents=True)
    pl.DataFrame({"ts": [T0, T0 + 60_000_000_000], "volume": [1.0, 0.0]}).write_parquet(arch / "2025-04.parquet")
    pl.DataFrame({"ts": [T0 + 120_000_000_000], "volume": [3.0]}).write_parquet(rest / "2025-04.parquet")
    end, src = panel._last_nonzero_end_1m(lake, "AAAUSDT")
    assert src == "1m" and end == T0 + BAR  # the REST minute at +2 min closes the 5m bar opened at T0


def test_rest_only_symbol_without_archive_1m_keeps_the_5m_rule(tmp_path):
    lake = tmp_path / "lake"
    rest = lake / "rest" / "klines" / "1m" / "BBBUSDT"
    rest.mkdir(parents=True)
    pl.DataFrame({"ts": [T0 + 600_000_000_000], "volume": [3.0]}).write_parquet(rest / "2025-04.parquet")
    assert panel._last_nonzero_end_1m(lake, "BBBUSDT") == (None, "5m")


def test_single_print_window_size_does_not_matter(tmp_path, monkeypatch):
    """The windowed pass must equal a single window: neighbours across window and year edges are read."""
    overrides = {("AAAUSDT", 10): {"close": DEV}, ("AAAUSDT", 11): {"close": DEV},
                 ("AAAUSDT", 20): {"close": DEV}, ("BBBUSDT", 21): {"close": DEV}}
    out, cons, sc = _stage(tmp_path / "w", overrides, [])
    files = sorted((out / "bars_5m").glob("*.parquet"))
    conf = pl.DataFrame(schema={"symbol": pl.Utf8, "start": pl.Int64, "end": pl.Int64})
    big = panel._single_print_candidates(files, conf, conf, window_ns=10**18)[0]
    small = panel._single_print_candidates(files, conf, conf, window_ns=3 * BAR)[0]
    assert big["nb"] == small["nb"] and big["confirmed"] == small["confirmed"]
    assert (big["cand"] == small["cand"]).all()
    assert big["cand"].sum() == 2  # bars AAA 20 and BBB 21 are single prints; AAA 10-11 are sustained
