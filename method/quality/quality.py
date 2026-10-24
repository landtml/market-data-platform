"""Data-quality scorecard for the lake: the instrument of the curation loop.

    python -m quant.data.quality [--out-dir DIR]

Each run writes scorecard_<UTC>.json, scorecard_latest.json, scorecard_latest.md and suspicious_windows.parquet
(the verifier's work queue, top 2000 by magnitude) under data/curated/quality/.

Inputs are read only where the lake has finished: a klines unit counts when its line is in _done/<stage>.jsonl and
its files exist. Nothing under data/quarantine/ is read. A metric whose input is absent is reported as not_measured
with the reason. Thresholds are provisional; each metric carries the rule it was judged against.

The 3-venue consensus table (M7) is data/lake/verify_consensus.parquet, one row per suspicious window, with the
consensus in {confirmed, refuted, disputed, undecided}. Licence and compliance (M14) is waived by decision
(2026-10-20): the evidence is still computed and kept in the detail, the status is "waived".
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import re
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
from quant.core import flag_bits, paths, validate_bars

INV_FAMILY = "um_m_klines_1m"
INV_MONTH_RE = r"-(\d{4}-\d{2})(?:-\d{2})?\.zip$"
MIN_NS = 60 * 1_000_000_000
FIVE_MIN_NS = 5 * MIN_NS
QUEUE_CAP = 2000

TARGETS = {
    "M1": "coverage: green >= 0.99, amber >= 0.90 (share of inventory symbol-months; delisted perps shown apart)",
    "M2": "checksum: green = 1.0, amber >= 0.999 (share of lake files with a valid sha256 line)",
    "M3a": "hard rules inside live ranges of instruments with exclude_reason null, symbols with 1m data "
           "(nan, nonpositive, ohlc_bad, dupes, unsorted, misaligned, missing): target 0, red if > 0",
    "M3b": "soft rules per 1M bars inside live ranges: zero_vol > 1000, stuck > 100, spike > 50 (amber)",
    "M4": "5m zero-volume share inside live range: green <= 0.5%, amber <= 2%, red > 2%; "
          "outage-window hit rate: green >= 0.8, amber >= 0.5",
    "M5": "5m bars after last nonzero volume in delisted perps: amber when any (trim in panel build)",
    "M6": "share of traded 5m bars with |close/mark - 1| > 50 bps: green <= 0.1%, amber <= 0.5%, red > 0.5%",
    "M7": "3-venue consensus: decided share = (confirmed + refuted) / all suspicious windows: green >= 0.80, "
          "amber >= 0.60, else red; disputed and undecided reported; windows no venue lists are unverifiable",
    "M8": "funding: share of expected events missing: green <= 0.1%, red above 0.1% or sign sanity fails; "
          "per-regime and per-change-date completeness; cap breaches unverifiable (snapshot only)",
    "M9": "OI: share of days with completeness < 0.9: green <= 1%, amber <= 5%, red > 5%",
    "M10": "unresolved mapping items: red if any onboard_ts is null, amber if any remain, green otherwise",
    "M11": "look-ahead / PIT pytest files: green when all pass, red on any failure",
    "M12": "cost calibration: every (year, size band) cell of 2020-2025 with eff data has charged spread coverage "
           ">= 0.65 on the eff proxy (coverage after era multipliers); green when all cells meet it, amber otherwise",
    "M13": "context cells older than the source cadence: green <= 1%, amber <= 5%, red > 5%",
    "M14": "compliance: waived by owner rule 2026-10-20 (terms and licences are not weighed for local research); "
           "the feed-by-feed evidence is kept in the detail and does not enter the counts or the worst items",
    "M15": "mark 5m coverage: internal holes / grid slots (leading and trailing reported, not scored): "
           "green = 0, amber <= 0.1%, red > 0.1%",
    "M16": "premium-index 5m coverage: internal holes / grid slots (leading and trailing reported, not scored): "
           "green = 0, amber <= 0.1%, red > 0.1%",
}

INPUTS = {
    "M1": "inventory tsv + lake _done", "M2": "CHECKSUMS.tsv + lake _done",
    "M3a/M3b": "lake/klines/1m archive months + lake/rest/klines/1m minutes the archive lacks (symbols with an "
                 "archive 1m directory); exchange gaps from lake/rest/_missing/klines_1m.parquet",
    "M4/M5/M6": "published panel (curated/current/bars_5m): volume, filled, tradable, mark_close, close",
    "M8": "lake/funding archive + lake/rest/funding tail + exchange gaps from lake/rest/_missing/funding.parquet",
    "M9": "lake/metrics/completeness + rest/metrics overlay (round 5)",
    "M15/M16": "lake/<family>/5m archive + lake/rest/<family>/5m; exchange gaps from rest/_missing/<family>.parquet",
    "M7/M11/M12/M13/M14": "unchanged inputs (consensus table, tests, era detail, context, licences)",
    "suspicious queue": "lake/klines/1m flags (no _issues cache filter) + panel M4 windows + panel M6 bad prints",
}

# Expected maximum age (days) of a context value: cadence plus publication lag plus weekend slack.
CONTEXT_EXPECTED_AGE = {"fred_daily": 4, "fred_weekly": 9, "deribit": 2, "alternative_me": 2,
                        "defillama": 2, "coinmetrics": 3}
CONTEXT_PREFIX_SOURCE = {"fred_": "fred", "dvol_": "deribit", "fng_": "alternative_me", "stable_": "defillama",
                         "cm_": "coinmetrics"}
CURATED_FILE_SOURCE = {"instruments.parquet": ["binance_um", "binance_rest"], "instrument_events.parquet": ["binance_um"],
                       "fee_schedule.parquet": ["binance_um"], "universe_pit.parquet": ["binance_um"],
                       "issues.parquet": ["binance_um"], "spread_obs_5m.parquet": ["binance_um"],
                       "spread_obs_tardis.parquet": ["tardis"], "mcap_rank_weekly.parquet": ["cmc"]}
LICENSE_ALIASES = {"binance_um": ["binance"], "fred": ["fred", "st. louis", "st louis"],
                   "deribit": ["deribit"], "alternative_me": ["alternative.me", "alternative_me", "alternative me"],
                   "defillama": ["defillama", "defi llama"], "coinmetrics": ["coin metrics", "coinmetrics"],
                   "cmc": ["cmc", "coinmarketcap"], "coinbase": ["coinbase"], "tardis": ["tardis"],
                   "kraken": ["kraken"], "binance_rest": ["binance rest", "fapi"]}

# Impact on research validity: 0 is a gate; then coverage, bad prices and survivorship; funding; costs; OI/context.
IMPACT = {"M11": 0, "M14": 0, "M1": 1, "M2": 1, "M3a": 1, "M3b": 2, "M4": 1, "M5": 1, "M6": 1, "M7": 1,
          "M10": 1, "M15": 1, "M8": 2, "M16": 2, "M12": 3, "M9": 4, "M13": 4}

FIXABLE_ORDER = {"free-source": 0, "code": 1, "owner-decision": 2, "paid-only": 3}
STATUS_ORDER = {"red": 0, "not_measured": 1, "amber": 2, "green": 3, "proven": 4, "waived": 5}
WAIVED = "waived"
# proven: red/amber, but shown unfixable from free sources by evidence on disk (configs/quality_proofs.json). A proof
# holds only while the value stays at or below the proven max_value and its evidence file exists; a regression or a
# missing evidence file reopens the metric (ultra-premium definition, ledger 2026-10-21).
PROVEN = "proven"


@dataclass(frozen=True)
class Sources:
    """Every input the scorecard reads. Tests point these at a temporary tree."""

    inventory: Path = paths.SAMPLES / "s-binance" / "tmp" / "inv_um.tsv"
    checksums: Path = paths.RAW / "binance" / "CHECKSUMS.tsv"
    lake: Path = paths.LAKE / "binance_um"
    lake_root: Path = paths.LAKE
    rest: Path | None = None  # REST overlay root (rest/_missing, rest/klines/1m, ...); None = lake / 'rest'
    panel: Path = paths.CURATED / "current" / "bars_5m"  # the published panel: research's 5m bars (M4, M5, M6)
    kraken_verify: Path = paths.LAKE / "kraken" / "verify"  # unused since M7 reads the consensus table
    verify_consensus: Path = paths.LAKE / "verify_consensus.parquet"
    era_detail: Path = paths.ROOT / "configs" / "cost_era_mult_detail.json"
    universe_pit: Path | None = None  # M4 scope: the eligible (top 100) bars; None = universe_pit.parquet beside panel
    instruments: Path = paths.CURATED / "instruments.parquet"
    events: Path = paths.CURATED / "instrument_events.parquet"
    curated: Path = paths.CURATED
    context: Path = paths.CURATED / "context_daily.parquet"
    context_sources: Path = paths.CURATED / "context_sources.json"
    spreads: Path = paths.CURATED / "spread_obs_5m.parquet"
    fund_info: Path = paths.SAMPLES / "s-binance" / "tmp" / "fundingInfo.json"
    licenses: Path = paths.ROOT / "docs" / "DATA_LICENSES.md"
    proofs: Path = paths.ROOT / "configs" / "quality_proofs.json"
    out_dir: Path = paths.CURATED / "quality"
    # M11 runs every point-in-time / look-ahead test (the old two-file list pointed at paths gone since the restructure)
    tests: tuple = tuple(sorted((paths.ROOT / "tests" / "bias_checks").glob("test_*.py")))
    test_cwd: Path = paths.ROOT


@dataclass
class Metric:
    id: str
    name: str
    value: float | None
    unit: str
    target: str
    status: str
    evidence: str
    fixable_by: str
    fix: str = ""
    reason: str = ""
    detail: dict = field(default_factory=dict)


def grade(value: float, green: float, amber: float, *, higher_is_better: bool = False) -> str:
    if higher_is_better:
        return "green" if value >= green else "amber" if value >= amber else "red"
    return "green" if value <= green else "amber" if value <= amber else "red"


def worst(*statuses: str) -> str:
    return min(statuses, key=STATUS_ORDER.get)


def _rel(p: Path) -> str:
    try:
        return str(p.relative_to(paths.ROOT))
    except ValueError:
        return str(p)


def _metric(mid: str, name: str, value, unit: str, status: str, evidence: Path, fixable: str, fix: str = "",
            reason: str = "", detail: dict | None = None) -> Metric:
    return Metric(mid, name, value, unit, TARGETS[mid], status, _rel(evidence), fixable, fix, reason,
                  detail or {})


def _not(mid: str, name: str, unit: str, evidence: Path, fixable: str, reason: str, fix: str) -> Metric:
    return _metric(mid, name, None, unit, "not_measured", evidence, fixable, fix=fix, reason=reason)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- lake reading (done markers only)

def read_done(lake: Path, stage: str) -> list[str]:
    """Unit ids whose line is in _done/<stage>.jsonl. A torn last line (the pipeline is appending) is skipped."""
    path = lake / "_done" / f"{stage}.jsonl"
    if not path.exists():
        return []
    units = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            units.append(json.loads(line)["unit"])
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
    return list(dict.fromkeys(units))


def _split_unit(unit: str) -> tuple[str, str]:
    sym, _, month = unit.partition("|")
    return sym, month


def family_units(lake: Path, stage: str, family: str) -> list[tuple[str, str]]:
    """(symbol, month) of finished 5m units of a family whose file is present."""
    out = []
    for unit in read_done(lake, stage):
        sym, month = _split_unit(unit)
        if month and (lake / family / "5m" / sym / f"{month}.parquet").exists():
            out.append((sym, month))
    return out


def klines_units(src: Sources) -> list[tuple[str, str]]:
    """(symbol, month) of finished klines units whose 5m file exists. 1m may be pruned by ever_liquid (5m is kept)."""
    out = []
    for sym, month in map(_split_unit, read_done(src.lake, "klines_1m")):
        if month and (src.lake / "klines" / "5m" / sym / f"{month}.parquet").exists():
            out.append((sym, month))
    return out


def ever_liquid_summary(src: Sources) -> dict | None:
    path = src.lake / "ever_liquid.parquet"
    if not path.exists():
        return None
    d = pl.read_parquet(path, columns=["symbol", "keep_1m"])
    keep = int(d["keep_1m"].sum())
    return {"symbols": d.height, "keep_1m": keep, "pruned_1m": d.height - keep}


def gapfill_summary(src: Sources, stage: str) -> dict | None:
    """Totals of the daily gap-fill log _issues/gapfill_daily_<stage>.parquet, or None before it exists."""
    path = src.lake / "_issues" / f"gapfill_daily_{stage}.parquet"
    if not path.exists():
        return None
    d = pl.read_parquet(path)
    cols = ["days_missing", "minutes_missing_before", "minutes_filled", "minutes_missing_after",
            "days_fetched", "days_unavailable", "dupes_conflict"]
    out = {"rows": d.height, "symbols": d["symbol"].n_unique(), "months": d["month"].n_unique()}
    out.update({c: int(d[c].sum()) for c in cols})
    return out


def _paths(base: Path, units: list[tuple[str, str]]) -> list[str]:
    return [str(base / sym / f"{month}.parquet") for sym, month in units]


def _scan(files: list[str]) -> pl.LazyFrame:
    """Scan parquet files; the symbol is the parent directory name."""
    return (pl.scan_parquet(files, include_file_paths="__src")
            .with_columns(pl.col("__src").str.split("/").list.get(-2).alias("sym")))


def read_inventory(src: Sources) -> pl.DataFrame | None:
    if not src.inventory.exists():
        return None
    df = pl.read_csv(src.inventory, separator="\t", infer_schema=False)
    return (df.filter((pl.col("family") == INV_FAMILY) & pl.col("key").str.ends_with(".zip"))
            .with_columns(pl.col("key").str.extract(INV_MONTH_RE, 1).alias("month"))
            .filter(pl.col("month").is_not_null())
            .select(pl.col("symbol"), pl.col("month")).unique())


def read_instruments(src: Sources) -> pl.DataFrame | None:
    if not src.instruments.exists():
        return None
    cols = ["symbol", "contract_type", "status_now", "exclude_reason", "has_archive", "onboard_ts"]
    cols += [c for c in ("listed_end_ts",) if c in pq.read_schema(src.instruments).names]
    return pl.read_parquet(src.instruments, columns=cols)


def delisted_perps(ins: pl.DataFrame) -> set[str]:
    return set(ins.filter((pl.col("contract_type") == "PERPETUAL")
                          & pl.col("status_now").is_in(["SETTLING", "ABSENT"]))["symbol"].to_list())


def live_perps(ins: pl.DataFrame) -> set[str]:
    return set(ins.filter((pl.col("contract_type") == "PERPETUAL")
                          & pl.col("status_now").is_in(["TRADING", "PENDING_TRADING"]))["symbol"].to_list())


def lake_completeness(src: Sources, units: list[tuple[str, str]]) -> dict:
    progress = _load_json(src.lake / "_progress.json") if (src.lake / "_progress.json").exists() else {}
    done = set(units)
    info = {"symbol_months_done": len(done), "symbols_done": len({s for s, _ in done}),
            "progress_updated": progress.get("updated"), "stop_reason": progress.get("stop_reason"),
            "stage_partial": progress.get("stop_reason") is None}
    inv = read_inventory(src)
    if inv is None:
        return {**info, "symbol_months_inventory": None, "symbols_inventory": None, "fraction": None}
    inv_pairs = set(map(tuple, inv.rows()))
    info.update({"symbol_months_inventory": len(inv_pairs), "symbols_inventory": inv["symbol"].n_unique(),
                 "fraction": len(done & inv_pairs) / max(len(inv_pairs), 1),
                 "lake_units_outside_inventory": len(done - inv_pairs)})
    return info


# ---------------------------------------------------------------- M1 coverage, M2 checksum

def m1_coverage(src: Sources, units, ins: pl.DataFrame | None) -> Metric:
    name = "coverage: lake symbol-months / archive inventory"
    inv = read_inventory(src)
    if inv is None:
        return _not("M1", name, "share", src.inventory, "free-source", "inventory missing",
                    "rebuild the archive inventory")
    inv_pairs = set(map(tuple, inv.rows()))
    done = set(units) & inv_pairs
    overall = len(done) / len(inv_pairs)
    status = grade(overall, 0.99, 0.90, higher_is_better=True)
    detail = {"symbol_months_inventory": len(inv_pairs), "symbol_months_in_lake": len(done),
              "lake_units_outside_inventory": len(set(units) - inv_pairs)}
    if ins is not None:
        for label, group in (("delisted_perp", delisted_perps(ins)), ("live_perp", live_perps(ins))):
            pairs = {p for p in inv_pairs if p[0] in group}
            got = done & pairs
            share = len(got) / len(pairs) if pairs else None
            detail.update({f"{label}_inventory": len(pairs), f"{label}_in_lake": len(got),
                           f"{label}_coverage": share})
            if share is not None:
                status = worst(status, grade(share, 0.99, 0.90, higher_is_better=True))
    fix = (f"finish the klines_1m download: {len(done)} of {len(inv_pairs)} inventory symbol-months are in the lake; "
           "rerun b-pipe stage klines_1m")
    return _metric("M1", name, round(overall, 6), "share", status, src.inventory, "free-source", fix=fix,
                   detail=detail)


def m2_checksum(src: Sources, units) -> Metric:
    name = "checksum: lake files with a sha256 line in CHECKSUMS.tsv"
    if not src.checksums.exists():
        return _not("M2", name, "share", src.checksums, "code", "CHECKSUMS.tsv missing", "record sha256 for zips")
    if not units:
        return _not("M2", name, "share", src.checksums, "code", "no finished klines units", "wait for klines_1m")
    cs = pl.read_csv(src.checksums, separator="\t", has_header=False, infer_schema=False,
                     new_columns=["key", "sha256", "size", "time"], truncate_ragged_lines=True)
    have = set(cs.filter(pl.col("sha256").str.contains(r"^[0-9a-f]{64}$"))["key"].to_list())
    keys = [f"data/futures/um/monthly/klines/{s}/1m/{s}-1m-{m}.zip" for s, m in units]
    missing = [k for k in keys if k not in have]
    value = 1 - len(missing) / len(keys)
    return _metric("M2", name, round(value, 6), "share", grade(value, 1.0, 0.999, higher_is_better=True),
                   src.checksums, "code", fix=f"record the sha256 for {len(missing)} lake units without a line",
                   detail={"lake_files": len(keys), "without_checksum": len(missing),
                           "first_missing": missing[:20]})


# ---------------------------------------------------------------- REST overlays and exchange gaps

# REST answered empty (or has no coverage there) and the archive has no row: the w-gapfill classification. The
# endpoint refusing the symbol (endpoint_error) is a defect that a free source could still fix.
REST_EXCHANGE_REASONS = ("absent_from_endpoint", "endpoint_no_rows_for_symbol", "endpoint_no_rows_in_window",
                         "before_endpoint_coverage")
_EMPTY_TS = np.empty(0, dtype=np.int64)


def _rest_root(src: Sources) -> Path:
    return src.rest if src.rest is not None else src.lake / "rest"


def rest_ledger(src: Sources, family: str) -> pl.DataFrame:
    """rest/_missing/<family>.parquet: one row per slot or minute the REST stage left empty (symbol, ts, reason)."""
    p = _rest_root(src) / "_missing" / f"{family}.parquet"
    if not p.exists():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "ts": pl.Int64, "reason": pl.Utf8})
    return pl.read_parquet(p, columns=["symbol", "ts", "reason"]).with_columns(pl.col("ts").cast(pl.Int64))


def exchange_gaps(led: pl.DataFrame) -> pl.DataFrame:
    """The ledger rows that are exchange gaps: no archive row and an empty REST answer."""
    return led.filter(pl.col("reason").is_in(REST_EXCHANGE_REASONS))


# ---------------------------------------------------------------- M3 validator rules, inside live ranges

HARD_RULES = ("nan", "nonpositive", "ohlc_bad", "dupes", "unsorted", "misaligned", "missing")
SOFT_AMBER_PER_1M = {"zero_vol": 1000.0, "stuck": 100.0, "spike": 50.0}
STUCK_RUN = 30  # identical closes in a row: the validator's stuck limit
M1_VALUE_COLS = ("open", "high", "low", "close", "volume")


def in_scope(ins: pl.DataFrame | None) -> set[str] | None:
    """Symbols with an instrument row whose exclude_reason is null; None when the master is missing."""
    if ins is None:
        return None
    return set(ins.filter(pl.col("exclude_reason").is_null())["symbol"].to_list())


def _load_1m(path: Path, month: str, *, rest: bool) -> pl.DataFrame:
    """One month of 1m bars: Float64 values (nan for null), flags (archive validator bits; 0 for REST minutes)."""
    d = pl.read_parquet(path, columns=["ts", *M1_VALUE_COLS, *([] if rest else ["flags"])])
    flags = pl.lit(0, dtype=pl.Int64) if rest else pl.col("flags").cast(pl.Int64).fill_null(0)
    return d.select(
        pl.col("ts").cast(pl.Int64),
        *[pl.col(c).cast(pl.Float64).fill_null(float("nan")) for c in M1_VALUE_COLS],
        flags.alias("flags"), pl.lit(month).alias("month"), pl.lit(rest).alias("rest_added"))


def _merged_1m(arch: list[tuple[str, Path]], rest: list[tuple[str, Path]]) -> pl.DataFrame | None:
    """One symbol's 1m bars: every archive month, plus the REST minutes the archive lacks in that month. Called only for
    a symbol with an archive 1m directory (the panel rule: a 5m-only symbol keeps its 5m rule). A month that gains REST
    minutes gets its validator flags recomputed with quant.core.validate_bars, as the gap-fill merge does; other months
    keep their archive flags. Rows are in ts order; column rest_added marks the REST minutes."""
    fa, fr = dict(arch), dict(rest)
    parts: list[pl.DataFrame] = []
    for month in sorted(set(fa) | set(fr)):
        a = _load_1m(fa[month], month, rest=False) if month in fa else None
        r = _load_1m(fr[month], month, rest=True) if month in fr else None
        if r is not None and a is not None:
            r = r.join(a.select("ts"), on="ts", how="anti")
        if r is None or r.height == 0:
            if a is not None:
                parts.append(a)
            continue
        m = (r if a is None else pl.concat([a, r])).sort("ts")
        res = validate_bars(m["ts"].to_numpy(), m["open"].to_numpy(), m["high"].to_numpy(), m["low"].to_numpy(),
                            m["close"].to_numpy(), m["volume"].to_numpy(), MIN_NS, 30)
        parts.append(m.with_columns(pl.Series("flags", np.asarray(res["flags"], dtype=np.int64))))
    return pl.concat(parts) if parts else None


def _exchange_per_bar(ts: np.ndarray, gaps: np.ndarray, inr: np.ndarray, exch_ts: np.ndarray) -> np.ndarray:
    """Per bar: how many of the missing minutes before it are exchange gaps (their minute is in the ledger)."""
    out = np.zeros(ts.size, dtype=np.int64)
    bars = np.flatnonzero((gaps > 0) & inr)
    if bars.size == 0 or exch_ts.size == 0:
        return out
    cnt = gaps[bars]
    rep = np.repeat(bars, cnt)
    first = np.cumsum(cnt) - cnt
    k = np.arange(rep.size) - np.repeat(first, cnt) + 1
    missing_ts = ts[rep - 1] + k * MIN_NS
    hit = np.isin(missing_ts, exch_ts)
    return out + np.bincount(rep[hit], minlength=ts.size)


def _m3_counts(d: pl.DataFrame, exch_ts: np.ndarray, spans: list[tuple[int, int]] | None = None):
    """Rule counts per month of one symbol's merged 1m frame, inside its live range (up to the last nonzero-volume
    minute) and inside one of its in-scope listings (spans of [onboard_ts, listed_end_ts); None = no listing limit), so
    the dead period of a reused ticker and an excluded later listing are not scored. Gaps and the live end cross month
    edges; runs do not. Returns (counts per month, bars per month, month labels, REST minutes per month), or None
    without a live range."""
    ts = d["ts"].to_numpy()
    o, h, lo, c, v = (d[k].to_numpy() for k in M1_VALUE_COLS)
    flags = d["flags"].to_numpy()
    listed = np.ones(ts.size, dtype=bool)
    if spans is not None:
        listed = np.zeros(ts.size, dtype=bool)
        for a, b in spans:
            listed |= (ts >= a) & (ts < b)
    if not ((v > 0) & listed).any():
        return None
    live_end = int(ts[(v > 0) & listed].max())
    n = ts.size
    months = d["month"].to_numpy()
    boundary = np.ones(n, dtype=bool)
    boundary[1:] = months[1:] != months[:-1]
    starts = np.flatnonzero(boundary)
    names = [str(x) for x in months[starts]]
    dts = np.diff(np.concatenate(([ts[0]], ts)))
    dts[0] = MIN_NS  # first bar of the instrument: nothing to compare with
    change = np.empty(n, dtype=bool)
    change[0] = True
    change[1:] = (c[1:] != c[:-1]) | boundary[1:]
    run_id = np.cumsum(change)
    run_len = np.bincount(run_id)[run_id]
    gaps = np.where(dts > MIN_NS, np.round(dts / MIN_NS) - 1, 0).astype(np.int64)
    inr = (ts <= live_end) & listed
    masks = {
        "nan": np.isnan(np.vstack([o, h, lo, c, v])).any(axis=0),
        "nonpositive": (o <= 0) | (h <= 0) | (lo <= 0) | (c <= 0) | (v < 0),
        "ohlc_bad": (h < lo) | (h < o) | (h < c) | (lo > o) | (lo > c),
        "dupes": dts == 0,
        "unsorted": dts < 0,
        "misaligned": ts % MIN_NS != 0,
        "zero_vol": v == 0,
        "stuck": run_len >= STUCK_RUN,
        "spike": (flags & flag_bits()["spike"]) != 0,
    }

    def per_month(x) -> np.ndarray:
        return np.add.reduceat(np.asarray(x).astype(np.int64), starts)

    counts = {k: per_month(m & inr) for k, m in masks.items()}
    counts["missing"] = per_month(np.where(inr, gaps, 0))
    counts["missing_exchange"] = per_month(np.where(inr, _exchange_per_bar(ts, gaps, inr, exch_ts), 0))
    return counts, per_month(inr), names, per_month(d["rest_added"].to_numpy())


def m3_rules(src: Sources, units, ins: pl.DataFrame | None) -> list[Metric]:
    """M3a hard rules (target 0) and M3b soft rules (rates per 1M bars), over in-scope instruments' live ranges.
    Input: the archive 1m months plus the REST 1m minutes the archive lacks (symbols with an archive 1m directory).
    Hard-rule holes whose minute is an exchange gap in rest/_missing/klines_1m.parquet are reported apart and are not
    counted in M3a."""
    ev = src.lake / "klines" / "1m"
    rest_1m = _rest_root(src) / "klines" / "1m"
    name_a = "hard rules inside live ranges, REST minutes merged, exchange gaps excluded (instruments with no exclusion)"
    name_b = "soft rules per 1M bars inside live ranges, REST minutes merged (zero volume, stuck closes, spikes)"
    scope = in_scope(ins)
    if scope is None:
        reason = "instrument master missing: exclude_reason scope unknown"
        return [_not("M3a", name_a, "defects", ev, "code", reason, "build the instrument master"),
                _not("M3b", name_b, "per 1M bars", ev, "code", reason, "build the instrument master")]
    spans: dict[str, list[tuple[int, int]]] = {}
    if "listed_end_ts" in ins.columns:
        lo_, hi_ = np.iinfo(np.int64).min, np.iinfo(np.int64).max
        for sym, a, b in (ins.filter(pl.col("exclude_reason").is_null())
                          .select("symbol", "onboard_ts", "listed_end_ts").iter_rows()):
            spans.setdefault(sym, []).append((lo_ if a is None else int(a), hi_ if b is None else int(b)))
    arch: dict[str, list[tuple[str, Path]]] = {}
    five_only = 0
    for sym, month in units:
        if sym not in scope:
            continue
        f1 = ev / sym / f"{month}.parquet"
        if not f1.exists():  # 1m pruned by ever_liquid: this symbol-month is scored on 5m only
            five_only += 1
            continue
        arch.setdefault(sym, []).append((month, f1))
    rest: dict[str, list[tuple[str, Path]]] = {}
    if rest_1m.is_dir():
        for sd in sorted(rest_1m.iterdir()):
            if sd.name in scope and (ev / sd.name).is_dir():
                rest[sd.name] = [(f.stem, f) for f in sorted(sd.glob("*.parquet"))]
    ledger_1m = rest_ledger(src, "klines_1m")
    # one ledger row per minute; scored symbols only (the refused settled or pruned symbols are out of scope)
    refused = int(((ledger_1m["reason"] == "endpoint_error") & ledger_1m["symbol"].is_in(sorted(scope))).sum())
    exch_by: dict[str, list[int]] = {}
    for sym, ts_ in exchange_gaps(ledger_1m).select("symbol", "ts").iter_rows():
        exch_by.setdefault(sym, []).append(ts_)
    totals = {k: 0 for k in (*HARD_RULES, *SOFT_AMBER_PER_1M, "missing_exchange")}
    bars_total = rest_minutes = symbols_scored = 0
    month_rows: list[dict] = []
    for sym in sorted(set(arch) | set(rest)):
        d = _merged_1m(arch.get(sym, []), rest.get(sym, []))
        if d is None:
            continue
        scan = _m3_counts(d, np.sort(np.asarray(exch_by.get(sym, []), dtype=np.int64)) if sym in exch_by
                          else _EMPTY_TS, spans.get(sym))
        if scan is None:
            continue  # all-flat instrument: no live range
        symbols_scored += 1
        counts_f, bars_f, months_f, rest_f = scan
        for k in totals:
            totals[k] += int(counts_f[k].sum())
        bars_total += int(bars_f.sum())
        rest_minutes += int(rest_f.sum())
        hard_f = sum(counts_f[r] for r in HARD_RULES) - counts_f["missing_exchange"]
        for i, month in enumerate(months_f):
            month_rows.append({"symbol": sym, "month": month, "bars": int(bars_f[i]), "hard": int(hard_f[i]),
                               "exchange_gap": int(counts_f["missing_exchange"][i]),
                               "spike": int(counts_f["spike"][i]), "stuck": int(counts_f["stuck"][i]),
                               "zero_vol": int(counts_f["zero_vol"][i]), "rest_minutes": int(rest_f[i])})
    if bars_total == 0:
        reason = "no in-scope instrument with a live range in the lake yet"
        return [_not("M3a", name_a, "defects", ev, "code", reason, "wait for klines_1m"),
                _not("M3b", name_b, "per 1M bars", ev, "code", reason, "wait for klines_1m")]
    excluded = len({s for s, _ in units} - scope)
    gap_log = gapfill_summary(src, "klines_1m")
    exch_total = totals["missing_exchange"]
    hard_incl = sum(totals[r] for r in HARD_RULES)
    hard_defects = hard_incl - exch_total
    top_hard = sorted(month_rows, key=lambda r: (-r["hard"], r["symbol"], r["month"]))[:20]
    top_soft = sorted(month_rows, key=lambda r: (-(r["spike"] + r["stuck"]), r["symbol"], r["month"]))[:20]
    fix_a = (f"fix {hard_defects} hard-rule defects; worst {top_hard[0]['symbol']} {top_hard[0]['month']} "
             f"({top_hard[0]['hard']})") if hard_defects else "keep the hard rules at 0 (exchange gaps excluded)"
    counts_raw = {r: totals[r] for r in HARD_RULES}
    counts_defect = {**counts_raw, "missing": totals["missing"] - exch_total}
    m3a = _metric("M3a", name_a, hard_defects, "defects inside live ranges (exchange gaps excluded)",
                  "green" if hard_defects == 0 else "red", ev, "code", fix=fix_a,
                  detail={"counts": counts_raw, "counts_defect": counts_defect,
                          "exchange_gap": {"minutes": exch_total,
                                           "reasons": list(REST_EXCHANGE_REASONS),
                                           "source": "rest/_missing/klines_1m.parquet (archive and REST both empty)"},
                          "hard_incl_exchange_gap": hard_incl,
                          "rest_minutes_merged": rest_minutes, "symbols_with_rest_minutes": len(rest),
                          "rest_refused_minutes_not_excluded": refused,
                          "bars_in_live_ranges": bars_total,
                          "instruments_scored": symbols_scored, "instruments_excluded_from_scope": excluded,
                          "top20_symbol_months": top_hard,
                          "symbol_months_5m_only": five_only, "gapfill_klines_1m_log": gap_log,
                          "remeasured_after_gapfill": gap_log is not None})
    rates = {k: totals[k] * 1e6 / bars_total for k in SOFT_AMBER_PER_1M}
    over = [k for k in SOFT_AMBER_PER_1M if rates[k] > SOFT_AMBER_PER_1M[k]]
    peak = max(rates, key=rates.get)
    m3b = _metric("M3b", name_b, round(sum(rates.values()), 3), "soft flags per 1M bars",
                  "amber" if over else "green", ev, "code",
                  fix=f"highest soft rule {peak} at {rates[peak]:.0f} per 1M bars; thresholds "
                      + ", ".join(f"{k} {v:g}" for k, v in SOFT_AMBER_PER_1M.items()),
                  detail={"rates_per_1m": {k: round(v, 3) for k, v in rates.items()},
                          "counts": {k: totals[k] for k in SOFT_AMBER_PER_1M},
                          "thresholds_per_1m": SOFT_AMBER_PER_1M, "over_threshold": over,
                          "bars_in_live_ranges": bars_total, "rest_minutes_merged": rest_minutes,
                          "top20_by_spike_plus_stuck": top_soft})
    return [m3a, m3b]


def spike_windows(src: Sources, units, scope: set[str] | None = None, *,
                  use_issues_cache: bool = True) -> pl.DataFrame:
    """One row per spike-flagged 1m bar of in-scope instruments; magnitude = |close / mean(prev, next) - 1| in bps.
    One scan per instrument; rows keep the order of `units` and then the file's row order."""
    bit = flag_bits()["spike"]
    if scope is not None:
        units = [u for u in units if u[0] in scope]
    issues_path = src.lake / "_issues" / "klines_1m.parquet"
    if use_issues_cache and issues_path.exists():
        issues = pl.read_parquet(issues_path, columns=["symbol", "month", "spikes"])
        have = {(s, m) for s, m, n in issues.iter_rows() if n}
        units = [u for u in units if u in have]
    by_sym: dict[str, list[tuple[int, str, Path]]] = {}
    for pos, (sym, month) in enumerate(units):
        f = src.lake / "klines" / "1m" / sym / f"{month}.parquet"
        if f.exists():
            by_sym.setdefault(sym, []).append((pos, month, f))
    parts: list[pl.DataFrame] = []
    for sym, items in by_sym.items():
        d = (pl.scan_parquet([str(f) for _, _, f in items], include_file_paths="__src")
             .select(pl.col("__src"), pl.col("ts").cast(pl.Int64),
                     pl.col("close").cast(pl.Float64).fill_null(float("nan")), pl.col("flags"))
             .collect())
        if d.height == 0:
            continue
        flags = d["flags"].to_numpy()
        idx = np.flatnonzero(flags & bit)
        if idx.size == 0:
            continue
        ts = d["ts"].to_numpy().astype(np.int64)
        close = d["close"].to_numpy().astype(np.float64)
        boundary = d.select((pl.col("__src") != pl.col("__src").shift(1)).fill_null(True)).to_series().to_numpy()
        starts = np.flatnonzero(boundary)
        ends = np.concatenate((starts[1:] - 1, [d.height - 1]))
        prev = np.concatenate(([close[0]], close[:-1]))
        prev[starts] = close[starts]  # first bar of each file: nothing before it in that file
        nxt = np.concatenate((close[1:], [close[-1]]))
        nxt[ends] = close[ends]
        ref = (prev + nxt) / 2
        file_no = np.cumsum(boundary) - 1
        src_names = d["__src"].gather(starts).to_list()
        pos_of = {str(f): pos for pos, _, f in items}
        file_pos = np.asarray([pos_of[s] for s in src_names], dtype=np.int64)[file_no[idx]]
        mag = np.nan_to_num(np.abs(close[idx] / ref[idx] - 1) * 1e4)
        parts.append(pl.DataFrame({
            "_pos": file_pos, "_within": idx - starts[file_no[idx]],
            "symbol": [sym] * idx.size, "start_ts": ts[idx], "end_ts": ts[idx] + MIN_NS,
            "reason": ["spike"] * idx.size, "magnitude_bps": mag,
        }, schema={"_pos": pl.Int64, "_within": pl.Int64, "symbol": pl.Utf8, "start_ts": pl.Int64,
                   "end_ts": pl.Int64, "reason": pl.Utf8, "magnitude_bps": pl.Float64}))
    schema = {"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64, "reason": pl.Utf8,
              "magnitude_bps": pl.Float64}
    if not parts:
        return pl.DataFrame(schema=schema)
    return (pl.concat(parts).sort(["_pos", "_within"], maintain_order=True)
            .select(list(schema)))


# ---------------------------------------------------------------- M4 outage / flat detection

T_ORIGIN_NS = int(dt.datetime(2019, 12, 31, tzinfo=dt.timezone.utc).timestamp()) * 10**9
N_SLOTS = 15 * 366 * 288  # 5m slots from T_ORIGIN_NS: covers 2020 to 2034


def _by_symbol(units) -> list[tuple[str, list[str]]]:
    out: dict[str, list[str]] = {}
    for sym, month in units:
        out.setdefault(sym, []).append(month)
    return sorted((s, sorted(ms)) for s, ms in out.items())


def m4_outages(src: Sources, units) -> tuple[Metric, pl.DataFrame]:
    """Per symbol (memory bounded): live range from its nonzero-volume bars, zero-volume bars inside it, and per-5m
    slot counts across symbols for the market-wide flat windows (>= 5 live symbols, >= 30% flat)."""
    ev = src.lake / "klines" / "5m"
    name = "zero-volume 5m bars inside live range; market-wide flat windows"
    empty = pl.DataFrame(schema={"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64,
                                 "reason": pl.Utf8, "magnitude_bps": pl.Float64})
    if not units:
        return _not("M4", name, "share", ev, "code", "no finished 5m files", "wait for klines_1m"), empty
    live_by = np.zeros(N_SLOTS, dtype=np.int64)
    flat_by = np.zeros(N_SLOTS, dtype=np.int64)
    ranges: dict[str, tuple[int, int]] = {}
    all_syms: set[str] = set()
    bars = zeros = 0
    years_bars: dict[int, int] = {}
    years_zero: dict[int, int] = {}
    for sym, months in _by_symbol(units):
        files = [ev / sym / f"{m}.parquet" for m in months if (ev / sym / f"{m}.parquet").exists()]
        if not files:
            continue
        all_syms.add(sym)
        d = pl.read_parquet(files, columns=["ts", "volume"])
        ts = d["ts"].to_numpy().astype(np.int64)
        vol = d["volume"].to_numpy().astype(np.float64)
        nz = ts[vol > 0]
        if nz.size == 0:
            continue
        lo, hi = int(nz.min()), int(nz.max())
        ranges[sym] = (lo, hi)
        inside = (ts >= lo) & (ts <= hi)
        zero_in = inside & (vol == 0)
        slots = (ts - T_ORIGIN_NS) // FIVE_MIN_NS
        live_by += np.bincount(slots[inside], minlength=N_SLOTS)[:N_SLOTS]
        flat_by += np.bincount(slots[zero_in], minlength=N_SLOTS)[:N_SLOTS]
        bars += int(inside.sum())
        zeros += int(zero_in.sum())
        yrs = ts.astype("datetime64[ns]").astype("datetime64[Y]").astype(np.int64) + 1970
        for y, c in zip(*np.unique(yrs[inside], return_counts=True)):
            years_bars[int(y)] = years_bars.get(int(y), 0) + int(c)
        for y, c in zip(*np.unique(yrs[zero_in], return_counts=True)):
            years_zero[int(y)] = years_zero.get(int(y), 0) + int(c)
    if not ranges:
        return _not("M4", name, "share", ev, "code", "every bar has zero volume", "check the 5m build"), empty
    lo_all = min(v[0] for v in ranges.values())
    hi_all = max(v[1] for v in ranges.values())
    return _m4_assemble(src, ev, name, live_by, flat_by, bars, zeros, lo_all, hi_all, len(ranges),
                        len(all_syms), years_bars, years_zero, {"reader": "lake archive 5m only"})


# ---------------------------------------------------------------- M4 / M5 / M6 on the published panel

PANEL_BATCH = 2_000_000
PANEL_COLS = ["ts", "instrument_id", "symbol", "volume", "filled", "tradable", "close", "mark_close"]
BAD_BPS_THRESHOLD = 0.005  # |close / mark - 1| above 50 bps


def panel_files(src: Sources) -> list[Path]:
    return sorted(src.panel.glob("*.parquet")) if src.panel.is_dir() else []


def _m4_assemble(src: Sources, ev: Path, name: str, live_by: np.ndarray, flat_by: np.ndarray, bars: int, zeros: int,
                 lo_all: int, hi_all: int, n_ranges: int, n_all: int, years_bars: dict, years_zero: dict,
                 extra: dict, zslot: np.ndarray | None = None, ztrad: np.ndarray | None = None
                 ) -> tuple[Metric, pl.DataFrame]:
    """Zero-volume share and the market-wide flat windows (>= 5 live symbols, >= 30% flat) from per-slot counts, matched
    to the USDS-M outage events. Shared by the lake and the panel readers. zslot and ztrad (one entry per zero-volume
    bar: its 5m slot and its tradable flag) give the bars inside the flat windows, all symbols."""
    empty = pl.DataFrame(schema={"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64,
                                 "reason": pl.Utf8, "magnitude_bps": pl.Float64})
    share = zeros / bars if bars else None
    cand_idx = np.flatnonzero((live_by >= 5) & (flat_by >= 0.30 * np.maximum(live_by, 1)))
    if zslot is not None and ztrad is not None:
        in_win = ((live_by >= 5) & (flat_by >= 0.30 * np.maximum(live_by, 1)))[zslot]
        extra = {**extra, "flat_window_zero_bars_all_symbols": int(in_win.sum()),
                 "flat_window_zero_bars_tradable": int((in_win & ztrad).sum())}
    windows: list[dict] = []
    if cand_idx.size:
        breaks = np.flatnonzero(np.diff(cand_idx) > 1)
        starts = np.concatenate(([0], breaks + 1))
        ends = np.concatenate((breaks, [cand_idx.size - 1]))
        for run_id, (s, e) in enumerate(zip(starts, ends), start=1):
            seg = cand_idx[s:e + 1]
            windows.append({"run": run_id, "start": T_ORIGIN_NS + int(seg[0]) * FIVE_MIN_NS,
                            "end": T_ORIGIN_NS + int(seg[-1]) * FIVE_MIN_NS, "n": int(seg.size),
                            "max_flat_share": float(np.max(flat_by[seg] / live_by[seg])),
                            "live_max": int(live_by[seg].max())})
    events = []
    if src.events.exists():
        events = (pl.read_parquet(src.events, columns=["event_type", "ts_utc", "end_ts_utc", "event_id",
                                                       "symbols", "scope"])
                  .filter(pl.col("event_type") == "outage").to_dicts())
    # market-wide USDS-M outages only (no symbol list, not COIN-M, not spot): the set a lake of perps should see
    market_wide = [e for e in events if not e.get("symbols") and "COIN-M" not in (e.get("scope") or "")
                   and "spot" not in (e.get("scope") or "").lower()]
    pad = 30 * MIN_NS
    in_cov, hit_events, hit_windows = [], set(), 0
    for e in market_wide:
        start = int(e["ts_utc"])
        end = int(e["end_ts_utc"]) if e["end_ts_utc"] is not None else start
        if end >= lo_all and start <= hi_all:
            in_cov.append(e["event_id"])
    for w in windows:
        w_start, w_end = int(w["start"]), int(w["end"]) + FIVE_MIN_NS
        matched = False
        for e in events:
            s = int(e["ts_utc"]) - pad
            end = int(e["end_ts_utc"]) if e["end_ts_utc"] is not None else int(e["ts_utc"])
            if w_start <= end + pad and s <= w_end:
                matched = True
                hit_events.add(e["event_id"])
        w["matched_outage"] = matched
        hit_windows += matched
    hit_rate = hit_windows / len(windows) if windows else None
    recall = len(hit_events & set(in_cov)) / len(in_cov) if in_cov else None
    status = grade(share, 0.005, 0.02) if share is not None else "not_measured"
    if hit_rate is not None:
        status = worst(status, grade(hit_rate, 0.8, 0.5, higher_is_better=True))
    per_year_d = {y: {"bars": bars_y, "zero": years_zero.get(y, 0),
                      "share": round(years_zero.get(y, 0) / bars_y, 6)}
                  for y, bars_y in sorted(years_bars.items())}
    queue = empty
    if windows:
        queue = pl.DataFrame({
            "symbol": ["MARKET"] * len(windows),
            "start_ts": [int(w["start"]) for w in windows],
            "end_ts": [int(w["end"]) + FIVE_MIN_NS for w in windows],
            "reason": ["market_flat"] * len(windows),
            # flat share in bps (0..10000) so the queue ranks outages with the price-based rows
            "magnitude_bps": [float(w["max_flat_share"]) * 1e4 for w in windows]},
            schema=empty.schema)
    fix = f"flag or drop the {len(windows)} market-wide flat windows ({hit_windows} match an outage event)"
    metric = _metric("M4", name, round(share, 6) if share is not None else None, "share of 5m bars", status, ev, "code", fix=fix,
                     detail={"bars_inside_live_range": bars, "zero_volume_bars": zeros,
                             "per_year": per_year_d, "symbols_with_live_range": n_ranges,
                             "symbols_all_flat": n_all - n_ranges,
                             "windows": len(windows), "windows_matched": hit_windows, "hit_rate": hit_rate,
                             "outage_events_in_coverage": len(in_cov), "event_recall": recall,
                             "recall_set": "market-wide USDS-M outages (no symbol list, not COIN-M or spot)",
                             "market_windows": windows, "min_live_symbols": 5, "flat_threshold": 0.30, **extra})
    return metric, queue


DAY_NS = 86_400 * 10**9
PANEL_FLAG_BIT_NAME = "single_bar_print"  # panel.PANEL_BITS key; read at run time, never hard-coded
PANEL_EMPTY = pl.DataFrame(schema={"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64,
                                   "reason": pl.Utf8, "magnitude_bps": pl.Float64})


def _pit_keys(src: Sources) -> pl.DataFrame | None:
    """One row per (instrument, UTC day) that universe_pit marks eligible: the research universe (top 100)."""
    path = src.universe_pit or src.panel.parent / "universe_pit.parquet"
    if not path.exists():
        return None
    u = pl.read_parquet(path, columns=["date", "instrument_id", "eligible"]).filter(pl.col("eligible"))
    return u.select("instrument_id", pl.col("date").cast(pl.Int32).cast(pl.Int64).alias("_day"),
                    pl.lit(True).alias("_pit")).unique(subset=["instrument_id", "_day"])


def _panel_flag_bit() -> int | None:
    """The panel's single-bar flag bit, read from panel.PANEL_BITS at run time (None until the panel defines it)."""
    from quant.data import panel as panel_mod
    return panel_mod.PANEL_BITS.get(PANEL_FLAG_BIT_NAME)


def _neighbour_states(files: list[Path], cand: pl.DataFrame) -> pl.DataFrame:
    """State of the 5m bar before and after each candidate (same instrument): 1 = more than 50 bps off the mark,
    2 = within 50 bps, 0 = no finite close and mark. A bar that is not in the panel has no row."""
    need = pl.concat([cand.select("instrument_id", (pl.col("ts") - FIVE_MIN_NS).alias("ts")),
                      cand.select("instrument_id", (pl.col("ts") + FIVE_MIN_NS).alias("ts"))]).unique()
    parts: list[pl.DataFrame] = []
    need_ts = need["ts"].unique().to_list()
    priced = pl.col("mark_close").is_finite() & (pl.col("mark_close") > 0) & pl.col("close").is_finite()
    dev = (pl.col("close") / pl.col("mark_close") - 1).abs()
    for f in files:
        # a lazy scan filtered on the candidate timestamps: row groups without them are skipped, so the neighbour pass
        # reads only the bars it needs (a batch-by-batch join peaked at 3.9 GB on the 2024 file)
        hit = (pl.scan_parquet(f).select("instrument_id", "ts", "close", "mark_close")
               .filter(pl.col("ts").is_in(need_ts))
               .join(need.lazy(), on=["instrument_id", "ts"], how="inner")
               .select("instrument_id", "ts", pl.when(priced).then(pl.when(dev > BAD_BPS_THRESHOLD).then(1)
                                                              .otherwise(2)).otherwise(0).cast(pl.Int8).alias("state"))
               .collect(engine="streaming"))
        if hit.height:
            parts.append(hit)
    if not parts:
        return pl.DataFrame(schema={"instrument_id": pl.Utf8, "ts": pl.Int64, "state": pl.Int8})
    return pl.concat(parts).unique(subset=["instrument_id", "ts"])


def _classify_neighbours(cand: pl.DataFrame, st: pl.DataFrame) -> pl.DataFrame:
    """Adds prev and next (neighbour state, -1 when the bar is absent) and the two classes: single (both neighbours on
    the mark) and sustained (a neighbour is also off the mark: two or more bars in a row)."""
    prev = st.select("instrument_id", pl.col("ts").alias("_prev"), pl.col("state").alias("prev"))
    nxt = st.select("instrument_id", pl.col("ts").alias("_next"), pl.col("state").alias("next"))
    c = (cand.with_columns((pl.col("ts") - FIVE_MIN_NS).alias("_prev"), (pl.col("ts") + FIVE_MIN_NS).alias("_next"))
         .join(prev, on=["instrument_id", "_prev"], how="left")
         .join(nxt, on=["instrument_id", "_next"], how="left")
         .with_columns(pl.col("prev").fill_null(-1).cast(pl.Int8), pl.col("next").fill_null(-1).cast(pl.Int8)))
    return c.with_columns(
        ((pl.col("prev") == 2) & (pl.col("next") == 2)).alias("single"),
        ((pl.col("prev") == 1) | (pl.col("next") == 1)).alias("sustained"))


def panel_scan(src: Sources, ins: pl.DataFrame | None) -> dict:
    """M4, M5 and M6 from one pass over the published panel (src.panel: the bars the research reads), plus one more
    pass for the neighbours of the off-mark bars.
    M4: real bars inside the live range of the PIT top-100 (universe_pit eligible that UTC day); the all-symbol share,
        the market-wide flat windows and their tradable bars are in the detail. Without universe_pit the scope falls
        back to all symbols, labelled in the detail.
    M5: bars after the last nonzero-volume bar of a delisted perpetual (every bar when there is none).
    M6: traded tradable bars whose 5m close is a single-bar reverting print: |close/mark - 1| > 50 bps with the bar
        before and the bar after both within 50 bps of mark. Sustained divergence (two or more bars in a row) is kept
        and reported apart; the panel marks single-bar prints non-tradable, so the value is what it leaves.
    Returns the metrics and their suspicious-window rows (schema of the lake readers).
    """
    ev = src.panel
    files = panel_files(src)
    if not files:
        na = "published panel not found"
        return {"m4": _not("M4", "zero-volume 5m bars inside live range (panel)", "share", ev, "code", na,
                           "publish the panel"), "m4_queue": PANEL_EMPTY,
                "m5": _not("M5", "5m bars after the last nonzero-volume bar, delisted perps (panel)", "bars", ev,
                           "code", na, "publish the panel"),
                "m6": _not("M6", "single-bar prints on tradable bars vs mark (panel)", "share of traded 5m bars", ev,
                           "free-source", na, "publish the panel"), "m6_queue": PANEL_EMPTY}
    ext = (pl.scan_parquet(files).filter(pl.col("volume") > 0).group_by("instrument_id")
           .agg(pl.col("ts").min().alias("lo"), pl.col("ts").max().alias("hi")).collect(engine="streaming"))
    n_all = int(pl.scan_parquet(files).select("instrument_id").unique().select(pl.len()).collect().item())
    dl = delisted_perps(ins) if ins is not None else set()
    pit = _pit_keys(src)
    bit = _panel_flag_bit()
    live_by = np.zeros(N_SLOTS, dtype=np.int64)
    flat_by = np.zeros(N_SLOTS, dtype=np.int64)
    years_bars: dict[int, int] = {}
    years_zero: dict[int, int] = {}
    bars = zeros = bars_all = zeros_all = filled_inside = dl_rows = tail_total = 0
    tails: Counter = Counter()
    dl_seen: set[str] = set()
    n_rows = traded = bad_tradable = bad_all = bad_untradable = traded_no_mark = 0
    bit_real = bit_traded = 0
    cand_parts: list[pl.DataFrame] = []
    zslot_parts: list[np.ndarray] = []
    ztrad_parts: list[np.ndarray] = []
    for f in files:
        pf = pq.ParquetFile(f)
        has_flags = bit is not None and "issue_flags" in pf.schema_arrow.names
        cols = PANEL_COLS + (["issue_flags"] if has_flags else [])
        for batch in pf.iter_batches(batch_size=PANEL_BATCH, columns=cols):
            d = pl.from_arrow(batch).join(ext, on="instrument_id", how="left")
            if pit is not None:
                d = d.with_columns((pl.col("ts") // DAY_NS).alias("_day")).join(pit, on=["instrument_id", "_day"],
                                                                               how="left")
            d = d.with_columns(
                ((pl.col("ts") >= pl.col("lo")) & (pl.col("ts") <= pl.col("hi"))).fill_null(False).alias("in_range"),
                pl.col("symbol").is_in(sorted(dl)).alias("dl"),
                (pl.col("hi").is_null() | (pl.col("ts") > pl.col("hi"))).alias("after_last"),
                pl.col("volume").eq(0).fill_null(False).alias("zero"),
                pl.col("filled").fill_null(False).alias("is_filled"))
            ts = d["ts"].to_numpy().astype(np.int64)
            in_range = d["in_range"].to_numpy()
            is_filled = d["is_filled"].to_numpy()
            zero = d["zero"].to_numpy()
            in_scope = d["_pit"].fill_null(False).to_numpy() if pit is not None else np.ones(d.height, dtype=bool)
            inside = in_range & ~is_filled
            zin = inside & zero
            slot = (ts - T_ORIGIN_NS) // FIVE_MIN_NS
            ok = (slot >= 0) & (slot < N_SLOTS)
            live_by += np.bincount(slot[inside & ok], minlength=N_SLOTS)[:N_SLOTS]
            flat_by += np.bincount(slot[zin & ok], minlength=N_SLOTS)[:N_SLOTS]
            zs = zin & ok
            zslot_parts.append(slot[zs])
            tradable = d["tradable"].to_numpy().astype(bool)
            ztrad_parts.append(tradable[zs])
            bars_all += int(inside.sum())
            zeros_all += int(zin.sum())
            ip = inside & in_scope
            zp = zin & in_scope
            bars += int(ip.sum())
            zeros += int(zp.sum())
            filled_inside += int((in_range & is_filled).sum())
            if ip.any():
                yrs = ts[ip].astype("datetime64[ns]").astype("datetime64[Y]").astype(np.int64) + 1970
                for y, c in zip(*np.unique(yrs, return_counts=True)):
                    years_bars[int(y)] = years_bars.get(int(y), 0) + int(c)
            if zp.any():
                yrs = ts[zp].astype("datetime64[ns]").astype("datetime64[Y]").astype(np.int64) + 1970
                for y, c in zip(*np.unique(yrs, return_counts=True)):
                    years_zero[int(y)] = years_zero.get(int(y), 0) + int(c)
            dl_mask = d["dl"].to_numpy()
            if dl_mask.any():
                dl_rows += int(dl_mask.sum())
                dl_seen.update(d.filter(pl.col("dl"))["symbol"].unique().to_list())
                tail_df = d.filter(pl.col("dl") & pl.col("after_last"))
                tail_total += tail_df.height
                if tail_df.height:
                    tails.update(dict(tail_df.group_by("symbol").len().iter_rows()))
            n_rows += d.height
            vol = d["volume"].to_numpy()
            cl = d["close"].to_numpy().astype(np.float64)
            mk = d["mark_close"].to_numpy().astype(np.float64)
            real = (vol > 0) & ~is_filled
            has_mark = np.isfinite(mk) & (mk > 0)
            with np.errstate(divide="ignore", invalid="ignore"):
                diff = np.abs(cl / mk - 1.0)
            off = np.isfinite(diff) & (diff > BAD_BPS_THRESHOLD)
            traded_m = real & has_mark & tradable
            traded += int(traded_m.sum())
            traded_no_mark += int((real & ~has_mark).sum())
            bad_tradable += int((traded_m & off).sum())
            bad_all += int((real & has_mark & off).sum())
            bad_untradable += int((real & has_mark & off & ~tradable).sum())
            if has_flags:
                flagged = (d["issue_flags"].to_numpy().astype(np.uint32) & np.uint32(bit)) != 0
                bit_real += int((real & flagged).sum())
                bit_traded += int((traded_m & flagged).sum())
            cand = real & has_mark & off
            if cand.any():
                sub = d.select("instrument_id", "symbol").filter(pl.Series(cand))
                cand_parts.append(sub.with_columns(pl.Series("ts", ts[cand]), pl.Series("tradable", tradable[cand]),
                                                   pl.Series("bps", diff[cand] * 1e4)))
    if n_all == 0:
        na = "no instrument in the panel"
        return {"m4": _not("M4", "zero-volume 5m bars inside live range (panel)", "share", ev, "code", na,
                           "rebuild the panel"), "m4_queue": PANEL_EMPTY,
                "m5": _not("M5", "5m bars after the last nonzero-volume bar, delisted perps (panel)", "bars", ev,
                           "code", na, "rebuild the panel"),
                "m6": _not("M6", "single-bar prints on tradable bars vs mark (panel)", "share of traded 5m bars", ev,
                           "free-source", na, "rebuild the panel"), "m6_queue": PANEL_EMPTY}
    lo_all = int(ext["lo"].min())
    hi_all = int(ext["hi"].max())
    scope = ("PIT top-100 eligible bars (universe_pit eligible on the bar's UTC day)" if pit is not None
             else "all symbols (universe_pit absent)")
    pan = {"scope": scope, "all_symbol_bars_inside_live_range": bars_all,
           "all_symbol_zero_volume_bars": zeros_all,
           "all_symbol_zero_volume_share": round(zeros_all / bars_all, 6) if bars_all else None,
           "panel_filled_bars_inside_live_range": filled_inside, "panel_rows": n_rows,
           "instruments_in_panel": n_all, "source": str(ev),
           "note": "real bars only (filled is false); REST minutes are not in the panel 5m volume, only the 5m end "
                   "rule and the premium overlay are; zero-volume bars on thin coins are real no-trade bars"}
    zslot = np.concatenate(zslot_parts) if zslot_parts else np.empty(0, dtype=np.int64)
    ztrad = np.concatenate(ztrad_parts) if ztrad_parts else np.empty(0, dtype=bool)
    m4, m4_queue = _m4_assemble(src, ev, "zero-volume 5m bars inside live range, PIT top-100 (panel)",
                                live_by, flat_by, bars, zeros, lo_all, hi_all, ext.height, n_all,
                                years_bars, years_zero, pan, zslot=zslot, ztrad=ztrad)
    share5 = tail_total / dl_rows if dl_rows else None
    m5 = _metric("M5", "5m bars after the last nonzero-volume bar, delisted perps (panel)", tail_total, "5m bars",
                 "amber" if tail_total else "green", ev, "code",
                 fix=f"trim the {tail_total} bars after last nonzero volume in {len(tails)} delisted perps in the panel "
                     "build" if tail_total else "keep the panel tails trimmed (0 bars after last volume)",
                 detail={"delisted_perps_in_panel": len(dl_seen), "perps_with_tail": len(tails),
                         "bars_checked": dl_rows, "tail_share_of_bars": round(share5, 6) if share5 is not None else None,
                         "instruments_all_flat": n_all - ext.height, "tails_by_symbol": dict(tails.most_common(20))})
    cand = pl.concat(cand_parts) if cand_parts else pl.DataFrame(schema={
        "instrument_id": pl.Utf8, "symbol": pl.Utf8, "ts": pl.Int64, "tradable": pl.Boolean, "bps": pl.Float64})
    if cand.height:
        st = _neighbour_states(files, cand)
        cls = _classify_neighbours(cand, st)
        ct = cls.filter(pl.col("tradable"))
        single_rows = ct.filter(pl.col("single"))
        single_n = single_rows.height
        sustained_n = ct.filter(pl.col("sustained")).height
        unresolved_n = ct.height - single_n - sustained_n
        queue_m6 = pl.DataFrame({
            "symbol": single_rows["symbol"].to_list(), "start_ts": single_rows["ts"].to_list(),
            "end_ts": (single_rows["ts"] + FIVE_MIN_NS).to_list(), "reason": ["mark_divergence"] * single_n,
            "magnitude_bps": single_rows["bps"].to_list()}, schema=PANEL_EMPTY.schema)
    else:
        single_n = sustained_n = unresolved_n = 0
        queue_m6 = PANEL_EMPTY
    single_share = single_n / traded if traded else None
    m6 = _metric("M6", "single-bar prints left on tradable bars vs mark, panel (|close/mark - 1| > 50 bps, with the "
                       "bars before and after within 50 bps)",
                 round(single_share, 6) if single_share is not None else None, "share of traded 5m bars",
                 grade(single_share, 0.001, 0.005) if single_share is not None else "not_measured", ev, "free-source",
                 fix=f"{single_n} single-bar reverting prints remain tradable; mark them tradable=False in the panel build"
                 if single_n else "no single-bar reverting print is left on a tradable bar",
                 detail={"traded_bars": traded, "single_bar_remaining": single_n,
                         "sustained_divergence_kept": sustained_n,
                         "neighbour_missing_unresolved": unresolved_n,
                         "bad_traded_tradable": bad_tradable, "bad_untradable_excluded": bad_untradable,
                         "bad_traded_all_tradability": bad_all, "traded_without_mark": traded_no_mark,
                         "threshold_bps": 50, "panel_flag_bit": PANEL_FLAG_BIT_NAME if bit is not None else None,
                         "panel_flag_bit_value": bit, "flag_bit_on_real_bars": bit_real,
                         "flag_bit_on_traded_tradable": bit_traded,
                         "note": "single-bar = the bar before and the bar after are both within 50 bps of mark; "
                                 "sustained = a neighbour is also off mark (kept as real divergence); the panel marks "
                                 "single-bar prints tradable=False, so this value is what it leaves"})
    return {"m4": m4, "m4_queue": m4_queue, "m5": m5, "m6": m6, "m6_queue": queue_m6}



# ---------------------------------------------------------------- M6 bad prints vs mark

def m6_bad_prints(src: Sources, units) -> tuple[Metric, pl.DataFrame]:
    """Per symbol: 5m close against the 5m mark close on months both have (memory bounded)."""
    ev = src.lake / "markPriceKlines" / "5m"
    name = "bad prints on traded bars: |close/mark - 1| > 50 bps (5m)"
    empty = pl.DataFrame(schema={"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64,
                                 "reason": pl.Utf8, "magnitude_bps": pl.Float64})
    refs = set(family_units(src.lake, "markPriceKlines_5m", "markPriceKlines"))
    common = sorted(set(refs) & set(units))
    if not common:
        return _not("M6", name, "share of 5m bars", ev, "free-source",
                    "no finished markPriceKlines_5m units overlap the lake",
                    "download markPriceKlines 5m (free archive) for the lake symbols"), empty
    n = traded = bad = bad_flat = 0
    frames: list[pl.DataFrame] = []
    for sym, months in _by_symbol(common):
        kf = [src.lake / "klines" / "5m" / sym / f"{m}.parquet" for m in months]
        rf = [src.lake / "markPriceKlines" / "5m" / sym / f"{m}.parquet" for m in months]
        k = pl.read_parquet([str(f) for f in kf if f.exists()], columns=["ts", "close", "volume"]) \
            .rename({"close": "c"})
        r = pl.read_parquet([str(f) for f in rf if f.exists()], columns=["ts", "close"]) \
            .rename({"close": "r"})
        j = k.join(r, on="ts", how="inner").filter(pl.col("r") > 0)
        if j.height == 0:
            continue
        off = (pl.col("c") / pl.col("r") - 1).abs() > 0.005
        stats = j.select((pl.len()).alias("n"), (pl.col("volume") > 0).sum().alias("traded"),
                         (off & (pl.col("volume") > 0)).sum().alias("bad"),
                         (off & (pl.col("volume") == 0)).sum().alias("bad_flat"))
        n += int(stats["n"][0])
        traded += int(stats["traded"][0])
        bad += int(stats["bad"][0])
        bad_flat += int(stats["bad_flat"][0])
        hits = (j.filter(pl.col("volume") > 0)
                .with_columns(((pl.col("c") / pl.col("r") - 1).abs() * 1e4).alias("magnitude_bps"))
                .filter(pl.col("magnitude_bps") > 50)
                .select(pl.lit(sym).alias("symbol"), pl.col("ts").alias("start_ts"),
                        (pl.col("ts") + FIVE_MIN_NS).alias("end_ts"), pl.lit("mark_divergence").alias("reason"),
                        "magnitude_bps"))
        if hits.height:
            frames.append(hits)
    if n == 0 or traded == 0:
        return _not("M6", name, "share of 5m bars", ev, "free-source", "mark and close do not overlap",
                    "check the mark download"), empty
    bad_rows = pl.concat(frames) if frames else empty
    share = bad / traded
    metric = _metric("M6", name, round(share, 6), "share of traded 5m bars", grade(share, 0.001, 0.005), ev,
                     "free-source", fix=f"{bad} traded bars deviate more than 50 bps from mark; inspect them",
                     detail={"bars": n, "traded_bars": traded, "bad_traded": bad, "bad_zero_volume": bad_flat,
                             "share_all_bars": round((bad + bad_flat) / n, 6), "months": len(common),
                             "threshold_bps": 50,
                             "note": "zero-volume bars hold a frozen close; they are trimmed with M5 tails"})
    return metric, bad_rows


# ---------------------------------------------------------------- M7 three-venue consensus

DEV_CUT_NS = int(dt.datetime(2025, 10, 1, tzinfo=dt.timezone.utc).timestamp()) * 10**9  # holdout is sealed from here
CONSENSUS_STATES = ("confirmed", "refuted", "disputed", "undecided")
M7_GREEN, M7_AMBER = 0.80, 0.60
VENUES = ("kraken", "bybit", "okx")


IMMATERIAL_BPS = 25.0  # a window whose Binance move is at most this is immaterial (venues: no_binance_move)
NO_BINANCE_MOVE = "no_binance_move"
VENUE_VERDICTS = {"kraken": ("kraken", "verify", "verdicts.parquet"), "bybit": ("bybit", "verify", "verdicts.parquet"),
                  "okx": ("okx", "verify", "verdicts.parquet")}
OKX_ELIGIBLE = ("okx", "verify", "eligible_verdicts.parquet")  # overlays okx verdicts.parquet, as bybit.build_consensus


def _venue_frame(lake: Path, venue: str) -> pl.DataFrame:
    """Per window: that venue's verdict, detail and magnitude (empty columns when the venue file is absent)."""
    files = [lake.joinpath(*VENUE_VERDICTS[venue])]
    if venue == "okx":
        files.append(lake.joinpath(*OKX_ELIGIBLE))
    frames = [pl.read_parquet(f, columns=["window_id", "verdict", "detail", "magnitude_bps"]) for f in files if f.exists()]
    if not frames:
        return pl.DataFrame(schema={"window_id": pl.Utf8, f"{venue}_verdict": pl.Utf8, f"{venue}_detail": pl.Utf8,
                                    f"{venue}_mag": pl.Float64})
    d = pl.concat(frames).unique(subset=["window_id"], keep="last", maintain_order=True)
    return d.select("window_id", pl.col("verdict").alias(f"{venue}_verdict"),
                    pl.col("detail").alias(f"{venue}_detail"), pl.col("magnitude_bps").alias(f"{venue}_mag"))


def _classify_windows(src: Sources) -> pl.DataFrame:
    """The consensus table with each venue's detail joined on. cls is the consensus, except that an undecided window
    whose Binance move is immaterial (a venue reports no_binance_move at <= 25 bps) is cls 'immaterial'."""
    t = pl.read_parquet(src.verify_consensus, columns=["window_id", "start_ts", "consensus", *VENUES])
    lake = src.verify_consensus.parent
    for v in VENUES:
        t = t.join(_venue_frame(lake, v), on="window_id", how="left")
    nbm = pl.any_horizontal([
        (pl.col(f"{v}_detail").str.starts_with(NO_BINANCE_MOVE) & (pl.col(f"{v}_mag") <= IMMATERIAL_BPS)).fill_null(False)
        for v in VENUES])
    imm = (pl.col("consensus") == "undecided") & nbm
    return t.with_columns(imm.alias("immaterial")).with_columns(
        pl.when(pl.col("immaterial")).then(pl.lit("immaterial")).otherwise(pl.col("consensus")).alias("cls"))


def _tally(df: pl.DataFrame) -> dict:
    counts = {s: int((df["cls"] == s).sum()) for s in CONSENSUS_STATES}
    return {"windows": df.height, **counts}


def _decided_share(df: pl.DataFrame) -> float | None:
    """decided / (windows - immaterial); None when nothing is scored."""
    den = df.height - int((df["cls"] == "immaterial").sum())
    if den <= 0:
        return None
    return round(int((df["cls"] == "confirmed").sum() + (df["cls"] == "refuted").sum()) / den, 6)


def _all_checked(t: pl.DataFrame, pred) -> pl.Expr:
    """True when at least one venue has a verdict and every venue that has one satisfies pred(venue)."""
    checked = [pl.col(v).is_not_null() for v in VENUES]
    rule = pl.all_horizontal([(~c) | pred(v) for c, v in zip(checked, VENUES)])
    return (pl.any_horizontal(checked) & rule).fill_null(False)


def m7_verification(src: Sources) -> Metric:
    name = "3-venue consensus: decided share of suspicious windows (confirmed or refuted; immaterial windows out)"
    ev = src.verify_consensus
    if not ev.exists():
        return _not("M7", name, "share of windows", ev, "free-source",
                    "verify_consensus.parquet absent (no 3-venue consensus table yet)",
                    "build the consensus table: kraken, bybit and okx verdict per suspicious window")
    t0 = pl.read_parquet(ev, columns=["consensus"])
    if t0.height == 0:
        return _not("M7", name, "share of windows", ev, "free-source", "consensus table is empty",
                    "verify the suspicious-window queue on the three venues")
    unknown = sorted(set(t0["consensus"].drop_nulls().unique().to_list()) - set(CONSENSUS_STATES))
    if unknown:
        return _not("M7", name, "share of windows", ev, "code", f"unknown consensus values {unknown}",
                    "fix the consensus labels to confirmed, refuted, disputed, undecided")
    t = _classify_windows(src)
    total = _tally(t)
    imm = int((t["cls"] == "immaterial").sum())
    den = t.height - imm
    decided = total["confirmed"] + total["refuted"]
    share = decided / den if den else None
    status = grade(share, M7_GREEN, M7_AMBER, higher_is_better=True) if share is not None else "not_measured"
    no_market_everywhere = pl.all_horizontal([pl.col(v) == "no_market" for v in VENUES]).fill_null(False)
    unverifiable = int(t.filter(no_market_everywhere).height)
    undecided_mat = t.filter(pl.col("cls") == "undecided")
    pair_absent = int(undecided_mat.filter(_all_checked(
        undecided_mat, lambda v: pl.col(f"{v}_detail").str.starts_with("pair_absent"))).height)
    few_trades = int(undecided_mat.filter(_all_checked(
        undecided_mat, lambda v: pl.col(f"{v}_detail").str.starts_with("few_trades"))).height)
    dev = t.filter(pl.col("start_ts") < DEV_CUT_NS)
    hold = t.filter(pl.col("start_ts") >= DEV_CUT_NS)
    dev_tally = _tally(dev)
    dev_tally["decided_share"] = _decided_share(dev)
    years = t.with_columns(pl.from_epoch(pl.col("start_ts"), time_unit="ns").dt.year().alias("year"),
                           (pl.col("start_ts") < DEV_CUT_NS).alias("dev"))
    by_year = {}
    for y in sorted(years["year"].unique().to_list()):
        g = years.filter(pl.col("year") == y)
        d, h = g.filter(pl.col("dev")), g.filter(~pl.col("dev"))
        row = {"dev": _tally(d), "holdout_counts": _tally(h)}
        row["dev"]["decided_share"] = _decided_share(d)
        by_year[str(y)] = row
    fix = (f"verify {total['undecided'] - unverifiable} undecided windows on another source; "
           f"{unverifiable} unverifiable: no other venue lists it; adjudicate {total['disputed']} disputed windows; "
           f"mask the {total['refuted']} refuted windows in the panel build")
    return _metric("M7", name, round(share, 6) if share is not None else None, "share of windows decided", status,
                   ev, "free-source", fix=fix,
                   detail={"windows": t.height, "counts": {k: total[k] for k in CONSENSUS_STATES},
                           "immaterial": imm, "denominator": den, "decided": decided,
                           "immaterial_rule": f"undecided and a venue reports {NO_BINANCE_MOVE} with magnitude <= "
                                              f"{IMMATERIAL_BPS:g} bps",
                           "unverifiable_no_other_venue_lists_it": unverifiable,
                           "unverifiable_pair_absent_everywhere": pair_absent,
                           "unverifiable_few_trades_everywhere": few_trades,
                           "undecided_verifiable": total["undecided"] - unverifiable,
                           "dev_before_2025_10_01": dev_tally, "holdout_counts_only": _tally(hold),
                           "by_year": by_year,
                           "rule": "decided = confirmed + refuted; denominator = all windows minus immaterial; "
                                   "unverifiable = kraken, bybit and okx all no_market; pair_absent and few_trades "
                                   "everywhere count undecided windows whose checked venues all say so; holdout rows "
                                   "are counted, not scored"})


# ---------------------------------------------------------------- M8 funding

FUNDING_CHANGE_DATES = ("2023-10-12", "2025-05-02", "2025-09-01", "2025-09-18", "2026-01-02")
FUNDING_WINDOW_NS = 3 * 86_400 * 10**9
HOUR_NS = 3_600 * 10**9


def _funding_symbol(path: Path, tail: Path | None = None) -> dict:
    """Missing events per row. Each step is matched to the interval (this row's or the previous row's) that fits
    it better, so a regime change does not create false gaps. Missing events are charged to the row that ends
    the step. tail = the REST file (lake/rest/funding) whose events after the archive's last one are appended, the
    same merge the panel uses (panel._read_funding)."""
    cols = ["ts", "funding_interval_hours", "last_funding_rate"]
    f = pl.read_parquet(path, columns=cols)
    if tail is not None and tail.exists():
        t = pl.read_parquet(tail, columns=cols)
        f = pl.concat([f, t.filter(pl.col("ts") > f["ts"].max()) if f.height else t])
    f = f.unique(subset="ts", keep="last").sort("ts")
    ts = f["ts"].to_numpy().astype(np.int64)
    rate = f["last_funding_rate"].to_numpy().astype(np.float64)
    iv = f["funding_interval_hours"].to_numpy().astype(np.float64)
    n = ts.size
    miss = np.zeros(n, dtype=np.int64)
    miss_ts = np.empty(0, dtype=np.int64)
    if n >= 2:
        steps = np.diff(ts).astype(np.float64)

        def fit(hours):
            ok = hours > 0
            safe = np.where(ok, hours, 1.0) * HOUR_NS
            per = np.maximum(np.round(steps / safe), 1)
            resid = np.where(ok, np.abs(steps - per * safe), np.inf)
            return per, resid

        # A step where the recorded interval changes straddles a regime change (4h<->1h, 8h<->1h, 8h<->4h): the
        # new regime starts at its first row, so no event is owed inside the step. Only same-regime steps count.
        p_now, _ = fit(iv[1:])
        same = iv[1:] == iv[:-1]
        per = np.where(same, p_now, 1.0)
        miss[1:] = (per - 1).astype(np.int64)
        # the missing events themselves: k * interval after the row before the gap (same regime, so iv[i] applies)
        rows = np.flatnonzero(miss > 0)
        cnt = miss[rows]
        rep_i = np.repeat(rows, cnt)
        first = np.cumsum(cnt) - cnt
        k = np.arange(rep_i.size) - np.repeat(first, cnt) + 1
        miss_ts = ts[rep_i - 1] + k * (iv[rep_i] * HOUR_NS).astype(np.int64)
    return {"ts": ts, "rates": rate, "iv": iv, "miss": miss, "miss_ts": miss_ts, "observed": int(n),
            "missing": int(miss.sum())}


def m8_funding(src: Sources, units_fund: list[str]) -> Metric:
    ev = src.lake / "funding"
    name = "funding: missing share of expected events; regime and change-date completeness; cap breaches unverifiable"
    if not units_fund:
        return _not("M8", name, "share of expected events missing", ev, "free-source",
                    "no finished funding units in the lake (stage 'funding' not run)", "run the funding stage")
    led = exchange_gaps(rest_ledger(src, "funding"))
    led_by: dict[str, np.ndarray] = {}
    for sym_, ts_ in led.select("symbol", "ts").iter_rows():
        led_by.setdefault(sym_, []).append(ts_ // 10**9)
    led_by = {k: np.asarray(v, dtype=np.int64) for k, v in led_by.items()}
    caps: dict[str, tuple[float, float]] = {}
    if src.fund_info.exists():
        for row in _load_json(src.fund_info):
            caps[row["symbol"]] = (float(row["adjustedFundingRateCap"]), float(row["adjustedFundingRateFloor"]))
    windows = {d: int(np.datetime64(d, "ns").astype(np.int64)) for d in FUNDING_CHANGE_DATES}
    per_symbol: dict[str, int] = {}
    total_obs = total_raw = exch_total = breaches = cap_known = 0
    regimes: dict[str, list[int]] = {}
    changes: dict[str, list[int]] = {d: [0, 0] for d in FUNDING_CHANGE_DATES}
    sign, sign_mean = None, None
    for sym in units_fund:
        p = ev / f"{sym}.parquet"
        if not p.exists():
            continue
        r = _funding_symbol(p, src.lake / "rest" / "funding" / f"{sym}.parquet")
        ex = int(np.isin(r["miss_ts"] // 10**9, led_by[sym]).sum()) if sym in led_by else 0
        total_obs += r["observed"]
        total_raw += r["missing"]
        exch_total += ex
        per_symbol[sym] = r["missing"] - ex
        for val in np.unique(r["iv"]):
            mask = r["iv"] == val
            acc = regimes.setdefault(f"{val:g}", [0, 0])
            acc[0] += int(mask.sum())
            acc[1] += int(r["miss"][mask].sum())
        for d, lo in windows.items():
            mask = (r["ts"] >= lo - FUNDING_WINDOW_NS) & (r["ts"] < lo + FUNDING_WINDOW_NS)
            changes[d][0] += int(mask.sum())
            changes[d][1] += int(r["miss"][mask].sum())
        if sym in caps:
            cap_known += 1
            hi, lo_cap = caps[sym]
            breaches += int(((r["rates"] > hi + 1e-9) | (r["rates"] < lo_cap - 1e-9)).sum())
        if sym == "BTCUSDT":
            years = pl.Series(r["ts"]).cast(pl.Datetime("ns")).dt.year().to_numpy()
            mask = years == 2021
            if mask.any():
                sign_mean = float(r["rates"][mask].mean())
                sign = sign_mean > 0
    if not per_symbol:
        return _not("M8", name, "share of expected events missing", ev, "free-source", "no funding files on disk",
                    "run the funding stage")
    # Round 5: a schedule slot that REST answered with no row is not an expected event: Binance did not pay it, so it
    # leaves the denominator as well as the defects. The slots come from each symbol's own funding_interval_hours
    # history (_funding_symbol), not from a current or assumed interval.
    expected_before = total_obs + total_raw
    total_miss = total_raw - exch_total  # defects: missing events that no free source holds
    expected = total_obs + total_miss
    share = total_miss / expected
    gap_syms = sum(1 for v in per_symbol.values() if v > 0)
    status = "red" if (share > 0.001 or sign is False) else ("amber" if sign is None else "green")
    reason = "" if sign is not None else "sign check: BTCUSDT 2021 funding not in the lake"
    by_regime = {k: {"observed": o, "missing": m, "completeness": round(o / (o + m), 6)}
                 for k, (o, m) in sorted(regimes.items(), key=lambda kv: float(kv[0]))}
    by_change = {d: {"window_days": 3, "observed": o, "missing": m,
                     "completeness": round(o / (o + m), 6) if (o + m) else None}
                 for d, (o, m) in changes.items()}
    fix = (f"fill {total_miss} missing funding events ({share:.4%} of expected) across {gap_syms} perps; "
           f"{breaches} cap breaches are unverifiable against the current snapshot")
    return _metric("M8", name, share, "share of expected funding events missing", status, ev, "free-source",
                   fix=fix, reason=reason,
                   detail={"perps": len(per_symbol), "observed_events": total_obs, "expected_events": expected,
                           "expected_events_before_rest_absence": expected_before,
                           "expected_events_removed_rest_absent": exch_total,
                           "missing_events": total_miss, "missing_events_raw": total_raw,
                           "exchange_gap_events": exch_total,
                           "exchange_gap_source": "rest/_missing/funding.parquet (archive and REST both empty)",
                           "symbols_with_gaps": gap_syms,
                           "by_regime_interval_hours": by_regime, "by_change_date": by_change,
                           "rule": "same-regime steps only: step must be an integer number of recorded intervals; regime-change steps are not charged",
                           "cap_breaches_unverifiable": breaches, "symbols_with_cap_info": cap_known,
                           "cap_check": "current fundingInfo snapshot only; dated cap history is not in the data",
                           "btc_2021_sign_positive": sign, "btc_2021_mean_rate": sign_mean,
                           "gaps_by_symbol_top20": dict(sorted(((k, v) for k, v in per_symbol.items() if v > 0),
                                                               key=lambda kv: -kv[1])[:20])})


# ---------------------------------------------------------------- M9 OI completeness

OI_PER_DAY = 288  # 5m grid slots per UTC day


def _oi_union_counts(src: Sources, sym: str, days: set[str]) -> dict[str, int]:
    """Distinct 5m timestamps per UTC day in the archive metrics file and the REST metrics file together."""
    years = {d[:4] for d in days}
    stamps: set[int] = set()
    for base in (src.lake / "metrics" / sym, _rest_root(src) / "metrics" / sym):
        for y in years:
            p = base / f"{y}.parquet"
            if p.exists():
                stamps.update(pl.read_parquet(p, columns=["ts"])["ts"].to_list())
    counts: dict[str, int] = {}
    for ts in stamps:
        day = str(np.datetime64(int(ts), "ns").astype("datetime64[D]"))
        if day in days:
            counts[day] = counts.get(day, 0) + 1
    return counts


def _m9_columns(path: Path) -> list[str]:
    """The completeness columns the file holds: observed and expected are read when present (the REST overlay needs them)."""
    have = set(pl.read_parquet_schema(path))
    return [c for c in ("day", "observed", "expected", "completeness") if c in have]


def _m9_rest_overlay(src: Sources, d: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Round 5: a low OI day is completed by the REST rows the archive lacks (rest/metrics/<SYM>/<YEAR>.parquet).
    Observed becomes the distinct 5m stamps of archive and REST together, capped at the day's expected slots."""
    if not {"observed", "expected"} <= set(d.columns):
        return d, 0
    rest_root = _rest_root(src) / "metrics"
    syms = [s for s in d["sym"].unique().to_list() if (rest_root / s).is_dir()]
    patched = []
    for s in syms:
        sub = d.filter((pl.col("sym") == s) & (pl.col("completeness") < 0.9))
        if sub.height == 0:
            continue
        counts = _oi_union_counts(src, s, set(sub["day"].to_list()))
        for day, obs, exp_ in sub.select("day", "observed", "expected").iter_rows():
            new = min(counts.get(day, 0), int(exp_))
            if new > obs:
                patched.append((s, day, new))
    if not patched:
        return d, 0
    p = pl.DataFrame(patched, schema={"sym": pl.Utf8, "day": pl.Utf8, "observed_new": pl.Int64}, orient="row")
    d = d.join(p, on=["sym", "day"], how="left").with_columns(
        pl.coalesce(pl.col("observed_new"), pl.col("observed")).alias("observed")).drop("observed_new")
    d = d.with_columns((pl.col("observed") / pl.col("expected")).alias("completeness"))
    return d, len(patched)


def _m9_listed_days(src: Sources, d: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Keep the OI days that overlap a listing of the symbol ([onboard_ts, listed_end_ts) of any instruments row): days
    before the onboard, after the end and in a reused ticker's dead period are not scored (wave U, u-m9: 33,693 of
    37,036 low days were outside every listing). Returns (scored days, days dropped)."""
    if "day" not in d.columns or not src.instruments.exists():
        return d, 0
    if not {"onboard_ts", "listed_end_ts"} <= set(pq.read_schema(src.instruments).names):
        return d, 0
    iv = (pl.read_parquet(src.instruments, columns=["symbol", "onboard_ts", "listed_end_ts"])
          .select(pl.col("symbol").alias("sym"), pl.col("onboard_ts").fill_null(np.iinfo(np.int64).min).alias("_a"),
                  pl.col("listed_end_ts").fill_null(np.iinfo(np.int64).max).alias("_b")))
    known = set(iv["sym"].to_list())
    day = 86_400 * 10**9
    rows = d.with_row_index("_r").with_columns(
        _dn=pl.col("day").str.to_date().cast(pl.Datetime("ns")).cast(pl.Int64))
    inside = (rows.filter(pl.col("sym").is_in(list(known))).join(iv, on="sym")
              .filter((pl.col("_dn") < pl.col("_b")) & (pl.col("_dn") + day > pl.col("_a")))["_r"].unique())
    keep = rows.filter(pl.col("_r").is_in(inside.implode()) | ~pl.col("sym").is_in(list(known))).drop("_r", "_dn")
    return keep, d.height - keep.height


def m9_metrics(src: Sources, units_metrics: list[str]) -> Metric:
    ev = src.lake / "metrics" / "completeness"
    name = "metrics (open interest): day completeness"
    frames = [pl.read_parquet(ev / f"{s}.parquet", columns=_m9_columns(ev / f"{s}.parquet")).with_columns(
        pl.lit(s).alias("sym")) for s in units_metrics if (ev / f"{s}.parquet").exists()]
    if not frames:
        return _not("M9", name, "share of days", ev, "free-source", "no finished metrics units",
                    "run the metrics stage (archive starts 2020-09)")
    d = pl.concat(frames)
    d, filled = _m9_rest_overlay(src, d)
    d, outside = _m9_listed_days(src, d)
    low = d.filter(pl.col("completeness") < 0.9)
    share_low = low.height / d.height
    return _metric("M9", name, round(share_low, 6), "share of days below 0.9",
                   grade(share_low, 0.01, 0.05), ev, "free-source",
                   fix=f"drop or repair the {low.height} OI days below 0.9 completeness",
                   detail={"days": d.height, "symbols": d["sym"].n_unique(), "days_raised_by_rest": filled,
                           "days_outside_listings_not_scored": outside,
                           "p05": float(d["completeness"].quantile(0.05)),
                           "median": float(d["completeness"].median()),
                           "days_below_50": int((d["completeness"] < 0.5).sum())})


# ---------------------------------------------------------------- M10 instrument mapping

def m10_mapping(src: Sources) -> Metric:
    name = "instrument mapping: unresolved items"
    if not (src.instruments.exists() and src.events.exists()):
        return _not("M10", name, "items", src.instruments, "code", "instrument master or events missing",
                    "build the catalogue")
    ins = pl.read_parquet(src.instruments)
    ev_df = pl.read_parquet(src.events)
    renames = ev_df.filter(pl.col("event_type") == "rename")
    null_ratio = renames.filter(pl.col("ratio").is_null())
    unexplained = null_ratio.filter(pl.col("ratio_note").is_null() | (pl.col("ratio_note") == "")).height
    onboard_null = ins.filter(pl.col("onboard_ts").is_null()).height
    snapshot_only = ins.filter(~pl.col("has_archive") & pl.col("exclude_reason").is_null()).height
    excluded = ins.filter(pl.col("exclude_reason").is_not_null()).group_by("exclude_reason").len()
    value = onboard_null + unexplained + snapshot_only
    status = "red" if onboard_null else "amber" if value else "green"
    fix = (f"fetch the archive or exclude with a reason for {snapshot_only} snapshot-only instruments; "
           f"{unexplained} renames with a null ratio and no reason")
    return _metric("M10", name, value, "items", status, src.events, "code", fix=fix,
                   detail={"renames": renames.height, "rename_null_ratio": null_ratio.height,
                           "rename_null_ratio_unexplained": unexplained, "onboard_null": onboard_null,
                           "snapshot_only_in_scope": snapshot_only, "instruments": ins.height,
                           "exclusions": dict(zip(excluded["exclude_reason"].to_list(), excluded["len"].to_list()))})


# ---------------------------------------------------------------- M11 look-ahead / PIT tests

PYTEST_COUNT = re.compile(r"(\d+) (passed|failed|errors?|skipped)")


def m11_pit(src: Sources) -> Metric:
    name = "point-in-time / look-ahead tests (pytest)"
    files = [f for f in src.tests if Path(f).exists()]
    ev = paths.ROOT / "tests"
    if not files:
        return _not("M11", name, "failing tests", ev, "code", "no test files", "add look-ahead tests")
    cmd = [sys.executable, "-m", "pytest", "-q", "--tb=line", "-p", "no:cacheprovider", *map(str, files)]
    proc = subprocess.run(cmd, cwd=src.test_cwd, capture_output=True, text=True, timeout=1500)
    summary = next((ln for ln in reversed(proc.stdout.splitlines()) if PYTEST_COUNT.search(ln)), "")
    counts = {"passed": 0, "failed": 0, "error": 0, "skipped": 0}
    for n, kind in PYTEST_COUNT.findall(summary):
        counts["error" if kind.startswith("error") else kind] = int(n)
    failed = counts["failed"] + counts["error"]
    status = "green" if proc.returncode == 0 and failed == 0 and counts["passed"] > 0 else "red"
    fails = [ln for ln in proc.stdout.splitlines() if "FAILED" in ln or "ERROR" in ln][:20]
    return _metric("M11", name, float(failed), "failing tests", status, ev, "code",
                   fix=f"fix {failed} failing look-ahead/PIT test(s)" if failed else "keep the PIT tests green",
                   detail={"passed": counts["passed"], "failed": failed, "skipped": counts["skipped"],
                           "files": [_rel(Path(f)) for f in files], "returncode": proc.returncode,
                           "failures": fails})


# ---------------------------------------------------------------- M12 cost calibration coverage

M12_COVERAGE = 0.65  # registered coverage target (RESEARCH.md 2026-10-21 11:10 UTC)


def m12_spread(src: Sources) -> Metric:
    """Coverage after era multipliers, per (year, size band) cell with eff rows in 2020-2025. The detail file is the
    output of w-refit-ay's era_mult.py; a cell with n = 0 has no data and is not scored."""
    name = "cost calibration: charged spread coverage on the eff proxy, every (year, band) cell 2020-2025"
    ev = src.era_detail
    if not ev.exists():
        return _not("M12", name, "min coverage", ev, "free-source", "era_mult_detail.json absent",
                    "re-run data/samples/w-refit-ay/era_mult.py")
    detail = _load_json(ev)
    cells, no_data = [], []
    for year in sorted(detail):
        for band, rec in detail[year].items():
            if not rec.get("n"):
                no_data.append(f"{year} {band}")
                continue
            cov = float(rec["coverage_after"])
            cells.append({"year": int(year), "band": band, "n": int(rec["n"]), "coverage_after": round(cov, 6),
                          "meets": cov >= M12_COVERAGE})
    if not cells:
        return _not("M12", name, "min coverage", ev, "free-source", "no (year, band) cell has eff rows",
                    "build the eff spread proxy for 2020-2025")
    failing = [c for c in cells if not c["meets"]]
    low = min(cells, key=lambda c: c["coverage_after"])
    status = "green" if not failing else "amber"
    fix = ("" if not failing else
           f"{len(failing)} cell(s) under {M12_COVERAGE} after era multipliers, lowest {low['year']} {low['band']} "
           f"at {low['coverage_after']}: a registered multiplier change needs an owner decision")
    return _metric("M12", name, low["coverage_after"], "min coverage over cells", status, ev,
                   "owner-decision" if failing else "code", fix=fix,
                   detail={"cells": cells, "cells_scored": len(cells), "cells_failing": len(failing),
                           "cells_without_data": no_data, "target": M12_COVERAGE,
                           "lowest_cell": low, "config": "configs/cost_era_mult.json",
                           "model": "data/curated/cost_model.json (fit3)",
                           "source_rule": "coverage_after from w-refit-ay era_mult_detail.json, in-sample to each year"})


# ---------------------------------------------------------------- M13 context staleness

def _context_class(source: str) -> str | None:
    return source if source in CONTEXT_EXPECTED_AGE else None


def m13_context(src: Sources) -> Metric:
    name = "context staleness: cells older than their source cadence"
    if not (src.context.exists() and src.context_sources.exists()):
        return _not("M13", name, "share of cells", src.context, "free-source",
                    "context_daily or its sources json missing", "rebuild context_daily")
    meta = _load_json(src.context_sources)["columns"]
    df = pl.read_parquet(src.context)
    total, over, by_class, skipped = 0, 0, {}, []
    for col in df.columns:
        if not col.endswith("__age_days"):
            continue
        base = col[: -len("__age_days")]
        source = meta.get(base, {}).get("source", "")
        cls = "fred_weekly" if base == "fred_walcl" else "fred_daily" if source == "fred" else _context_class(source)
        if cls is None:
            skipped.append(base)
            continue
        ages = df[col].drop_nulls()
        n, bad = ages.len(), int((ages > CONTEXT_EXPECTED_AGE[cls]).sum())
        total += n
        over += bad
        acc = by_class.setdefault(cls, [0, 0])
        acc[0] += n
        acc[1] += bad
    if total == 0:
        return _not("M13", name, "share of cells", src.context, "code", "no measurable context cells",
                    "check the sources map")
    share = over / total
    return _metric("M13", name, round(share, 6), "share of cells", grade(share, 0.01, 0.05), src.context,
                   "free-source", fix=f"refresh or re-check the {over} stale context cells",
                   detail={"cells": total, "stale_cells": over,
                           "by_class": {k: {"cells": v[0], "stale": v[1],
                                            "share": round(v[1] / v[0], 6) if v[0] else None,
                                            "expected_max_age_days": CONTEXT_EXPECTED_AGE[k]}
                                        for k, v in by_class.items()},
                           "skipped_no_cadence": skipped})


# ---------------------------------------------------------------- M14 compliance

def parse_licenses(path: Path) -> dict[str, dict]:
    """Rows of the markdown table in DATA_LICENSES.md whose header has Source, Status and Verdict columns."""
    table: dict[str, dict] = {}
    header: list[str] | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s.startswith("|"):
            header = None
            continue
        cells = [c.strip() for c in s.strip("|").split("|")]
        if all(set(c) <= set("-: ") for c in cells):
            continue
        low = [c.lower() for c in cells]
        if header is None:
            if any("source" in c for c in low) and any("status" in c for c in low):
                header = low
            continue
        row = dict(zip(header, cells))
        src_cell = next((v for k, v in row.items() if "source" in k), "")
        if src_cell:
            table[src_cell.lower()] = {"status": next((v for k, v in row.items() if "status" in k), "").lower(),
                                       "verdict": next((v for k, v in row.items() if "verdict" in k), "").lower(),
                                       "source": src_cell}
    return table


def _license_row(table: dict[str, dict], source: str) -> dict | None:
    for alias in LICENSE_ALIASES.get(source, [source]):
        for key, row in table.items():
            if alias in key:
                return row
    return None


def curated_feeds(src: Sources) -> dict[str, set[str]]:
    """Map every top-level file in data/curated to the sources it was built from (names only, no quarantine)."""
    feeds: dict[str, set[str]] = {}
    if not src.curated.exists():
        return feeds
    for p in sorted(src.curated.iterdir()):
        if p.is_dir():
            continue
        name = p.name
        if name in CURATED_FILE_SOURCE:
            feeds[name] = set(CURATED_FILE_SOURCE[name])
        elif name.startswith("bars_"):
            feeds[name] = {"binance_um"}
        elif name.startswith("context_") or name == "context_daily.parquet":
            feeds[name] = set()
        else:
            feeds[name] = {"unmapped"}
    if src.context.exists() and src.context_sources.exists():
        cols = _load_json(src.context_sources)["columns"]
        srcs = {v.get("source") for v in cols.values() if v.get("source")}
        cols_now = pl.read_parquet_schema(src.context).keys()
        srcs |= {CONTEXT_PREFIX_SOURCE[pre] for c in cols_now for pre in CONTEXT_PREFIX_SOURCE if c.startswith(pre)}
        feeds["context_daily.parquet"] = srcs
        feeds[src.context_sources.name] = srcs
    return feeds


def _m14_evidence(src: Sources) -> Metric:
    name = "compliance: every curated feed licensed for this use"
    if not src.licenses.exists():
        return _not("M14", name, "feeds failing", src.licenses, "code",
                    "DATA_LICENSES.md not written yet (w-comply)", "write DATA_LICENSES.md")
    table = parse_licenses(src.licenses)
    if not table:
        return _not("M14", name, "feeds failing", src.licenses, "code",
                    "no Source/Status/Verdict table in DATA_LICENSES.md", "add the table")
    feeds = curated_feeds(src)
    red, unclear, gaps = [], [], []
    for fname, sources in sorted(feeds.items()):
        for s in sorted(sources):
            if s == "unmapped":
                gaps.append({"file": fname, "source": None, "why": "file has no source mapping"})
                continue
            row = _license_row(table, s)
            if row is None:
                gaps.append({"file": fname, "source": s, "why": "no row in DATA_LICENSES.md"})
            elif "quarant" in row["status"] or "forbidden" in row["verdict"]:
                red.append({"file": fname, "source": s, "status": row["status"], "verdict": row["verdict"]})
            elif "unclear" in row["verdict"]:
                unclear.append({"file": fname, "source": s, "status": row["status"], "verdict": row["verdict"]})
            elif not row["status"].startswith("used"):
                gaps.append({"file": fname, "source": s, "why": f"status {row['status']!r}, not used"})
    status = "red" if red else "amber" if (unclear or gaps) else "green"
    fixable = "owner-decision" if red or (unclear and not gaps) else "code"
    parts = []
    if red:
        parts.append(f"remove or re-licence {len(red)} forbidden/quarantined feed(s): "
                     + ", ".join(sorted({r['source'] for r in red})))
    if unclear:
        parts.append(f"owner decision on {len(unclear)} unclear verdict(s): "
                     + ", ".join(sorted({r['source'] for r in unclear})))
    if gaps:
        parts.append(f"resolve {len(gaps)} licence gap(s) in DATA_LICENSES.md")
    return _metric("M14", name, float(len(red) + len(unclear) + len(gaps)), "feeds failing", status,
                   src.licenses, fixable, fix="; ".join(parts) or "keep licences current",
                   detail={"red": red, "unclear": unclear, "gaps": gaps, "feeds_checked": len(feeds)})


def m14_compliance(src: Sources) -> Metric:
    """Waived by owner rule (2026-10-20): terms and licences are not weighed for this local research. The evidence
    is computed as before and kept in the detail; the status is "waived" and it is outside the counts and the
    worst items."""
    m = _m14_evidence(src)
    return replace(m, status=WAIVED, reason="waived by owner rule 2026-10-20: terms and licences are not weighed "
                                            "for local research",
                   detail={**m.detail, "computed_status": m.status, "computed_fix": m.fix})


# ---------------------------------------------------------------- M15 / M16 mark and premium 5m coverage

def _ts_table(files: list[Path], code_of: dict[str, int]) -> pl.DataFrame:
    """Distinct (code, ts) over the given parquet files, in one scan. code is the symbol's index in code_of;
    the symbol is the parent directory name."""
    if not files:
        return pl.DataFrame(schema={"code": pl.Int32, "ts": pl.Int64})
    return (pl.scan_parquet([str(f) for f in files], include_file_paths="__src", extra_columns="ignore")
            .select(pl.col("__src").str.split("/").list.get(-2).replace_strict(code_of, return_dtype=pl.Int32)
                    .alias("code"), pl.col("ts").cast(pl.Int64))
            .unique().collect())


def _sym_files(root: Path, syms: list[str]) -> list[Path]:
    out: list[Path] = []
    for sym in syms:
        d = root / sym
        if d.is_dir():
            out.extend(sorted(d.glob("*.parquet")))
    return out


COVERAGE_CHUNK = 10  # symbols per scan: the 5m grid table of 40 symbols alone is ~2.5 GB (OOM at the 4G cap)


def ohlc_coverage(src: Sources, families: tuple, syms: list[str]) -> dict:
    """_ohlc_chunk over symbol chunks of COVERAGE_CHUNK, merged in order (sums; dicts keep `syms` order)."""
    out = None
    for i in range(0, max(len(syms), 1), COVERAGE_CHUNK):
        part = _ohlc_chunk(src, families, syms[i:i + COVERAGE_CHUNK])
        if out is None:
            out = part
            continue
        for fam in families:
            a, b = out[fam], part[fam]
            for k in ("grid_slots", "leading_slots", "internal_slots", "trailing_slots", "symbols",
                      "symbols_no_family", "family_slots_off_grid", "exchange_gap_slots"):
                a[k] += b[k]
            for r, n in b["exchange_reasons"].items():
                a["exchange_reasons"][r] = a["exchange_reasons"].get(r, 0) + n
            a["internal_by_symbol"].update(b["internal_by_symbol"])
            a["coverage_start"].update(b["coverage_start"])
    return out


def _listed_slots(grid: pl.DataFrame, src: Sources, code_of: dict[str, int]) -> pl.DataFrame:
    """Grid slots inside some listing of the symbol: [onboard_ts, listed_end_ts) of any instruments.parquet row, any
    exclude_reason. Drops the dead period between the listings of a reused ticker (MDTUSDT 2024-05-16..2026-10-21) and
    bars before the first onboard. A symbol with no instruments row keeps every slot."""
    if not src.instruments.exists() or grid.height == 0:
        return grid
    have = set(pq.read_schema(src.instruments).names)
    if not {"symbol", "onboard_ts", "listed_end_ts"} <= have:
        return grid
    iv = (pl.read_parquet(src.instruments, columns=["symbol", "onboard_ts", "listed_end_ts"])
          .filter(pl.col("symbol").is_in(list(code_of)))
          .select(pl.col("symbol").replace_strict(code_of, return_dtype=pl.Int32).alias("code"),
                  pl.col("onboard_ts").fill_null(np.iinfo(np.int64).min).alias("_a"),
                  pl.col("listed_end_ts").fill_null(np.iinfo(np.int64).max).alias("_b")))
    known = grid.filter(pl.col("code").is_in(iv["code"].unique().implode()))
    inside = (known.join(iv, on="code").filter((pl.col("ts") >= pl.col("_a")) & (pl.col("ts") < pl.col("_b")))
              .select("code", "ts").unique())
    return pl.concat([inside, grid.filter(~pl.col("code").is_in(iv["code"].unique().implode()))])


def _ohlc_chunk(src: Sources, families: tuple, syms: list[str]) -> dict:
    """Per family, classify each missing klines-5m grid slot as leading (before the family's first bar), trailing
    (after its last bar) or internal (between). The family's bars are the archive files plus the REST overlay
    (rest/<family>/5m). An internal hole whose slot the REST ledger holds as empty, with no archive row, is an exchange
    gap: counted apart and not a defect. Batched: one scan per table; the per-symbol output keeps the order of `syms`."""
    code_of = {s: i for i, s in enumerate(syms)}
    sym_of = dict(enumerate(syms))
    grid_files = _sym_files(src.lake / "klines" / "5m", syms)
    grid_codes = {code_of[f.parent.name] for f in grid_files}  # symbols with at least one finished grid file
    grid = _listed_slots(_ts_table(grid_files, code_of), src, code_of)
    gcount = {int(c): int(n) for c, n in grid.group_by("code").len().iter_rows()}
    chunk_codes = pl.DataFrame({"symbol": list(code_of), "code": list(code_of.values())},
                               schema={"symbol": pl.Utf8, "code": pl.Int32})
    out: dict = {}
    for fam in families:
        acc = {"grid_slots": 0, "leading_slots": 0, "internal_slots": 0, "trailing_slots": 0, "exchange_gap_slots": 0,
               "symbols": 0, "symbols_no_family": 0, "family_slots_off_grid": 0, "exchange_reasons": {},
               "internal_by_symbol": {}, "coverage_start": {}}
        out[fam] = acc
        scoped = [sym_of[c] for c in sorted(grid_codes)]
        fam_tbl = _ts_table(_sym_files(src.lake / fam / "5m", scoped) + _sym_files(_rest_root(src) / fam / "5m", scoped),
                            code_of)
        fstats = fam_tbl.group_by("code").agg(pl.col("ts").min().alias("pmin"), pl.col("ts").max().alias("pmax"))
        led = (rest_ledger(src, fam).join(chunk_codes, on="symbol", how="inner")
               .select("code", "ts", "reason").unique(subset=["code", "ts"], keep="first"))
        miss = (grid.join(fam_tbl, on=["code", "ts"], how="anti").join(fstats, on="code", how="left")
                .join(led, on=["code", "ts"], how="left"))
        lead = pl.col("pmin").is_null() | (pl.col("ts") < pl.col("pmin"))
        trail = ~lead & pl.col("pmax").is_not_null() & (pl.col("ts") > pl.col("pmax"))
        miss = (miss.with_columns(lead.alias("_lead"), trail.alias("_trail"))
                .with_columns((~pl.col("_lead") & ~pl.col("_trail")).alias("_inter"))
                .with_columns((pl.col("_inter") & pl.col("reason").is_in(REST_EXCHANGE_REASONS)).alias("_exch")))
        per = miss.group_by("code").agg(pl.col("_lead").sum(), pl.col("_trail").sum(), pl.col("_inter").sum(),
                                        pl.col("_exch").sum())
        per_d = {int(r[0]): (int(r[1]), int(r[2]), int(r[3]), int(r[4])) for r in per.iter_rows()}
        reasons = miss.filter(pl.col("_inter") & pl.col("reason").is_not_null()).group_by("reason").len()
        acc["exchange_reasons"] = {r: int(n) for r, n in reasons.iter_rows()}
        off_d = {int(c): int(n) for c, n in
                 fam_tbl.join(grid, on=["code", "ts"], how="anti").group_by("code").len().iter_rows()}
        pstart = {int(c): int(p) for c, p in fstats.select("code", "pmin").iter_rows()}
        for sym in syms:
            c = code_of[sym]
            if c not in grid_codes:
                continue
            acc["symbols"] += 1
            g = gcount.get(c, 0)
            acc["grid_slots"] += g
            if c not in pstart:
                acc["symbols_no_family"] += 1
                acc["leading_slots"] += g
                acc["coverage_start"][sym] = None
                continue
            lead_n, trail_n, inter_n, exch_n = per_d.get(c, (0, 0, 0, 0))
            acc["leading_slots"] += lead_n
            acc["trailing_slots"] += trail_n
            acc["internal_slots"] += inter_n - exch_n
            acc["exchange_gap_slots"] += exch_n
            acc["family_slots_off_grid"] += off_d.get(c, 0)
            acc["coverage_start"][sym] = str(np.datetime64(pstart[c], "ns").astype("datetime64[D]"))
            if inter_n - exch_n:
                acc["internal_by_symbol"][sym] = inter_n - exch_n
    return out


def _ohlc_metric(src: Sources, mid: str, family: str, label: str, cov: dict | None, units) -> Metric:
    ev = src.lake / family / "5m"
    name = f"{label} 5m coverage, archive + REST overlay: internal holes (exchange gaps excluded; leading and trailing not scored)"
    if not ev.is_dir() and not (_rest_root(src) / family / "5m").is_dir():
        return _not(mid, name, "share of grid slots", ev, "free-source", f"{family}/5m not in the lake",
                    f"download {family} 5m (free daily and monthly archive)")
    if cov is None or cov[family]["grid_slots"] == 0:
        return _not(mid, name, "share of grid slots", ev, "free-source", "no finished klines 5m grid yet",
                    "wait for klines_1m")
    c = cov[family]
    share = c["internal_slots"] / c["grid_slots"]
    starts = sorted(d for d in c["coverage_start"].values() if d)
    by_year: dict[str, int] = {}
    for d in starts:
        by_year[d[:4]] = by_year.get(d[:4], 0) + 1
    leading_syms = sum(1 for s in c["coverage_start"] if c["coverage_start"][s] is not None)
    gap_log = gapfill_summary(src, f"{family}_5m")
    internal_incl = c["internal_slots"] + c["exchange_gap_slots"]
    top = list(c["internal_by_symbol"].items())[:5]
    fix = (f"fill {c['internal_slots'] * 5} internal minutes in {len(c['internal_by_symbol'])} symbols "
           f"(largest: {', '.join(f'{s} {n} slots' for s, n in top)}); the REST endpoint has no row for them and the "
           "archive has none either" if c["internal_slots"] else
           "keep internal holes at 0; exchange gaps and leading/trailing are coverage limits, not defects")
    return _metric(mid, name, round(share, 9), "share of grid slots (internal holes, exchange gaps excluded)",
                   grade(share, 0.0, 0.001), ev, "free-source", fix=fix,
                   detail={"grid_slots": c["grid_slots"], "symbols": c["symbols"],
                           "internal_slots": c["internal_slots"], "internal_minutes": c["internal_slots"] * 5,
                           "exchange_gap_slots": c["exchange_gap_slots"],
                           "exchange_gap_minutes": c["exchange_gap_slots"] * 5,
                           "exchange_gap_reasons": c["exchange_reasons"],
                           "internal_incl_exchange_gap_slots": internal_incl,
                           "leading_slots": c["leading_slots"], "leading_minutes": c["leading_slots"] * 5,
                           "trailing_slots": c["trailing_slots"], "trailing_minutes": c["trailing_slots"] * 5,
                           "symbols_no_family_file": c["symbols_no_family"],
                           "family_slots_off_grid": c["family_slots_off_grid"],
                           "internal_top20_symbols": dict(list(c["internal_by_symbol"].items())[:20]),
                           "rest_overlay": str(_rest_root(src) / family / "5m"),
                           "ledger": f"{_rest_root(src)}/_missing/{family}.parquet",
                           "known_limitation": {
                               "leading_means": "grid slots before the family's first bar (archive coverage starts "
                                                "later); not a defect",
                               "earliest_coverage_start": starts[0] if starts else None,
                               "latest_coverage_start": starts[-1] if starts else None,
                               "coverage_start_count_by_year": dict(sorted(by_year.items())),
                               "symbols_with_family_bars": leading_syms},
                           "gapfill_log": gap_log,
                           "reconciliation": {
                               "log_minutes_missing_after": gap_log["minutes_missing_after"] if gap_log else None,
                               "measured_internal_incl_exchange_minutes": internal_incl * 5,
                               "note": "the gap-fill log counts holes inside each symbol's span, including exchange gaps"},
                           "log_days_unavailable": gap_log["days_unavailable"] if gap_log else None})


def m15_mark(src: Sources, cov: dict | None, units) -> Metric:
    return _ohlc_metric(src, "M15", "markPriceKlines", "mark-price", cov, units)


def m16_premium(src: Sources, cov: dict | None, units) -> Metric:
    return _ohlc_metric(src, "M16", "premiumIndexKlines", "premium-index", cov, units)


def ohlc_families_cov(src: Sources, units) -> dict | None:
    syms = sorted({s for s, _ in units})
    if not syms:
        return None
    return ohlc_coverage(src, ("markPriceKlines", "premiumIndexKlines"), syms)


# ---------------------------------------------------------------- remaining klines gap minutes, classified

GAP_DAY_NS = 86_400 * 10**9
LIQUID_QV_30D = 20_000_000.0


def classify_gap_slots(cnt: np.ndarray, act: np.ndarray, edge: np.ndarray, single_archive_gap: np.ndarray) -> np.ndarray:
    """Class per missing 5m slot. cnt: gap symbols missing at that slot; act: symbols whose span covers it;
    edge: slot within a day of the symbol's first or last bar; single_archive_gap: the symbol-month had a day
    unavailable in the daily archive. c allows one or two symbols at the slot (a single series or a settled-alias
    pair absent from the archives). Returns an array of 'a', 'b', 'c', 'd'."""
    out = np.full(cnt.shape, "d", dtype="<U1")
    a = (act >= 10) & (cnt >= 0.5 * np.maximum(act, 1))
    b = ~a & edge
    c = ~a & ~b & (cnt <= 2) & single_archive_gap
    out[a] = "a"
    out[b] = "b"
    out[c] = "c"
    return out


def trailing_median_qv(daily_qv: dict[int, float], day: int, window: int = 30, min_obs: int = 20) -> float | None:
    vals = [daily_qv[d] for d in range(day - window, day) if d in daily_qv]
    if len(vals) < min_obs:
        return None
    return float(np.median(vals))


def gap_classes(src: Sources) -> dict | None:
    """Classify the remaining klines_1m gap minutes (the daily gap-fill log) by symbol-month, at 5m-slot level.
    One 5m slot is 5 minutes; a partial bar contributes 5 - n_src minutes and an absent slot contributes 5. The grid
    reproduces the log's minutes_missing_after exactly (checked against 506,964)."""
    log = src.lake / "_issues" / "gapfill_daily_klines_1m.parquet"
    if not log.exists():
        return None
    five_root = src.lake / "klines" / "5m"
    rows = pl.read_parquet(log, columns=["symbol", "month", "days_unavailable"])
    spans: dict[str, tuple[int, int]] = {}
    for sym_dir in sorted(p for p in five_root.iterdir() if p.is_dir()) if five_root.exists() else []:
        files = sorted(sym_dir.glob("*.parquet"))
        if files:
            lo = int(pl.read_parquet(files[0], columns=["ts"])["ts"].min())
            hi = int(pl.read_parquet(files[-1], columns=["ts"])["ts"].max())
            spans[sym_dir.name] = (lo, hi)
    s_lo = np.array(sorted(v[0] for v in spans.values()), dtype=np.int64)
    s_hi = np.array(sorted(v[1] for v in spans.values()), dtype=np.int64)
    slot_sym, slot_month, slot_ts, slot_min, slot_unav = [], [], [], [], []
    for sym, month, unav in rows.iter_rows():
        if sym not in spans:
            continue
        lo, hi = spans[sym]
        y, mo = int(month[:4]), int(month[5:7])
        m0 = int(np.datetime64(f"{y:04d}-{mo:02d}-01", "ns").astype(np.int64))
        m1 = int(np.datetime64(f"{y + (mo == 12):04d}-{mo % 12 + 1:02d}-01", "ns").astype(np.int64))
        grid = np.arange(m0, m1, FIVE_MIN_NS, dtype=np.int64)
        grid = grid[(grid >= lo) & (grid <= hi)]
        f = five_root / sym / f"{month}.parquet"
        if f.exists():
            pf = pl.read_parquet(f, columns=["ts", "n_src"])
            present = pf["ts"].to_numpy().astype(np.int64)
            nsrc = pf["n_src"].to_numpy().astype(np.int64)
            hit = np.isin(present, grid)
            partial = np.clip(5 - nsrc[hit], 0, 5)
            pts = present[hit]
            absent = grid[~np.isin(grid, present)]
            ts_all = np.concatenate([pts[partial > 0], absent])
            min_all = np.concatenate([partial[partial > 0], np.full(absent.size, 5, dtype=np.int64)])
        else:
            ts_all = grid
            min_all = np.full(grid.size, 5, dtype=np.int64)
        if ts_all.size == 0:
            continue
        slot_sym += [sym] * ts_all.size
        slot_month += [month] * ts_all.size
        slot_ts.append(ts_all)
        slot_min.append(min_all)
        slot_unav += [int(unav > 0)] * ts_all.size
    if not slot_ts:
        return {"minutes_total": 0}
    ts = np.concatenate(slot_ts)
    minutes = np.concatenate(slot_min)
    sym_arr = np.array(slot_sym)
    month_arr = np.array(slot_month)
    unav_arr = np.array(slot_unav, dtype=bool)
    uniq, inv, counts = np.unique(ts, return_inverse=True, return_counts=True)
    cnt = counts[inv]
    act_u = np.searchsorted(s_lo, uniq, side="right") - np.searchsorted(s_hi, uniq, side="left")
    act = act_u[inv]
    lo_sym = np.array([spans[s][0] for s in sym_arr], dtype=np.int64)
    hi_sym = np.array([spans[s][1] for s in sym_arr], dtype=np.int64)
    edge = (ts - lo_sym < GAP_DAY_NS) | (hi_sym - ts < GAP_DAY_NS)
    cls = classify_gap_slots(cnt, act, edge, unav_arr)
    summary = {"minutes_total": int(minutes.sum()), "symbol_months": int(len(set(zip(sym_arr, month_arr)))),
               "symbols": int(len(set(sym_arr))), "classes": {}}
    for k in ("a", "b", "c", "d"):
        m = cls == k
        pairs = set(zip(sym_arr[m], month_arr[m]))
        summary["classes"][k] = {"minutes": int(minutes[m].sum()), "symbol_months": len(pairs)}
    # class a: exchange-wide, with BTCUSDT check
    a_mask = cls == "a"
    btc = sym_arr == "BTCUSDT"
    btc_ts = set(ts[btc].tolist())
    d_mask = cls == "d"
    summary["diag_class_d"] = {
        "cnt_hist": {int(k): int(v) for k, v in zip(*np.unique(cnt[d_mask], return_counts=True))},
        "active_median": float(np.median(act[d_mask])) if d_mask.any() else None,
        "symbols_top10": [{"symbol": k, "minutes": int(v)} for k, v in sorted(
            ((str(sy), int(minutes[d_mask & (sym_arr == sy)].sum())) for sy in set(sym_arr[d_mask])),
            key=lambda kv: -kv[1])[:10]],
        "days_unavailable_slots": int((d_mask & unav_arr).sum())}
    alias_c = np.char.endswith(sym_arr.astype(str), "SETTLED") & (cls == "c")
    summary["class_c_alias_settled_minutes"] = int(minutes[alias_c].sum())
    summary["class_c_symbols"] = {str(s_): int(minutes[(cls == "c") & (sym_arr == s_)].sum())
                                  for s_ in sorted(set(sym_arr[(cls == "c")]))}
    summary["class_a_btc_missing_slots"] = int(sum(1 for x in ts[a_mask] if int(x) in btc_ts))
    summary["class_a_median_symbols_missing_per_slot"] = float(np.median(cnt[a_mask])) if a_mask.any() else None
    # class c: liquidity at the slot's day (trailing 30-day median daily quote volume >= 20M)
    c_mask = cls == "c"
    c_syms = sorted(set(sym_arr[c_mask]))
    liquid_minutes, liquid_pairs, liquid_checked = 0, set(), 0
    for sym in c_syms:
        fs = sorted(glob.glob(str(five_root / sym / "*.parquet")))
        q = pl.concat([pl.read_parquet(f, columns=["ts", "quote_volume"]) for f in fs])
        day = (q["ts"] // GAP_DAY_NS).to_numpy()
        qv = q["quote_volume"].to_numpy()
        uniq_days, inv_d = np.unique(day, return_inverse=True)
        sums = np.bincount(inv_d, weights=qv)
        daily = {int(d): float(v) for d, v in zip(uniq_days, sums)}
        sel = np.flatnonzero(c_mask & (sym_arr == sym))
        for i in sel:
            liquid_checked += 1
            d_ = int(ts[i] // GAP_DAY_NS)
            med = trailing_median_qv(daily, d_)
            if med is not None and med >= LIQUID_QV_30D:
                liquid_minutes += int(minutes[i])
                liquid_pairs.add((sym, str(month_arr[i])))
    summary["class_c_liquid"] = {"threshold_qv_30d": LIQUID_QV_30D, "minutes": liquid_minutes,
                                 "symbol_months": len(liquid_pairs), "symbols_checked": len(c_syms),
                                 "symbol_months_list": sorted(liquid_pairs)[:50]}
    pair_min: dict[tuple[str, str], int] = {}
    pair_cls: dict[tuple[str, str], dict] = {}
    for s_, m_, mi, k in zip(sym_arr, month_arr, minutes, cls):
        key = (str(s_), str(m_))
        pair_min[key] = pair_min.get(key, 0) + int(mi)
        dd = pair_cls.setdefault(key, {"a": 0, "b": 0, "c": 0, "d": 0})
        dd[str(k)] += int(mi)
    top = sorted(pair_min.items(), key=lambda kv: -kv[1])[:10]
    summary["top10_symbol_months"] = [{"symbol": k[0], "month": k[1], "minutes": v, **pair_cls[k]} for k, v in top]
    return summary


# ---------------------------------------------------------------- assembly

OWNER_FIXABLE = ("owner-decision", "paid-only")
ROUND1_ALIAS = {"M3a": "M3", "M3b": "M3"}  # round 1 scored hard and soft rules together


def fix_class(m: Metric) -> str:
    """free-data: a free source or a code change fixes it. owner/paid: needs the owner or a paid source."""
    return "free-data" if m.fixable_by in ("free-source", "code") else "owner/paid"


def status_counts(metrics: list[Metric]) -> dict[str, int]:
    """green, amber, red and not_measured; proven and waived are reported apart and are in neither set."""
    counts = {k: sum(1 for m in metrics if m.status == k) for k in ("green", "amber", "red", "not_measured")}
    counts["proven"] = sum(1 for m in metrics if m.status == PROVEN)
    counts["waived"] = sum(1 for m in metrics if m.status == WAIVED)
    return counts


def apply_proofs(metrics: list[Metric], path: Path, root: Path = paths.ROOT) -> list[Metric]:
    """Red/amber metrics with a standing proof (value <= max_value, evidence file present) become PROVEN; the computed
    status and the proof stay in the detail."""
    if not path.exists():
        return metrics
    proofs = _load_json(path)
    out = []
    for m in metrics:
        p = proofs.get(m.id)
        if (p and m.status in ("red", "amber") and m.value is not None and m.value <= p["max_value"]
                and (root / p["evidence"]).exists()):
            m = replace(m, status=PROVEN, reason=f"proven unfixable from free sources: {p['ruling']}",
                        detail={**m.detail, "computed_status": m.status, "proof": p})
        out.append(m)
    return out


def _worst_items(metrics: list[Metric]) -> list[dict]:
    """Top five open items an automated loop can act on (owner and paid items are listed apart)."""
    open_ = [m for m in metrics if m.status not in ("green", WAIVED, PROVEN) and m.fixable_by not in OWNER_FIXABLE]
    open_.sort(key=lambda m: (IMPACT.get(m.id, 9), FIXABLE_ORDER.get(m.fixable_by, 9),
                              STATUS_ORDER.get(m.status, 9), m.id))
    return [{"id": m.id, "name": m.name, "status": m.status, "impact": IMPACT.get(m.id, 9),
             "fixable_by": m.fixable_by, "fix_class": fix_class(m), "fix": m.fix} for m in open_[:5]]


def _owner_list(metrics: list[Metric]) -> list[dict]:
    items = [m for m in metrics if m.status not in ("green", WAIVED, PROVEN) and m.fixable_by in OWNER_FIXABLE]
    items.sort(key=lambda m: (IMPACT.get(m.id, 9), m.id))
    return [{"id": m.id, "name": m.name, "status": m.status, "fixable_by": m.fixable_by,
             "fix_class": "owner/paid", "fix": m.fix} for m in items]


def diff_rows(base_card: dict, metrics: list[Metric]) -> list[dict]:
    base = {m["id"]: m for m in base_card["metrics"]}
    rows = []
    for m in metrics:
        b = base.get(m.id)
        note = ""
        if b is None and m.id in ROUND1_ALIAS:
            b = base.get(ROUND1_ALIAS[m.id])
            note = "round-1 combined M3"
        if b is None:
            note = "new in round 2"
        bv, v = b["value"] if b else None, m.value
        change = ""
        if isinstance(bv, (int, float)) and isinstance(v, (int, float)):
            change = f"{v - bv:+.6g}"
        rows.append({"id": m.id, "name": m.name, "round1_value": bv,
                     "round1_status": b["status"] if b else "-", "round2_value": v, "round2_status": m.status,
                     "change": change, "note": note})
    return rows


def merge_windows(df: pl.DataFrame) -> pl.DataFrame:
    """Join contiguous rows of one symbol and reason into one window; magnitude is the window maximum."""
    if df.height == 0:
        return df
    df = df.sort(["symbol", "reason", "start_ts"])
    new_run = ((pl.col("symbol") != pl.col("symbol").shift()) | (pl.col("reason") != pl.col("reason").shift())
               | (pl.col("start_ts") - pl.col("end_ts").shift() > 0)).fill_null(True)
    return (df.with_columns(new_run.cum_sum().alias("run"))
            .group_by("run", "symbol", "reason", maintain_order=True)
            .agg(pl.col("start_ts").min(), pl.col("end_ts").max(), pl.col("magnitude_bps").max())
            .select("symbol", "start_ts", "end_ts", "reason", "magnitude_bps"))


def run(src: Sources, baseline: Path | None = None, label: str = "") -> tuple[dict, pl.DataFrame]:
    t0 = time.monotonic()
    units = klines_units(src)
    ins = read_instruments(src)
    scope = in_scope(ins)
    fund_units = read_done(src.lake, "funding")
    metric_units = read_done(src.lake, "metrics")
    panel = panel_scan(src, ins)
    m4, m4_queue, m6, m6_queue = panel["m4"], panel["m4_queue"], panel["m6"], panel["m6_queue"]
    # spike windows by symbol chunks: each chunk keeps only its top QUEUE_CAP rows (the global top rows are inside
    # the union of the chunks' tops), so the full spike set (millions of rows) is never held at once
    spike_schema = {"symbol": pl.Utf8, "start_ts": pl.Int64, "end_ts": pl.Int64, "reason": pl.Utf8,
                    "magnitude_bps": pl.Float64}
    syms_all = sorted({s_ for s_, _ in units})
    spike_tops, spike_total = [], 0
    for i in range(0, len(syms_all), COVERAGE_CHUNK):
        part = set(syms_all[i:i + COVERAGE_CHUNK])
        sp = merge_windows(spike_windows(src, [u for u in units if u[0] in part], scope,
                                         use_issues_cache=False))
        spike_total += sp.height
        spike_tops.append(sp.sort("magnitude_bps", descending=True).head(QUEUE_CAP))
    spikes_q = pl.concat(spike_tops) if spike_tops else pl.DataFrame(schema=spike_schema)
    m6_q = merge_windows(m6_queue)
    queue = pl.concat([spikes_q, m4_queue, m6_q])
    queue_total = spike_total + m4_queue.height + m6_q.height
    queue = queue.sort("magnitude_bps", descending=True).head(QUEUE_CAP)
    timings: dict[str, float] = {"spikes_and_queue": round(time.monotonic() - t0, 2)}
    metrics: list[Metric] = [m4, panel["m5"], m6]

    def timed(label_: str, fn):
        start = time.monotonic()
        out = fn()
        metrics.extend(out if isinstance(out, list) else [out])
        timings[label_] = round(time.monotonic() - start, 2)

    cov_box: dict = {}

    def ohlc():
        cov_box["cov"] = ohlc_families_cov(src, units)
        return [m15_mark(src, cov_box["cov"], units), m16_premium(src, cov_box["cov"], units)]

    timed("M1", lambda: m1_coverage(src, units, ins))
    timed("M2", lambda: m2_checksum(src, units))
    timed("M3", lambda: m3_rules(src, units, ins))
    timed("M7", lambda: m7_verification(src))
    timed("M8", lambda: m8_funding(src, fund_units))
    timed("M9", lambda: m9_metrics(src, metric_units))
    timed("M10", lambda: m10_mapping(src))
    timed("M11", lambda: m11_pit(src))
    timed("M12", lambda: m12_spread(src))
    timed("M13", lambda: m13_context(src))
    timed("M14", lambda: m14_compliance(src))
    timed("M15_M16", ohlc)
    metrics.sort(key=lambda m: (int(re.sub(r"\D", "", m.id)), m.id))
    metrics = apply_proofs(metrics, src.proofs)
    counts = status_counts(metrics)
    card = {
        "label": label,
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "runtime_s": round(time.monotonic() - t0, 2),
        "runtime_by_part_s": timings,
        "lake_completeness": lake_completeness(src, units),
        "ever_liquid": ever_liquid_summary(src),
        "gap_classes": gap_classes(src),
        "targets": TARGETS,
        "inputs": INPUTS,
        "counts": counts,
        "suspicious_windows": {"queued": queue.height, "before_cap": queue_total, "cap": QUEUE_CAP,
                               "by_reason": dict(zip(*queue.group_by("reason").len().to_dict(as_series=False)
                                                     .values())) if queue.height else {}},
        "worst_items": _worst_items(metrics),
        "owner_list": _owner_list(metrics),
        "metrics": [asdict(m) for m in metrics],
    }
    if baseline is not None:
        card["baseline"] = str(baseline)
        card["diff_vs_baseline"] = diff_rows(_load_json(baseline), metrics)
    return card, queue


def _fmt(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)


def render_md(card: dict) -> str:
    lc = card["lake_completeness"]
    head = f"# Data-quality scorecard {card.get('label', '')}".rstrip()
    lines = [f"{head} ({card['generated_utc']})", "",
             f"Lake: {lc.get('symbol_months_done')} symbol-months done over {lc.get('symbols_done')} symbols; "
             f"fraction of inventory {_fmt(lc.get('fraction'))}. Runtime {card['runtime_s']} s. "
             f"Counts: {card['counts']}.", "",
             "| id | metric | value | unit | status | fixable_by |", "|---|---|---|---|---|---|"]
    for m in card["metrics"]:
        lines.append(f"| {m['id']} | {m['name']} | {_fmt(m['value'])} | {m['unit']} | {m['status']} | "
                     f"{m['fixable_by']} |")
    lines += ["", "## Worst items (top 5, not owner or paid)", "",
              "| # | id | status | fix class | proposed fix |", "|---|---|---|---|---|"]
    lines += [f"| {i} | {w['id']} | {w['status']} | {w['fix_class']} | {w['fix']} |"
              for i, w in enumerate(card["worst_items"], 1)]
    lines += ["", "## Owner or paid list (not in worst items)", ""]
    lines += [f"- {w['id']} ({w['status']}, {w['fix_class']}): {w['fix']}" for w in card["owner_list"]]
    if card.get("diff_vs_baseline"):
        lines += ["", f"## Diff vs baseline ({card.get('baseline', '')})", "",
                  "| id | metric | round 1 | status | round 2 | status | change | note |",
                  "|---|---|---|---|---|---|---|---|"]
        for r in card["diff_vs_baseline"]:
            lines.append(f"| {r['id']} | {r['name']} | {_fmt(r['round1_value'])} | {r['round1_status']} | "
                         f"{_fmt(r['round2_value'])} | {r['round2_status']} | {r['change']} | {r['note']} |")
    nm = [m for m in card["metrics"] if m["status"] == "not_measured"]
    if nm:
        lines += ["", "## Not measured", ""] + [f"- {m['id']}: {m['reason']}" for m in nm]
    return "\n".join(lines) + "\n"


def _json_default(o):
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return None if not np.isfinite(o) else float(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, float) and not np.isfinite(o):
        return None
    raise TypeError(f"not serialisable: {type(o).__name__}")


def write_outputs(card: dict, queue: pl.DataFrame, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dated = out_dir / f"scorecard_{stamp}.json"
    text = json.dumps(card, indent=2, default=_json_default)
    dated.write_text(text, encoding="utf-8")
    (out_dir / "scorecard_latest.json").write_text(text, encoding="utf-8")
    (out_dir / "scorecard_latest.md").write_text(render_md(card), encoding="utf-8")
    queue.write_parquet(out_dir / "suspicious_windows.parquet", compression="zstd")
    return dated


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m quant.data.quality", description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", type=Path, default=None)
    ap.add_argument("--baseline", type=Path, default=None, help="earlier scorecard json for the diff table")
    ap.add_argument("--label", default="", help="round label written into the report heading")
    args = ap.parse_args(argv)
    src = Sources() if args.out_dir is None else Sources(out_dir=args.out_dir)
    card, queue = run(src, baseline=args.baseline, label=args.label)
    written = write_outputs(card, queue, src.out_dir)
    print(f"wrote {written}")
    print(json.dumps({"counts": card["counts"], "worst_items": [w["id"] for w in card["worst_items"]],
                      "runtime_s": card["runtime_s"], "queued": queue.height}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
