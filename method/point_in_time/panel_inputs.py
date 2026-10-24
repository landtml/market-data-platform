"""Point-in-time input helpers for the staged panel rebuild (used by quant.data.panel).

Every helper here maps a source to a per-bar column on a grid of bar opens `t` (ns). The rule is one rule for all of
them: a value is visible at bar t only when its usable_from (publish time) is <= t. Each column is a pure function of
its inputs, so tests/bias_checks/test_panel_rebuild_*.py can perturb inputs after t and check that the column at t
does not move.

* REST backfill (lake/binance_um/rest): the archive wins where its row exists; REST only fills rows the archive lacks.
* Binance OI metrics: stamp t is the snapshot at t (bar end), usable at t. A zero open interest on a live perp
  is a bad value: each zero column is null, never a reading of zero.
* Index and spot 5m klines: the kline opened at u closes at u + 5m - 1 ms, so it is usable at row u + 5m. The row
  at t carries the close of the kline opened at t - 5m.
* Bybit funding and 1h OI: usable_from_ts, asof backward (last value published at or before t).
* Delist announcements: the instrument flag is true at every bar t >= the earliest publish time of a mapped futures
  delist notice.
* Bad-print consensus (lake/verify_consensus.parquet): windows classed 'refuted' make bars non-tradable, 'disputed'
  is a flag only; every other class leaves the flags alone.
* Auxiliary files (context_ext_daily, tick_history, announcements) are copied into the staged set unchanged; each
  carries its own publish column (see AUX_USABLE).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

BAR_NS = 5 * 60 * 1_000_000_000

# Publish-time column of each auxiliary file copied into the staged set (aux/<name>).
AUX_USABLE = {
    "context_ext_daily.parquet": "usable_from",
    "tick_history.parquet": "capture_ts",
    "announcements.parquet": "release_ts",
}


def rest_fill(archive: pl.DataFrame | None, rest: pl.DataFrame | None,
              key: tuple[str, ...] = ("instrument_id", "ts")) -> tuple[pl.DataFrame | None, int]:
    """Archive rows win; REST rows are added only where the archive has no row for the key. Returns (frame, added)."""
    if rest is None or rest.height == 0:
        return archive, 0
    if archive is None or archive.height == 0:
        return rest, rest.height
    add = rest.join(archive.select(list(key)), on=list(key), how="anti")
    if add.height == 0:
        return archive, 0
    cols = archive.columns
    out = pl.concat([archive, add.select(cols)], how="vertical")
    return out, add.height


def kline_close_on_next_bar(df: pl.DataFrame, name: str, iid: str) -> pl.DataFrame:
    """Close of the 5m kline opened at u, placed on row u + 5m (the first bar at which it is published).

    df: raw kline frame with ts (open, ns) and close. The close is published at close_ts < u + 5m, so row t sees the
    kline opened at t - 5m and never the one opened at t.
    """
    return df.select(
        (pl.col("ts").cast(pl.Int64) + BAR_NS).alias("ts"),
        pl.lit(iid).alias("instrument_id"),
        pl.col("close").cast(pl.Float64).alias(name),
    )


def oi_rows(df: pl.DataFrame, iid: str) -> pl.DataFrame:
    """Binance OI metrics: stamp t is usable at row t (snapshot at the bar end, see module docstring).

    A zero in either OI column is a bad value on a live perp and becomes null (see module docstring).
    """
    def _nonzero(col: str) -> pl.Expr:
        v = pl.col(col).cast(pl.Float64)
        return pl.when(v != 0).then(v).otherwise(None)

    return df.select(
        pl.col("ts").cast(pl.Int64),
        pl.lit(iid).alias("instrument_id"),
        _nonzero("sum_open_interest").alias("oi_sum_open_interest"),
        _nonzero("sum_open_interest_value").alias("oi_sum_open_interest_value"),
    )


def asof_value(grid: pl.DataFrame, events: pl.DataFrame, out: str) -> pl.Series:
    """Last event with usable <= t for each (instrument_id, ts) row of grid, in grid row order.

    grid: columns ts (Int64) and instrument_id, sorted by ts. events: columns instrument_id, usable (Int64), value.
    Rows with no earlier event are null.
    """
    ev = events.select(["instrument_id", "usable", "value"]).sort("usable")
    left = grid.select(["ts", "instrument_id"]).with_row_index("_row").sort("ts")
    j = left.join_asof(ev, left_on="ts", right_on="usable", by="instrument_id", strategy="backward")
    return j.sort("_row").get_column("value").alias(out)


def bybit_events(df: pl.DataFrame) -> pl.DataFrame:
    """Bybit funding or 1h OI, keyed by the Binance instrument id: (instrument_id, usable, value)."""
    return df.select(
        pl.col("binance_instrument_id").alias("instrument_id"),
        pl.col("usable_from_ts").cast(pl.Int64).alias("usable"),
        pl.col("value").cast(pl.Float64).alias("value"),
    )


def delist_table(announcements: pl.DataFrame) -> pl.DataFrame:
    """instrument_id -> earliest publish time (release_ts, ns) of a mapped futures delist notice."""
    a = announcements.filter((pl.col("action") == "delist") & (pl.col("market") == "futures")
                             & (pl.col("instrument_map_status") == "mapped"))
    # w-meta joins several ids with '|' (announcements.parquet); ',' is accepted too
    a = a.with_columns(pl.col("instrument_ids").str.replace_all(",", "|").str.split("|")).explode("instrument_ids").with_columns(
        pl.col("instrument_ids").str.strip_chars().alias("instrument_id"))
    a = a.filter(pl.col("instrument_id") != "")
    return a.group_by("instrument_id").agg(pl.col("release_ts").cast(pl.Int64).min().alias("delist_from"))


def delist_column(ts: pl.Series, delist_from: pl.Series) -> pl.Series:
    """True where ts >= delist_from (null delist_from = never announced)."""
    return (ts >= delist_from).fill_null(False).alias("delist_announced")


def consensus_windows(path: Path) -> dict[str, pl.DataFrame]:
    """Bad-print consensus windows by class: 'refuted' and 'disputed', each as (symbol, start, end).

    A MARKET window applies to every symbol. Rows with other classes are not returned (they keep the spike flag).
    """
    if not Path(path).exists():
        empty = pl.DataFrame(schema={"symbol": pl.Utf8, "start": pl.Int64, "end": pl.Int64})
        return {"refuted": empty, "disputed": empty}
    v = pl.read_parquet(path, columns=["symbol", "start_ts", "end_ts", "consensus"])
    out = {}
    for cls in ("refuted", "disputed"):
        w = v.filter(pl.col("consensus") == cls).select(
            pl.col("symbol").cast(pl.Utf8), pl.col("start_ts").cast(pl.Int64).alias("start"),
            pl.col("end_ts").cast(pl.Int64).alias("end"))
        out[cls] = w
    return out


def consensus_confirmed(path: Path) -> pl.DataFrame:
    """Bad-print consensus windows of class 'confirmed' (symbol, start, end). Used to exempt single-bar prints."""
    if not Path(path).exists():
        return pl.DataFrame(schema={"symbol": pl.Utf8, "start": pl.Int64, "end": pl.Int64})
    v = pl.read_parquet(path, columns=["symbol", "start_ts", "end_ts", "consensus"])
    return v.filter(pl.col("consensus") == "confirmed").select(
        pl.col("symbol").cast(pl.Utf8), pl.col("start_ts").cast(pl.Int64).alias("start"),
        pl.col("end_ts").cast(pl.Int64).alias("end"))


def aux_names() -> list[str]:
    return sorted(AUX_USABLE)


def visible_rows(df: pl.DataFrame, usable_col: str, t: int) -> pl.DataFrame:
    """Rows of an auxiliary file a reader may use at bar t (publish time <= t)."""
    return df.filter(pl.col(usable_col) <= t)
