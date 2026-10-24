#!/usr/bin/env python3
"""Build a manifest of a parquet lake from file metadata only (footers), never from the data pages.

    python tools/build_manifest.py /path/to/lake --out manifest/manifest.json

For every partition: file count, rows (sum of footer num_rows), bytes, schema hashes, min/max of the time column
from row-group statistics, and a sha256 over the sorted (relative path, size) listing. The whole lake gets the same
listing hash, so the lake's shape is pinned by one string. A gzipped per-file listing (path, bytes, rows) is written
next to the manifest unless --no-listing is given.

Partition rule (deterministic, see manifest/README.md):
  * file stem is a date (YYYY, YYYY-MM, YYYY-MM-DD) two or more folders deep: the file sits in a per-symbol folder;
    partition = the folder above the symbol folder, key = symbol. One folder deep: partition = that folder, key = date.
  * otherwise, when its folder holds >= KEYED_MIN parquet files or the stem is a ticker (ends in USDT, USDC, BUSD,
    USD): partition = the folder, key = file stem.
  * otherwise the file is a single table: partition = folder/stem.
Category: "auxiliary" when a path part starts with "_" or is one of AUX_PARTS (intermediate parts, issue logs,
verification verdicts, bookkeeping; some duplicate rows held elsewhere), else "market_data" for keyed per-symbol
series and "table" for single tables. "primary" totals = market_data + table, so intermediate copies are not double
counted.

Needs only pyarrow. Stdlib otherwise.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow.parquet as pq

DATE_STEM = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")
SYMBOL_STEM = re.compile(r"^\S+(USDT|USDC|BUSD|USD)$")
KEYED_MIN = 20
AUX_PARTS = {"verify", "completeness"}
TIME_COLUMNS = ("ts", "start_ts", "ts_ms", "day_ns")


def list_parquet(root: Path, exclude: tuple[str, ...] = ()) -> list[tuple[str, int]]:
    """Sorted (relative posix path, size) for every *.parquet under root. Symlinks are not followed.
    `exclude`: relative directory prefixes to skip entirely."""
    out: list[tuple[str, int]] = []
    stack = [root]
    while stack:
        d = stack.pop()
        with os.scandir(d) as it:
            for e in it:
                if e.is_dir(follow_symlinks=False):
                    if Path(e.path).relative_to(root).as_posix() in exclude:
                        continue
                    stack.append(Path(e.path))
                elif e.name.endswith(".parquet") and e.is_file(follow_symlinks=False):
                    out.append((Path(e.path).relative_to(root).as_posix(), e.stat(follow_symlinks=False).st_size))
    out.sort()
    return out


def listing_sha256(items) -> str:
    h = hashlib.sha256()
    for rel, size in items:
        h.update(f"{rel}\t{size}\n".encode())
    return h.hexdigest()


def partition_of(rel: str, dir_counts: dict[str, int]) -> tuple[str, str]:
    """(partition, key) for one relative path; see the module docstring."""
    p = rel.split("/")
    folder, stem = "/".join(p[:-1]), p[-1][: -len(".parquet")]
    if DATE_STEM.match(stem) and len(p) >= 3:
        return ("/".join(p[:-2]), p[-2])
    if DATE_STEM.match(stem):
        return (folder or ".", stem)  # dated files directly in a dataset folder (e.g. bars_5m/2024.parquet)
    if dir_counts[folder] >= KEYED_MIN or SYMBOL_STEM.match(stem):
        return (folder or ".", stem)
    return ((folder + "/" if folder else "") + stem, "")


def category_of(partition: str, key: str) -> str:
    parts = partition.split("/")
    if any(x.startswith("_") for x in parts) or AUX_PARTS.intersection(parts):
        return "auxiliary"
    return "market_data" if key else "table"


def schema_hash(schema) -> str:
    text = ";".join(f"{f.name}:{f.type}" for f in schema)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def to_iso(v, col: str) -> str | None:
    if v is None:
        return None
    v = int(v)
    if col == "ts_ms" or v < 10**14:
        ns = v * 1_000_000
    else:
        ns = v
    return dt.datetime.fromtimestamp(ns / 1e9, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_int(v):
    """Row-group statistic as an int (epoch units of the column); datetime-like values become epoch ns."""
    if isinstance(v, (int,)) and not isinstance(v, bool):
        return v
    if hasattr(v, "timestamp"):
        if getattr(v, "tzinfo", None) is None and hasattr(v, "replace"):
            v = v.replace(tzinfo=dt.timezone.utc)
        return int(v.timestamp() * 1_000_000_000) if hasattr(v, "timestamp") else None
    return None


def read_footer(path: Path) -> dict:
    md = pq.ParquetFile(path).metadata
    schema = md.schema.to_arrow_schema()
    names = schema.names
    tcol = next((c for c in TIME_COLUMNS if c in names), None)
    tmin = tmax = None
    if tcol is not None:
        idx = names.index(tcol)
        # map arrow field index to the parquet leaf column index (flat schemas: identical)
        for rg in range(md.num_row_groups):
            st = md.row_group(rg).column(idx).statistics
            if st is None or not st.has_min_max:
                continue
            lo, hi = _as_int(st.min), _as_int(st.max)
            if lo is None or hi is None:
                continue
            tmin = lo if tmin is None else min(tmin, lo)
            tmax = hi if tmax is None else max(tmax, hi)
    return {"rows": md.num_rows, "schema": schema_hash(schema), "schema_text": [f"{f.name}:{f.type}" for f in schema],
            "tcol": tcol, "tmin": tmin, "tmax": tmax}


def build(root: Path, workers: int = 8, progress: bool = True, exclude: tuple[str, ...] = ()) -> tuple[dict, list[tuple[str, int, int]]]:
    root = root.resolve()
    files = list_parquet(root, exclude)
    dir_counts: dict[str, int] = defaultdict(int)
    for rel, _ in files:
        dir_counts[rel.rsplit("/", 1)[0] if "/" in rel else ""] += 1

    def job(item):
        return read_footer(root / item[0])

    footers = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for i, f in enumerate(ex.map(job, files, chunksize=64)):
            footers.append(f)
            if progress and (i + 1) % 10000 == 0:
                print(f"  footers read: {i + 1}/{len(files)}", file=sys.stderr)

    parts: dict[str, dict] = {}
    schemas: dict[str, list[str]] = {}
    listing = []
    for (rel, size), f in zip(files, footers):
        part, key = partition_of(rel, dir_counts)
        d = parts.setdefault(part, {"category": category_of(part, key), "files": 0, "rows": 0, "bytes": 0,
                                    "keys": set(), "schema_hashes": defaultdict(int), "time_column": None,
                                    "ts_min": None, "ts_max": None, "_listing": []})
        d["files"] += 1
        d["rows"] += f["rows"]
        d["bytes"] += size
        if key:
            d["keys"].add(key)
        d["schema_hashes"][f["schema"]] += 1
        schemas.setdefault(f["schema"], f["schema_text"])
        if f["tcol"]:
            d["time_column"] = d["time_column"] or f["tcol"]
            if f["tmin"] is not None:
                d["ts_min"] = f["tmin"] if d["ts_min"] is None else min(d["ts_min"], f["tmin"])
                d["ts_max"] = f["tmax"] if d["ts_max"] is None else max(d["ts_max"], f["tmax"])
        d["_listing"].append((rel, size))
        listing.append((rel, size, f["rows"]))

    out_parts = {}
    for name in sorted(parts):
        d = parts[name]
        out_parts[name] = {
            "category": d["category"],
            "files": d["files"],
            "rows": d["rows"],
            "bytes": d["bytes"],
            "keys": len(d["keys"]),
            "schema_hashes": dict(sorted(d["schema_hashes"].items())),
            "time_column": d["time_column"],
            "ts_min": to_iso(d["ts_min"], d["time_column"] or ""),
            "ts_max": to_iso(d["ts_max"], d["time_column"] or ""),
            "listing_sha256": listing_sha256(d["_listing"]),
        }

    def total(cat=None, source=None):
        sel = [v for k, v in out_parts.items()
               if (cat is None or v["category"] == cat) and (source is None or k.split("/")[0] == source)]
        return {"files": sum(v["files"] for v in sel), "rows": sum(v["rows"] for v in sel),
                "bytes": sum(v["bytes"] for v in sel)}

    def add(a, b):
        return {k: a[k] + b[k] for k in a}

    sources = sorted({k.split("/")[0] for k in out_parts if "/" in k})
    manifest = {
        "format": "parquet-lake-manifest/1",
        "generator": "tools/build_manifest.py",
        "method": "parquet footers only (num_rows, schema, row-group min/max statistics); no data pages read",
        "pyarrow": __import__("pyarrow").__version__,
        "totals": {"all_parquet": total(), "primary": add(total("market_data"), total("table")),
                   "market_data": total("market_data"), "table": total("table"), "auxiliary": total("auxiliary")},
        "by_source": {s: {"all_parquet": total(source=s), "market_data": total("market_data", s),
                          "table": total("table", s), "auxiliary": total("auxiliary", s)} for s in sources},
        "excluded_dirs": sorted(exclude),
        "lake_listing_sha256": listing_sha256(files),
        "partitions": out_parts,
        "schemas": dict(sorted(schemas.items())),
    }
    return manifest, listing


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("lake_root", type=Path)
    ap.add_argument("--out", type=Path, required=True, help="manifest.json path")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--exclude", action="append", default=[], help="relative directory to skip (repeatable)")
    ap.add_argument("--no-listing", action="store_true", help="skip the gzipped per-file listing")
    a = ap.parse_args(argv)
    manifest, listing = build(a.lake_root, a.workers, exclude=tuple(a.exclude))
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(manifest, indent=1) + "\n")
    if not a.no_listing:
        lp = a.out.with_name(a.out.stem + "_files.tsv.gz")
        with gzip.GzipFile(lp, "wb", mtime=0) as g:
            g.write(b"path\tbytes\trows\n")
            for rel, size, rows in listing:
                g.write(f"{rel}\t{size}\t{rows}\n".encode())
    t = manifest["totals"]
    print(json.dumps({"lake_listing_sha256": manifest["lake_listing_sha256"], **t}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
