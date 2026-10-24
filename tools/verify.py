#!/usr/bin/env python3
"""Recompute a lake's manifest and compare it with a committed one. Exit 0 on match, 1 on any mismatch.

    python tools/verify.py --sample                          # offline, no lake, well under a minute
    python tools/verify.py /path/to/lake                     # full check against manifest/manifest.json
    python tools/verify.py /path/to/lake --fast              # path+size listing hash only (stdlib, no footers)
    python tools/verify.py /path/to/lake --partition binance_um/funding   # one partition only
    python tools/verify.py --inventory                       # print the README inventory table from the manifest

--sample runs five checks on the bundled synthetic lake (sample/lake):
  1. its manifest recomputes to sample/manifest.json exactly (files, rows, bytes, schemas, listing hashes);
  2. every sample partition has the same column schema hash as that partition in the real manifest;
  3. tamper test: a copy with one row removed from one file must FAIL the manifest check;
  4. quality scan: the 1m validator rules find exactly the planted defects in sample/EXPECTED_DEFECTS.json;
  5. 5m bars equal a fresh resample of the 1m bars (open/high/low/close from traded minutes, sums over all).
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))

COMPARE_FIELDS = ("category", "files", "rows", "bytes", "keys", "schema_hashes", "time_column", "ts_min", "ts_max",
                  "listing_sha256")


def compare(expected: dict, actual: dict, only: str | None = None) -> list[str]:
    """Differences between two manifests, as readable lines. Empty list = match."""
    diffs: list[str] = []
    ep, ap = expected["partitions"], actual["partitions"]
    names = [only] if only else sorted(set(ep) | set(ap))
    for name in names:
        if name not in ep:
            diffs.append(f"partition {name}: not in the committed manifest")
            continue
        if name not in ap:
            diffs.append(f"partition {name}: missing on disk")
            continue
        for f in COMPARE_FIELDS:
            if ep[name].get(f) != ap[name].get(f):
                diffs.append(f"partition {name}: {f} expected {ep[name].get(f)!r}, found {ap[name].get(f)!r}")
    if only is None:
        if expected["lake_listing_sha256"] != actual["lake_listing_sha256"]:
            diffs.append(f"lake listing sha256 expected {expected['lake_listing_sha256']}, "
                         f"found {actual['lake_listing_sha256']}")
        if expected["totals"] != actual["totals"]:
            diffs.append(f"totals expected {expected['totals']}, found {actual['totals']}")
    return diffs


def check_manifest(lake: Path, manifest_path: Path, only: str | None = None, quiet: bool = False) -> list[str]:
    import build_manifest as bm

    expected = json.loads(manifest_path.read_text())
    actual, _ = bm.build(lake, progress=not quiet, exclude=tuple(expected.get("excluded_dirs", [])))
    return compare(expected, actual, only)


def check_fast(lake: Path, manifest_path: Path) -> list[str]:
    import build_manifest as bm

    expected = json.loads(manifest_path.read_text())
    got = bm.listing_sha256(bm.list_parquet(lake.resolve(), tuple(expected.get("excluded_dirs", []))))
    if got != expected["lake_listing_sha256"]:
        return [f"lake listing sha256 expected {expected['lake_listing_sha256']}, found {got}"]
    return []


# ---- quality scan (the 1m validator rules: dupes, unsorted, gaps, OHLC sanity, non-positive prices, stuck runs) ----

STUCK_LIMIT = 30
MIN_NS = 60_000_000_000


def scan_1m(path: Path) -> dict:
    import numpy as np
    import pyarrow.parquet as pq

    t = pq.read_table(path, columns=["ts", "open", "high", "low", "close"]).to_pydict()
    ts = np.asarray(t["ts"], dtype=np.int64)
    o, h, lo, c = (np.asarray(t[k]) for k in ("open", "high", "low", "close"))
    d = np.diff(ts)
    out = {"unsorted": [int(ts[i + 1]) for i in np.flatnonzero(d < 0)],
           "dupe": [int(ts[i + 1]) for i in np.flatnonzero(d == 0)],
           "gap": [{"after_ts": int(ts[i]), "missing_minutes": int(d[i] // MIN_NS - 1)} for i in np.flatnonzero(d > MIN_NS)],
           "ohlc_bad": [int(ts[i]) for i in np.flatnonzero((h < np.maximum(o, c)) | (lo > np.minimum(o, c)) | (h < lo))],
           "nonpositive": [int(ts[i]) for i in np.flatnonzero((o <= 0) | (h <= 0) | (lo <= 0) | (c <= 0))]}
    stuck, run_start = [], 0
    for i in range(1, len(c) + 1):
        if i == len(c) or c[i] != c[run_start]:
            if i - run_start >= STUCK_LIMIT:
                stuck.append({"start_ts": int(ts[run_start]), "run": int(i - run_start)})
            run_start = i
    out["stuck"] = stuck
    return {k: v for k, v in out.items() if v}


def check_quality(sample_root: Path) -> list[str]:
    expected = json.loads((sample_root / "EXPECTED_DEFECTS.json").read_text())
    diffs = []
    for path in sorted((sample_root / "lake").rglob("*.parquet")):
        rel = path.relative_to(sample_root / "lake").as_posix()
        if "/klines/1m/" not in rel:
            continue
        found = scan_1m(path)
        want = expected.get(rel, {})
        if found != want:
            diffs.append(f"quality scan {rel}: expected {want}, found {found}")
    return diffs


def check_resample(sample_root: Path) -> list[str]:
    import numpy as np
    import pyarrow.parquet as pq

    import make_synthetic_sample as ms

    diffs = []
    lake = sample_root / "lake"
    for p5 in sorted(lake.glob("binance_um/klines/5m/*/*.parquet")):
        p1 = lake / "binance_um/klines/1m" / p5.parent.name / p5.name
        one = {k: np.asarray(v) for k, v in pq.read_table(p1).to_pydict().items()}
        five = pq.read_table(p5).to_pydict()
        fresh = ms.resample_5m(one)
        for k in ("ts", "open", "high", "low", "close", "count", "n_src"):
            if not np.array_equal(np.asarray(five[k]), fresh[k]):
                diffs.append(f"resample {p5.relative_to(lake)}: column {k} differs from the 1m aggregate")
        for k in ("volume", "quote_volume"):
            if not np.allclose(np.asarray(five[k]), fresh[k], rtol=1e-12, atol=0):
                diffs.append(f"resample {p5.relative_to(lake)}: column {k} differs from the 1m sum")
    return diffs


def check_schema_parity(sample_manifest: Path, real_manifest: Path) -> list[str]:
    s = json.loads(sample_manifest.read_text())["partitions"]
    r = json.loads(real_manifest.read_text())["partitions"]
    diffs = []
    for name, part in s.items():
        if name not in r:
            diffs.append(f"schema parity: sample partition {name} has no counterpart in the real manifest")
        elif set(part["schema_hashes"]) != set(r[name]["schema_hashes"]):
            diffs.append(f"schema parity: {name} sample {sorted(part['schema_hashes'])} vs real "
                         f"{sorted(r[name]['schema_hashes'])}")
    return diffs


def tamper_detected(sample_root: Path) -> bool:
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmp:
        lake = Path(tmp) / "lake"
        shutil.copytree(sample_root / "lake", lake)
        victim = sorted(lake.rglob("*.parquet"))[0]
        t = pq.read_table(victim)
        pq.write_table(t.slice(1), victim, compression="zstd")
        return bool(check_manifest(lake, sample_root / "manifest.json", quiet=True))


def inventory(manifest_path: Path) -> str:
    """Markdown tables of the market-data partitions and the totals, straight from a manifest."""
    m = json.loads(manifest_path.read_text())
    lines = ["| Partition | Symbols | Files | Rows | GB | From | To |", "|---|---:|---:|---:|---:|---|---|"]
    for name, v in m["partitions"].items():
        if v["category"] != "market_data":
            continue
        lines.append(f"| `{name}` | {v['keys']:,} | {v['files']:,} | {v['rows']:,} | {v['bytes'] / 1e9:.2f} | "
                     f"{(v['ts_min'] or '')[:10]} | {(v['ts_max'] or '')[:10]} |")
    lines += ["", "| Scope | Files | Rows | GB |", "|---|---:|---:|---:|"]
    for k, v in m["totals"].items():
        lines.append(f"| {k} | {v['files']:,} | {v['rows']:,} | {v['bytes'] / 1e9:.2f} |")
    return "\n".join(lines)


def run_sample() -> int:
    root = REPO / "sample"
    results = []

    def step(name, diffs):
        ok = not diffs
        results.append(ok)
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
        for line in diffs[:20]:
            print(f"       {line}")

    step("1 sample manifest recomputes exactly", check_manifest(root / "lake", root / "manifest.json", quiet=True))
    step("2 sample schemas equal the real lake's schemas", check_schema_parity(root / "manifest.json",
                                                                              REPO / "manifest/manifest.json"))
    step("3 tamper test: one deleted row is detected", [] if tamper_detected(root) else ["tampered copy passed"])
    step("4 quality scan finds exactly the planted defects", check_quality(root))
    step("5 5m bars equal the traded-minute resample of 1m", check_resample(root))
    ok = all(results)
    print("VERIFY SAMPLE:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("lake_root", nargs="?", type=Path)
    ap.add_argument("--manifest", type=Path, default=REPO / "manifest/manifest.json")
    ap.add_argument("--sample", action="store_true", help="verify the bundled synthetic sample (no lake needed)")
    ap.add_argument("--fast", action="store_true", help="compare the path+size listing hash only")
    ap.add_argument("--partition", help="compare one partition only")
    ap.add_argument("--inventory", action="store_true", help="print the inventory tables of --manifest and exit")
    a = ap.parse_args(argv)
    if a.inventory:
        print(inventory(a.manifest))
        return 0
    if a.sample:
        return run_sample()
    if a.lake_root is None:
        ap.error("give a lake root or --sample")
    diffs = check_fast(a.lake_root, a.manifest) if a.fast else check_manifest(a.lake_root, a.manifest, a.partition)
    for line in diffs:
        print("MISMATCH", line)
    print("VERIFY:", "PASS" if not diffs else f"FAIL ({len(diffs)} differences)")
    return 0 if not diffs else 1


if __name__ == "__main__":
    sys.exit(main())
