"""bookTicker reducer: quant.core.bookticker_reduce (one streamed pass, Rust) against the Python reference.

The reference is spreads.reduce_zip_passes + BucketReducer (engine="python"). Counts must match exactly, floats to
1e-9 relative. The real-slice case reads data/samples/r-bt (the first 2M rows of ETHUSDT 2024-02, re-zipped) and is
skipped when that file is absent. The synthetic cases are built here in tmp_path.
"""

from __future__ import annotations

import random
import zipfile
from pathlib import Path

import numpy as np
import pytest

from quant.core import bookticker_reduce
from quant.data.sources import spreads as S

REPO = Path(__file__).resolve().parents[2]
REAL_SLICE = REPO / "data" / "samples" / "r-bt" / "ETHUSDT-2024-02-head2m.zip"
HEADER = "update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,transaction_time,event_time"
T0_MS = 1_700_000_000_000  # epoch milliseconds, the archive's unit
WINDOW = ((T0_MS - 60_000) * 10**6, (T0_MS + 3 * 3_600_000 + 60_000) * 10**6)  # ns, as _month_window pads a month


def _reference(path: Path, window, passes: int | None = None):
    if passes is None:
        edges = S.balanced_edges(path, window, S.PASS_ROWS)
    else:
        edges = np.linspace(window[0], window[1], passes + 1).astype(np.int64)
    red = S.reduce_zip_passes(path, window, edges=edges)
    return red.finish(), red.stats()


def _column(table, name: str) -> np.ndarray:
    """A table column as numpy, with bucket_start as int64 nanoseconds."""
    col = table[name].to_numpy(zero_copy_only=False)
    return col.astype("datetime64[ns]").astype(np.int64) if name == "bucket_start" else col


def _assert_same(table, res) -> None:
    assert table.num_rows == len(res["bucket_start"]), "bucket count"
    for name in S._OUT_COLUMNS:
        ref = _column(table, name)
        if name == "bucket_start":
            assert np.array_equal(ref, np.asarray(res[name], dtype=np.int64)), name
            continue
        got = np.asarray(res[name])
        if name in ("n_updates", "n_crossed"):
            assert np.array_equal(ref.astype(np.int64), got.astype(np.int64)), name
            continue
        ref = ref.astype(np.float64)
        assert np.array_equal(np.isnan(ref), np.isnan(got)), f"{name}: NaN pattern differs"
        ok = ~np.isnan(ref)
        denom = np.maximum(np.abs(ref[ok]), 1e-300)
        rel = np.abs(ref[ok] - got[ok]) / denom
        assert rel.size == 0 or float(rel.max()) <= 1e-9, f"{name}: max relative error {rel.max():.3g}"


def _stats_match(py_stats: dict, rs_stats: dict) -> None:
    for key in ("rows", "bad_rows", "out_of_window", "out_of_order", "max_span_buckets", "unit"):
        assert py_stats[key] == rs_stats[key], key


def _synthetic_lines(seed: int, n: int = 60_000) -> list[tuple[int, str]]:
    """(update_id, csv line) in update_id order: monotone timestamps, crossed quotes, quiet gaps over buckets, bad rows
    and rows outside the window. The caller shuffles the file order locally."""
    rng = random.Random(seed)
    ts = T0_MS
    uid = 4_000_000_000
    out: list[tuple[int, str]] = []
    for i in range(n):
        uid += rng.randint(1, 3)
        ts += 0 if rng.random() < 0.05 else rng.randint(1, 90)
        if rng.random() < 0.004:
            ts += rng.randint(300_000, 1_500_000)  # a quiet stretch: the state spans several 5-minute buckets
        mid = 2000.0 + 30 * rng.random()
        if rng.random() < 0.02:
            bid = ask = mid  # crossed or locked: counted, left out of the spread statistics
        else:
            half = rng.choice([0.005, 0.01, 0.05, 0.4]) * rng.random()
            bid, ask = mid - half, mid + half
        bq, aq = rng.uniform(0.1, 200), rng.uniform(0.1, 200)
        bid_s, ask_s = f"{bid:.8f}", f"{ask:.8f}"
        bq_s, aq_s = f"{bq:.8f}", f"{aq:.8f}"
        r = rng.random()
        if r < 0.004:
            bid_s = "0.00000000"  # bad: non-positive price
        elif r < 0.008:
            ask_s = "nan"  # bad: not finite
        elif r < 0.012:
            bq_s = "-1.00000000"  # bad: negative size
        elif r < 0.014:
            aq_s = "inf"  # bad: not finite
        line = f"{uid},{bid_s},{bq_s},{ask_s},{aq_s},{ts},{ts + 3}"
        out.append((uid, line))
    # rows outside the window: before it, and after it at the end of the file (both ignored, counted)
    out.insert(0, (uid - 10**6, f"{uid - 10**6},2000.0,1.0,2000.1,1.0,1,1"))
    late = WINDOW[1] // 10**6 + 1000  # ms, past the window end
    out.append((uid + 1, f"{uid + 1},2000.0,1.0,2000.1,1.0,{late},{late}"))
    return out


def _write_zip(path: Path, lines: list[str], name: str = "bookTicker.csv") -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(name, "\n".join([HEADER, *lines]) + "\n")
    return path


def _shuffled_file_order(rows: list[tuple[int, str]], seed: int, block: int = 40) -> list[str]:
    """File order: update_id order, shuffled inside blocks of `block` rows (uid displacement < block)."""
    rng = random.Random(seed)
    lines = [line for _, line in rows]
    for start in range(0, len(lines), block):
        chunk = lines[start : start + block]
        rng.shuffle(chunk)
        lines[start : start + block] = chunk
    return lines


@pytest.fixture(scope="module")
def synthetic_zip(tmp_path_factory) -> Path:
    rows = _synthetic_lines(seed=7)
    d = tmp_path_factory.mktemp("bt")
    return _write_zip(d / "synthetic.zip", _shuffled_file_order(rows, seed=11))


def test_synthetic_out_of_order_and_bad_rows_match_reference(synthetic_zip: Path) -> None:
    table, py_stats = _reference(synthetic_zip, WINDOW)
    res = bookticker_reduce(str(synthetic_zip), *WINDOW, 64)  # a small reorder buffer, so the disorder is exercised
    _stats_match(py_stats, res["stats"])
    assert res["stats"]["bad_rows"] > 0 and res["stats"]["out_of_window"] > 0
    assert res["stats"]["uid_disorder_file_order"] > 0, "the file must be out of update_id order"
    assert 1 < res["stats"]["max_reorder_used"] <= 65  # peak before the emit that keeps the buffer at 64
    assert res["stats"]["max_span_buckets"] >= 2, "a quiet stretch must span buckets"
    _assert_same(table, res)


def test_pass_chunking_does_not_change_the_reference(synthetic_zip: Path) -> None:
    one, _ = _reference(synthetic_zip, WINDOW, passes=1)
    three, _ = _reference(synthetic_zip, WINDOW, passes=3)
    res = bookticker_reduce(str(synthetic_zip), *WINDOW)
    _assert_same(one, res)
    _assert_same(three, res)


def test_engine_switch_gives_the_same_table(synthetic_zip: Path) -> None:
    py_table, py_stats = S.reduce_zip_window(synthetic_zip, WINDOW, engine="python")
    rs_table, rs_stats = S.reduce_zip_window(synthetic_zip, WINDOW, engine="rust")
    _stats_match(py_stats, rs_stats)
    rs_res = {n: _column(rs_table, n) for n in S._OUT_COLUMNS}
    _assert_same(py_table, {**rs_res, "stats": rs_stats})
    with pytest.raises(ValueError, match="unknown engine"):
        S.reduce_zip_window(synthetic_zip, WINDOW, engine="cuda")


def test_rejects_timestamps_that_decrease_in_update_id_order(tmp_path: Path) -> None:
    rows = _synthetic_lines(seed=3, n=200)
    lines = [line for _, line in rows]
    a, b = lines[50].split(","), lines[51].split(",")
    a[5], b[5] = b[5], a[5]  # swap two timestamps: the sequence now goes backwards in time
    lines[50], lines[51] = ",".join(a), ",".join(b)
    path = _write_zip(tmp_path / "back.zip", lines)
    with pytest.raises(ValueError, match="decrease in update_id order"):
        bookticker_reduce(str(path), *WINDOW)


def test_reorder_buffer_overflow_is_an_error_not_a_reorder(tmp_path: Path) -> None:
    rows = _synthetic_lines(seed=5, n=400)
    lines = [line for _, line in rows]
    late = lines.pop(100)  # one quote arrives 300 rows late
    lines.insert(400, late)
    path = _write_zip(tmp_path / "late.zip", lines)
    with pytest.raises(ValueError, match="reorder buffer"):
        bookticker_reduce(str(path), *WINDOW, 8)


@pytest.mark.skipif(not REAL_SLICE.exists(), reason="data/samples/r-bt slice not built")
def test_real_slice_matches_reference() -> None:
    table, py_stats = _reference(REAL_SLICE, (1706745600000 * 10**6 - 86_400 * 10**9, 1709251200 * 10**9 + 86_400 * 10**9))
    res = bookticker_reduce(str(REAL_SLICE), 1706745600000 * 10**6 - 86_400 * 10**9, 1709251200 * 10**9 + 86_400 * 10**9)
    _stats_match(py_stats, res["stats"])
    assert res["stats"]["out_of_order"] == 0 and res["stats"]["bad_rows"] == 0
    _assert_same(table, res)
