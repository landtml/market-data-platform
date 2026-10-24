"""5m OHLC from 1m klines follows the exchange's own 5m kline: open, high, low and close come from the minutes that
traded. A placeholder minute (gap fill: o=h=l=c=previous close, volume 0, trades 0) is not a trade. Volume, quote
volume, trades and taker sums stay sums over every minute. A 5m slot with no traded minute keeps the placeholder.

Golden case: ATOMUSDT 2020-07-26 02:20 UTC (archive 5m open 3.850, the 1m placeholder at 02:20 is 3.848).
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest
from quant.core import resample_bars

MIN = 60_000_000_000
FIVE = 5 * MIN
START = 1_595_721_600_000_000_000  # 2020-07-26 00:00:00 UTC, day- and 5m-aligned


def _kl(rows: list[tuple]) -> dict:
    """rows: (minute index from START, o, h, l, c, v, trades). A placeholder has v=0 and trades=0 with o=h=l=c."""
    rows = sorted(rows)
    idx = np.array([r[0] for r in rows], dtype=np.int64)
    o, h, lo, c, v = (np.array([r[k] for r in rows], dtype=np.float64) for k in (1, 2, 3, 4, 5))
    n = np.array([r[6] for r in rows], dtype=np.int64)
    return {
        "ts": START + idx * MIN, "o": o, "h": h, "l": lo, "c": c, "v": v, "qv": v * c, "n": n,
        "tbv": v * 0.5, "tbqv": v * 0.5 * c,
    }


def _ph(i: int, price: float) -> tuple:
    """Placeholder minute: o=h=l=c=price, no volume, no trades."""
    return (i, price, price, price, price, 0.0, 0)


def _run(k: dict, traded=None) -> dict:
    kw = {} if traded is None else {"traded": traded}
    return resample_bars(k["ts"], k["o"], k["h"], k["l"], k["c"], k["v"], k["qv"], k["n"], k["tbv"], k["tbqv"],
                         FIVE, **kw)


def _bucket(r: dict, minute: int) -> int:
    hits = np.flatnonzero(r["ts"] == START + minute * MIN)
    assert len(hits) == 1, f"no 5m bucket at minute {minute}"
    return int(hits[0])


def test_golden_atomusdt_placeholder_first_minute_open_is_first_trade():
    # 02:20 placeholder at 3.848, first trade at 02:21 opens at 3.850 (archive 5m open 3.850).
    k = _kl([_ph(140, 3.848), (141, 3.850, 3.851, 3.849, 3.850, 4.0, 3), (142, 3.850, 3.853, 3.850, 3.852, 2.0, 2)])
    r = _run(k)
    b = _bucket(r, 140)
    assert r["o"][b] == 3.850
    assert r["c"][b] == 3.852
    assert r["h"][b] == 3.853
    assert r["l"][b] == 3.849


def test_a_placeholder_first_minute_open_is_first_traded_minute_open():
    k = _kl([_ph(0, 100.0), (1, 101.0, 102.0, 100.5, 101.5, 10.0, 5), (2, 101.5, 101.8, 101.2, 101.4, 4.0, 2)])
    r = _run(k)
    b = _bucket(r, 0)
    assert r["o"][b] == 101.0
    assert r["h"][b] == 102.0
    assert r["l"][b] == 100.5
    assert r["c"][b] == 101.4
    assert r["n_src"][b] == 3
    assert not r["filled"][b]


@pytest.mark.parametrize("prev_close", [3.90, 3.70])
def test_b_placeholder_outside_traded_range_does_not_move_high_or_low(prev_close):
    # Minute 0 (previous 5m slot) closes at prev_close; the 1m placeholder at minute 5 carries it into the next slot.
    k = _kl([(0, 3.80, 3.95, 3.75, prev_close, 1.0, 1), _ph(5, prev_close),
             (6, 3.85, 3.86, 3.84, 3.855, 2.0, 2), (7, 3.855, 3.857, 3.853, 3.856, 1.0, 1)])
    r = _run(k)
    b = _bucket(r, 5)
    assert r["h"][b] == 3.86
    assert r["l"][b] == 3.84
    assert r["o"][b] == 3.85
    assert r["c"][b] == 3.856


def test_c_slot_with_no_traded_minute_keeps_placeholder():
    # Slot 0..4 is all placeholders at 3.90; slot 5..9 is missing entirely; slot 10..14 trades.
    k = _kl([_ph(i, 3.90) for i in range(5)] + [(10, 3.91, 3.92, 3.90, 3.915, 1.0, 1)])
    r = _run(k)
    b0 = _bucket(r, 0)
    assert (r["o"][b0], r["h"][b0], r["l"][b0], r["c"][b0]) == (3.90, 3.90, 3.90, 3.90)
    assert r["v"][b0] == 0.0 and r["n"][b0] == 0 and r["tbv"][b0] == 0.0
    assert r["n_src"][b0] == 5 and bool(r["complete"][b0]) and not r["filled"][b0]
    b1 = _bucket(r, 5)
    assert (r["o"][b1], r["h"][b1], r["l"][b1], r["c"][b1]) == (3.90, 3.90, 3.90, 3.90)
    assert r["n_src"][b1] == 0 and bool(r["filled"][b1]) and not bool(r["complete"][b1])
    b2 = _bucket(r, 10)
    assert r["o"][b2] == 3.91 and r["c"][b2] == 3.915


def test_d_volume_quote_trades_taker_sums_cover_every_minute():
    k = _kl([_ph(0, 50.0), (1, 50.5, 51.0, 50.2, 50.8, 3.0, 4), _ph(2, 50.8), (3, 50.9, 51.2, 50.7, 51.0, 2.5, 6),
             (4, 51.0, 51.1, 50.9, 50.95, 0.5, 1)])
    r = _run(k)
    b = _bucket(r, 0)
    assert r["v"][b] == pytest.approx(6.0, rel=1e-12, abs=0.0)
    assert r["qv"][b] == pytest.approx(float(np.sum(k["qv"])), rel=1e-12, abs=0.0)
    assert r["n"][b] == 11
    assert r["tbv"][b] == pytest.approx(3.0, rel=1e-12, abs=0.0)
    assert r["tbqv"][b] == pytest.approx(float(np.sum(k["tbqv"])), rel=1e-12, abs=0.0)
    assert r["n_src"][b] == 5 and bool(r["complete"][b])


def test_explicit_traded_mask_equal_to_trade_count_matches_default():
    k = _kl([_ph(0, 100.0), (1, 101.0, 102.0, 100.5, 101.5, 10.0, 5), _ph(2, 101.5), (4, 101.5, 101.9, 101.1, 101.6,
                                                                                   2.0, 3)])
    default = _run(k)
    explicit = _run(k, traded=k["n"] > 0)
    for col in ("ts", "o", "h", "l", "c", "v", "qv", "n", "tbv", "tbqv", "n_src", "complete", "filled"):
        np.testing.assert_array_equal(default[col], explicit[col], err_msg=col)


def test_explicit_traded_mask_uses_volume_when_trades_absent():
    # trades are unknown: the traded test is volume > 0. Minute 0 is a placeholder with trades 0 but volume 0.
    k = _kl([_ph(0, 100.0), (1, 101.0, 102.0, 100.5, 101.5, 10.0, 0), (2, 101.5, 101.8, 101.2, 101.4, 4.0, 0)])
    r = _run(k, traded=k["v"] > 0)
    b = _bucket(r, 0)
    assert r["o"][b] == 101.0
    assert r["l"][b] == 100.5


def test_traded_mask_length_must_match_rows():
    k = _kl([(0, 1.0, 1.0, 1.0, 1.0, 1.0, 1)])
    with pytest.raises(ValueError):
        _run(k, traded=np.array([True, False]))


def test_pipeline_5m_with_null_trade_count_uses_volume_as_traded_test():
    # _resample5 receives a 1m table whose count column is null: a minute traded when volume > 0.
    from quant.data.pipeline.binance_um import KLINE_NAMES, _resample5

    rows = [_ph(0, 100.0), (1, 101.0, 102.0, 100.5, 101.5, 10.0, 0), (2, 101.5, 101.8, 101.2, 101.4, 4.0, 0)]
    k = _kl(rows)
    n = len(rows)
    t = pa.table({
        "ts": k["ts"], "open": k["o"], "high": k["h"], "low": k["l"], "close": k["c"], "volume": k["v"],
        "close_ts": k["ts"] + MIN - 1, "quote_volume": k["qv"], "count": pa.array([None] * n, pa.int64()),
        "taker_buy_volume": k["tbv"], "taker_buy_quote_volume": k["tbqv"],
    }).select(KLINE_NAMES)
    r = _resample5(t)
    b = _bucket({"ts": r["ts"].to_numpy()}, 0)
    assert r["open"].to_numpy()[b] == 101.0
    assert r["low"].to_numpy()[b] == 100.5
    assert r["volume"].to_numpy()[b] == pytest.approx(14.0, rel=1e-12, abs=0.0)
    assert r["n_src"].to_numpy()[b] == 3
