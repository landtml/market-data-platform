"""Bybit funding and 1h OI in the staged panel: the last value with usable_from_ts <= t; later events are inert."""

import polars as pl
from quant.data import panel_inputs as pin

BAR = pin.BAR_NS
T0 = 1_704_067_200_000_000_000
N = 24


def _events(stamps_bars, values):
    return pl.DataFrame({"instrument_id": ["X"] * len(values), "usable": [T0 + s * BAR for s in stamps_bars],
                         "value": values})


def _column(events, grid_ts):
    grid = pl.DataFrame({"ts": grid_ts, "instrument_id": ["X"] * len(grid_ts)})
    return pin.asof_value(grid, events, "bybit_oi_1h").to_list()


def test_value_appears_at_its_usable_bar_and_holds_until_the_next():
    grid = [T0 + i * BAR for i in range(N)]
    col = _column(_events([3, 9], [1.5, 2.5]), grid)
    assert col[2] is None and col[3] == 1.5 and col[8] == 1.5 and col[9] == 2.5 and col[-1] == 2.5


def test_perturbing_events_published_after_t_never_changes_bar_t():
    grid = [T0 + i * BAR for i in range(N)]
    base = _column(_events([3, 9, 15], [1.5, 2.5, 3.5]), grid)
    for t in range(N):
        keep = [s for s in (3, 9, 15) if s <= t]
        vals = [v for s, v in zip((3, 9, 15), (1.5, 2.5, 3.5)) if s <= t]
        later = [(s, 100.0 + s) for s in (3, 9, 15) if s > t]
        ss = [s for s, _ in [(s, 0) for s in keep]] + [s for s, _ in later]
        vv = vals + [v for _, v in later]
        col = _column(_events(ss, vv), grid)
        assert col[: t + 1] == base[: t + 1]
