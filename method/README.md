# method/

Read-only copies of a few source files from the private repository, kept to show how point-in-time correctness and
data quality are enforced. They are not runnable here: they import the private packages and read the private data
folder. Each file is unchanged from the snapshot of 2026-10-24.

| File | Why it is here |
|---|---|
| `point_in_time/panel_inputs.py` | The "usable from" rule: an auxiliary row is visible at bar t only when its publish time is <= t (`visible_rows`), and REST data only fills slots the archive lacks. |
| `quality/validate.rs` | The Rust validator: duplicates, unsorted rows, gaps, OHLC sanity, non-positive prices, stuck feeds, spikes; one flag bit per rule. `tools/verify.py` applies a subset of these rules to the sample. |
| `quality/quality.py` | The data-quality scorecard (metrics M1-M16) that ends in green / amber / red / proven per metric. |
| `quality/quality_proofs.json` | The two metrics accepted as unfixable from free sources, each with the largest value it covers; a regression reopens the metric (`docs/quality_proofs.md`). |
| `tests/bias_checks/test_panel.py` | Includes the universe look-ahead test: perturb day D's bars and assert day D's universe row does not change; and the test that a reader never sees a mix of two published versions. |
| `tests/bias_checks/test_context.py` | Look-ahead tests for macro and sentiment context: each column is shifted by its publication lag; a shortened lag or a future peek fails. |
| `tests/bias_checks/test_panel_rebuild_bybit.py` | Perturbing a Bybit event published after t never changes bar t. |
| `tests/bias_checks/test_panel_rebuild_r5.py` | The single-bar print rule, and a source scan asserting no feature module reads the data-cleaning bit (it looks one bar ahead). |
| `tests/bias_checks/test_panel_rebuild_f.py` | Stale-run, month-boundary and delist-notice rules from the audit fixes (F-wave). |
| `tests/equality/test_resample_traded.py` | 5m bars take open/high/low/close from traded minutes only, the exchange's own rule (the defect found by the audit). |
| `tests/equality/test_core_bars.py` | Rust resampling equals a pandas reference; Rust trade bars equal the owner's library bar for bar. |
| `tests/equality/test_bookticker_rust.py` | The Rust quote reducer (43x faster) produces the same output as the Python reference. |
