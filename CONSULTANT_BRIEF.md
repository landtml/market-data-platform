# Consultant brief

**Problem.** Trading research on historical data fails quietly when the data lies: coins that were delisted are
missing (so the past looks better than it was), values are used before they were published (look-ahead), or a bad
price print is treated as real. Most of the effort in quantitative research should go into the data, and most
personal projects skip it.

**Approach.** The owner built a private store of crypto futures market data from the exchange's free public archive:
one- or five-minute price bars for about 1,000 Binance contracts from 2020 to October 2026, including the ones that were
later delisted, plus funding payments, reference prices and open interest. Prices were cross-checked against three
other exchanges. Every value carries the time it became knowable, and the research code may only read it after that
time. A rule-based quality scorecard grades the data, and an audit compares samples with the exchange's own files.

**Evidence.** The store holds 2.16 billion rows in 154,690 files (55 GB). The repository does not ship the data
(the exchanges' terms limit redistribution, and the owner keeps it private); it ships a manifest generated from the files' metadata that pins every file
name, size and row count with a fingerprint, plus a tool that recomputes it and fails on any change. A synthetic
sample lets anyone run the tools in under a minute. On a random re-audit sample, 17,280 of 17,280 five-minute bars and
2,574 of 2,574 funding payments equalled the exchange's records exactly.

**Limits.** The data is private, so an outside reader can verify the method and the manifest, and spot-check single
files against the public archive, but not the whole store. The audits were run by AI agents and are quoted from
their reports. The planned research study has not been run, and there is no claim of a profitable strategy: the one
momentum strategy tested lost money out of sample after costs. The project was built in five days with AI coding agents under the
owner's direction; the owner set the rules and decisions, and the agents wrote most of the code.

## Five questions to expect, with straight answers

1. **"Did you write this yourself?"** No, not line by line. AI coding agents wrote most of the code and reports. I
   set the goal, the stack (Rust for row-level work, Python for orchestration), the hard rules (no look-ahead,
   delisted coins kept, a sealed holdout, costs always on) and the pre-registration, and I ruled on open questions.
   What I can stand behind is the verification: equality tests against references, audits against the exchange's
   files, and the manifest. (README, "How this was built"; docs/WORKLOG_EXCERPT.md.)

2. **"How do I know the 2 billion rows exist?"** The manifest pins every file path, size and row count, with a sha256
   over the whole listing. Any line can be spot-checked against the public Binance archive, and I can run the
   verifier on the real lake in about 15 seconds while you watch. The earlier "2.5 billion" figure counted the whole
   data folder, including a rollback copy and scratch files; the lake itself is 2.16 billion. (manifest/README.md.)

3. **"What was the hardest data problem?"** Binance reuses ticker names for new coins. The first build stitched an
   old, dead coin's history onto the new coin under one ID, which hid a delisting and created fake price jumps. The
   audit caught it; the fix is one instrument per listing, and a re-audit confirmed it. (docs/DATA_REPORT_2026-10-22.md, F1.)

4. **"Did you find a profitable strategy?"** No. A momentum strategy that looked good in an older project lost money
   out of sample here (Sharpe -0.12 to -0.91) once delisted coins, funding and costs were included. A hedged
   funding-carry test was positive but exploratory, small after 2021 and fading. The pre-registered study has not
   been run. (README, "What was learned".)

5. **"How do you prevent look-ahead bias?"** Every value has a "usable from" time and readers filter on it; the
   tradable universe for a day uses only data up to the previous day; tests change future data and assert that past
   outputs do not move; a source scan stops features from reading data-cleaning flags that look one bar ahead.
   (method/point_in_time/, method/tests/bias_checks/.)
