//! Validation of bar and value-series data: `validate_bars`, `validate_series`, `flag_bits`.
//!
//! Inputs are numpy arrays (ts int64 ns UTC; prices and volume float64). No file IO. The scan runs with the GIL
//! released. Every row carries a `flags` bit for each rule it breaks (see `flag_bits`). `issues` lists one entry per
//! gap and per stuck run, and one entry per run of consecutive rows for the row rules. The counts are exact; the
//! issue list is capped at `MAX_ISSUES` per rule.
//!
//! Rule definitions (also in the Python docstrings):
//! - unsorted: ts[i] < ts[i-1]. dupe: ts[i] == ts[i-1]. "Not strictly increasing" is the union of the two.
//! - gap: ts[i] - ts[i-1] > interval; missing = ceil(step / interval) - 1 grid slots strictly between the two rows.
//!   Equal to step/interval - 1 for aligned data. The gap issue's ts is the first missing slot. The flag goes on row i.
//! - misaligned: ts % interval != 0.
//! - ohlc_bad: h < o, h < c, l > o, l > c or h < l. nonpositive: any of o, h, l, c <= 0. nan: any NaN in a row.
//! - zero_vol: v == 0.
//! - stuck: close and volume equal to the previous row's for more than `stuck_limit` consecutive rows (V2).
//! - spike: |r_t| > spike_k * sigma_t, where r_t is the log return and sigma_t = max(median(|r|) / 0.6745, 1e-4)
//!   over the previous `spike_window` returns (a zero-centred MAD converted to a normal sigma). The median is exact:
//!   a sliding two-heap median with lazy deletion, O(n log w) amortised. The move must retrace at least 80% within the
//!   next 3 rows. The rule looks ahead, so it is a post-hoc check and not a causal filter.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};

use numpy::{PyArray1, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

pub const UNSORTED: u32 = 1 << 0;
pub const DUPE: u32 = 1 << 1;
pub const GAP: u32 = 1 << 2;
pub const MISALIGNED: u32 = 1 << 3;
pub const OHLC_BAD: u32 = 1 << 4;
pub const NONPOSITIVE: u32 = 1 << 5;
pub const NAN: u32 = 1 << 6;
pub const ZERO_VOL: u32 = 1 << 7;
pub const STUCK: u32 = 1 << 8;
pub const SPIKE: u32 = 1 << 9;

/// Rule id and bit, in the order `flag_bits` reports them.
pub const RULES: [(&str, u32); 10] = [
    ("unsorted", UNSORTED),
    ("dupe", DUPE),
    ("gap", GAP),
    ("misaligned", MISALIGNED),
    ("ohlc_bad", OHLC_BAD),
    ("nonpositive", NONPOSITIVE),
    ("nan", NAN),
    ("zero_vol", ZERO_VOL),
    ("stuck", STUCK),
    ("spike", SPIKE),
];

pub const SPIKE_K: f64 = 12.0;
pub const SPIKE_WINDOW: usize = 1440;
const SPIKE_MIN_HISTORY: usize = 30;
const SPIKE_SIGMA_FLOOR: f64 = 1e-4;
/// Phi^-1(0.75): a normal sigma is MAD / 0.6745.
const MAD_TO_SIGMA: f64 = 0.6745;
const REVERSAL_FRACTION: f64 = 0.8;
const REVERSAL_BARS: usize = 3;
const MAX_ISSUES: usize = 100_000;
const MAX_DAYS: i64 = 1_000_000;
const DAY_NS: i64 = 86_400_000_000_000;

#[derive(Debug, Clone, PartialEq)]
pub struct Issue {
    pub rule: &'static str,
    pub ts: i64,
    pub n: u64,
    pub detail: String,
}

#[derive(Default)]
struct IssueLog {
    items: Vec<Issue>,
    counts: HashMap<&'static str, usize>,
}

impl IssueLog {
    fn push(&mut self, rule: &'static str, ts: i64, n: u64, detail: impl FnOnce() -> String) {
        let count = self.counts.entry(rule).or_insert(0);
        if *count < MAX_ISSUES {
            *count += 1;
            self.items.push(Issue { rule, ts, n, detail: detail() });
        }
    }
}

#[derive(Debug)]
pub struct BarScan {
    pub rows: usize,
    pub missing: u64,
    pub dupes: u64,
    pub unsorted: u64,
    pub ohlc_bad: u64,
    pub zero_vol: u64,
    pub nonpositive: u64,
    pub stuck_runs: u64,
    pub max_gap_ns: i64,
    pub max_gap_at: Option<i64>,
    pub issues: Vec<Issue>,
    pub flags: Vec<u32>,
}

#[derive(Debug)]
pub struct DayCount {
    pub day_ns: i64,
    pub observed: u64,
    pub expected: u64,
}

#[derive(Debug)]
pub struct SeriesScan {
    pub rows: usize,
    pub dupes: u64,
    pub unsorted: u64,
    pub missing: u64,
    pub nan: u64,
    pub stuck_runs: u64,
    pub max_gap_ns: i64,
    pub max_gap_at: Option<i64>,
    pub issues: Vec<Issue>,
    pub flags: Vec<u32>,
    pub days: Vec<DayCount>,
}

struct Gaps {
    missing: u64,
    max_gap_ns: i64,
    max_gap_at: Option<i64>,
}

fn count_bit(flags: &[u32], bit: u32) -> u64 {
    flags.iter().filter(|&&f| f & bit != 0).count() as u64
}

/// Dupe, unsorted and gap flags, plus gap issues. `step` None disables the gap rule.
fn ts_rules(ts: &[i64], step: Option<i64>, flags: &mut [u32], issues: &mut IssueLog) -> Gaps {
    let mut g = Gaps { missing: 0, max_gap_ns: 0, max_gap_at: None };
    for i in 1..ts.len() {
        let (prev, cur) = (ts[i - 1], ts[i]);
        if cur == prev {
            flags[i] |= DUPE;
        } else if cur < prev {
            flags[i] |= UNSORTED;
        } else if let Some(iv) = step {
            let gap = cur as i128 - prev as i128;
            let iv128 = iv as i128;
            if gap > iv128 {
                let m = ((gap + iv128 - 1) / iv128 - 1) as u64;
                let first_missing = prev.saturating_add(iv);
                flags[i] |= GAP;
                g.missing += m;
                let gap64 = i64::try_from(gap).unwrap_or(i64::MAX);
                if gap64 > g.max_gap_ns {
                    g.max_gap_ns = gap64;
                    g.max_gap_at = Some(first_missing);
                }
                issues.push("gap", first_missing, m, || format!("{m} missing bar(s) between {prev} and {cur}"));
            }
        }
    }
    g
}

/// Stuck runs: rows whose values in every column equal the previous row's, for more than `limit` consecutive rows.
fn stuck_rules(ts: &[i64], cols: &[&[f64]], limit: usize, flags: &mut [u32], issues: &mut IssueLog) -> u64 {
    let n = ts.len();
    let same = |j: usize| cols.iter().all(|c| c[j] == c[j - 1]);
    let mut runs = 0u64;
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && same(j) {
            j += 1;
        }
        let len = j - i;
        if len > limit {
            runs += 1;
            for f in &mut flags[i..j] {
                *f |= STUCK;
            }
            issues.push("stuck", ts[i], len as u64, || {
                format!("rows {i}..{}: values unchanged for {len} bars", j - 1)
            });
        }
        i = j;
    }
    runs
}

/// One issue per run of consecutive rows that carry `bit`.
fn group_rows(ts: &[i64], flags: &[u32], bit: u32, rule: &'static str, issues: &mut IssueLog) {
    let n = flags.len();
    let mut i = 0;
    while i < n {
        if flags[i] & bit == 0 {
            i += 1;
            continue;
        }
        let start = i;
        while i < n && flags[i] & bit != 0 {
            i += 1;
        }
        issues.push(rule, ts[start], (i - start) as u64, || format!("rows {start}..{}", i - 1));
    }
}

fn log_return(prev: f64, cur: f64) -> f64 {
    if prev > 0.0 && cur > 0.0 {
        (cur / prev).ln()
    } else {
        f64::NAN
    }
}

fn abs_key(r: f64) -> u64 {
    if r.is_finite() {
        r.abs().to_bits()
    } else {
        0.0f64.to_bits()
    }
}

/// Exact median of a sliding window of non-negative f64 keys (stored as bit patterns, which sort like the values).
/// Two heaps (low: max, high: min) with lazy deletion. The deletion lists are rebuilt from the window when they
/// outgrow it, which keeps every operation O(log w) amortised.
struct SlidingMedian {
    cap: usize,
    ring: VecDeque<u64>,
    lo: BinaryHeap<u64>,
    hi: BinaryHeap<Reverse<u64>>,
    lo_del: BinaryHeap<u64>,
    hi_del: BinaryHeap<Reverse<u64>>,
    lo_n: usize,
    hi_n: usize,
}

impl SlidingMedian {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            ring: VecDeque::with_capacity(cap + 1),
            lo: BinaryHeap::new(),
            hi: BinaryHeap::new(),
            lo_del: BinaryHeap::new(),
            hi_del: BinaryHeap::new(),
            lo_n: 0,
            hi_n: 0,
        }
    }

    fn len(&self) -> usize {
        self.lo_n + self.hi_n
    }

    fn prune_lo(&mut self) {
        while let (Some(t), Some(d)) = (self.lo.peek(), self.lo_del.peek()) {
            if t != d {
                break;
            }
            self.lo.pop();
            self.lo_del.pop();
        }
    }

    fn prune_hi(&mut self) {
        while let (Some(t), Some(d)) = (self.hi.peek(), self.hi_del.peek()) {
            if t != d {
                break;
            }
            self.hi.pop();
            self.hi_del.pop();
        }
    }

    fn push(&mut self, x: u64) {
        self.ring.push_back(x);
        self.prune_lo();
        let to_lo = self.lo_n > 0 && self.lo.peek().is_some_and(|&t| x <= t);
        if to_lo {
            self.lo.push(x);
            self.lo_n += 1;
        } else {
            self.hi.push(Reverse(x));
            self.hi_n += 1;
        }
        self.balance();
    }

    fn pop_oldest(&mut self) {
        let Some(x) = self.ring.pop_front() else { return };
        self.prune_lo();
        if self.lo_n > 0 && self.lo.peek().is_some_and(|&t| x <= t) {
            self.lo_del.push(x);
            self.lo_n -= 1;
        } else {
            self.hi_del.push(Reverse(x));
            self.hi_n -= 1;
        }
        self.balance();
    }

    fn balance(&mut self) {
        while self.lo_n > self.hi_n + 1 {
            self.prune_lo();
            let Some(x) = self.lo.pop() else { break };
            self.lo_n -= 1;
            self.hi.push(Reverse(x));
            self.hi_n += 1;
        }
        while self.hi_n > self.lo_n {
            self.prune_hi();
            let Some(Reverse(x)) = self.hi.pop() else { break };
            self.hi_n -= 1;
            self.lo.push(x);
            self.lo_n += 1;
        }
        if self.lo_del.len() + self.hi_del.len() > self.cap + 64 {
            self.rebuild();
        }
    }

    fn rebuild(&mut self) {
        let mut keys: Vec<u64> = self.ring.iter().copied().collect();
        keys.sort_unstable();
        let lo_len = keys.len().div_ceil(2);
        self.hi = keys[lo_len..].iter().map(|&k| Reverse(k)).collect();
        keys.truncate(lo_len);
        self.lo = keys.into_iter().collect();
        self.lo_del.clear();
        self.hi_del.clear();
        self.lo_n = lo_len;
        self.hi_n = self.ring.len() - lo_len;
    }

    /// Median of the live window; the window must be non-empty.
    fn median(&mut self) -> f64 {
        self.prune_lo();
        if self.lo_n > self.hi_n {
            return f64::from_bits(*self.lo.peek().expect("non-empty window"));
        }
        self.prune_hi();
        let a = f64::from_bits(*self.lo.peek().expect("non-empty window"));
        let b = f64::from_bits(self.hi.peek().expect("non-empty window").0);
        0.5 * (a + b)
    }
}

/// Rows t whose log return is a spike: beyond spike_k sigma of the previous `window` returns, and retraced by at least
/// 80% within the next 3 rows.
pub fn spike_indices(c: &[f64], k: f64, window: usize) -> Vec<usize> {
    let n = c.len();
    if n < 2 || window < 2 {
        return Vec::new();
    }
    let ret: Vec<f64> = (0..n).map(|t| if t == 0 { f64::NAN } else { log_return(c[t - 1], c[t]) }).collect();
    let mut candidates = Vec::new();
    let mut win = SlidingMedian::new(window);
    for (t, &r) in ret.iter().enumerate().skip(1) {
        if r.is_finite() && win.len() >= SPIKE_MIN_HISTORY {
            let sigma = (win.median() / MAD_TO_SIGMA).max(SPIKE_SIGMA_FLOOR);
            if r.abs() > k * sigma {
                candidates.push(t);
            }
        }
        win.push(abs_key(r));
        if win.len() > window {
            win.pop_oldest();
        }
    }
    candidates.retain(|&t| {
        let m = ret[t];
        let ct = c[t];
        let last = (t + REVERSAL_BARS).min(n - 1);
        (t + 1..=last).any(|j| {
            let cj = c[j];
            cj > 0.0 && cj.is_finite() && (ct / cj).ln() / m >= REVERSAL_FRACTION
        })
    });
    candidates
}

#[allow(clippy::too_many_arguments)]
pub fn scan_bars(
    ts: &[i64],
    o: &[f64],
    h: &[f64],
    l: &[f64],
    c: &[f64],
    v: &[f64],
    interval: i64,
    stuck_limit: usize,
    spike_k: f64,
    spike_window: usize,
) -> BarScan {
    let n = ts.len();
    let mut flags = vec![0u32; n];
    let mut issues = IssueLog::default();
    let gaps = ts_rules(ts, Some(interval), &mut flags, &mut issues);
    for i in 0..n {
        if ts[i].rem_euclid(interval) != 0 {
            flags[i] |= MISALIGNED;
        }
        let (oo, hh, ll, cc, vv) = (o[i], h[i], l[i], c[i], v[i]);
        if oo.is_nan() || hh.is_nan() || ll.is_nan() || cc.is_nan() || vv.is_nan() {
            flags[i] |= NAN;
        }
        if oo <= 0.0 || hh <= 0.0 || ll <= 0.0 || cc <= 0.0 {
            flags[i] |= NONPOSITIVE;
        }
        if hh < oo || hh < cc || ll > oo || ll > cc || hh < ll {
            flags[i] |= OHLC_BAD;
        }
        if vv == 0.0 {
            flags[i] |= ZERO_VOL;
        }
    }
    let stuck_runs = stuck_rules(ts, &[c, v], stuck_limit, &mut flags, &mut issues);
    for t in spike_indices(c, spike_k, spike_window) {
        flags[t] |= SPIKE;
    }
    for (bit, rule) in [
        (DUPE, "dupe"),
        (UNSORTED, "unsorted"),
        (MISALIGNED, "misaligned"),
        (OHLC_BAD, "ohlc_bad"),
        (NONPOSITIVE, "nonpositive"),
        (NAN, "nan"),
        (ZERO_VOL, "zero_vol"),
        (SPIKE, "spike"),
    ] {
        group_rows(ts, &flags, bit, rule, &mut issues);
    }
    issues.items.sort_by_key(|i| i.ts);
    BarScan {
        rows: n,
        missing: gaps.missing,
        dupes: count_bit(&flags, DUPE),
        unsorted: count_bit(&flags, UNSORTED),
        ohlc_bad: count_bit(&flags, OHLC_BAD),
        zero_vol: count_bit(&flags, ZERO_VOL),
        nonpositive: count_bit(&flags, NONPOSITIVE),
        stuck_runs,
        max_gap_ns: gaps.max_gap_ns,
        max_gap_at: gaps.max_gap_at,
        issues: issues.items,
        flags,
    }
}

/// Civil (proleptic Gregorian) date of a day number since 1970-01-01 (Howard Hinnant's algorithm).
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// Distinct timestamps per UTC day across the observed span, against `DAY_NS / step` slots per day.
pub fn day_completeness(ts: &[i64], step: i64) -> Result<Vec<DayCount>, String> {
    let sorted_unique: Cow<[i64]> = if ts.windows(2).all(|w| w[0] < w[1]) {
        Cow::Borrowed(ts)
    } else {
        let mut v = ts.to_vec();
        v.sort_unstable();
        v.dedup();
        Cow::Owned(v)
    };
    let (Some(&first), Some(&last)) = (sorted_unique.first(), sorted_unique.last()) else {
        return Ok(Vec::new());
    };
    let first_day = first.div_euclid(DAY_NS);
    let span = last.div_euclid(DAY_NS) - first_day + 1;
    if span > MAX_DAYS {
        return Err(format!("timestamps span {span} days; refusing more than {MAX_DAYS}"));
    }
    let mut observed = vec![0u64; span as usize];
    for &t in sorted_unique.iter() {
        observed[(t.div_euclid(DAY_NS) - first_day) as usize] += 1;
    }
    let expected = (DAY_NS / step) as u64;
    Ok(observed
        .into_iter()
        .enumerate()
        .map(|(i, observed)| DayCount {
            day_ns: (first_day + i as i64) * DAY_NS,
            observed,
            expected,
        })
        .collect())
}

pub fn scan_series(ts: &[i64], x: &[f64], expected: Option<i64>, stuck_limit: usize) -> Result<SeriesScan, String> {
    let mut flags = vec![0u32; ts.len()];
    let mut issues = IssueLog::default();
    let gaps = ts_rules(ts, expected, &mut flags, &mut issues);
    for (f, xv) in flags.iter_mut().zip(x) {
        if xv.is_nan() {
            *f |= NAN;
        }
    }
    let stuck_runs = stuck_rules(ts, &[x], stuck_limit, &mut flags, &mut issues);
    for (bit, rule) in [(DUPE, "dupe"), (UNSORTED, "unsorted"), (NAN, "nan")] {
        group_rows(ts, &flags, bit, rule, &mut issues);
    }
    issues.items.sort_by_key(|i| i.ts);
    let days = match expected {
        Some(step) => day_completeness(ts, step)?,
        None => Vec::new(),
    };
    Ok(SeriesScan {
        rows: ts.len(),
        dupes: count_bit(&flags, DUPE),
        unsorted: count_bit(&flags, UNSORTED),
        missing: gaps.missing,
        nan: count_bit(&flags, NAN),
        stuck_runs,
        max_gap_ns: gaps.max_gap_ns,
        max_gap_at: gaps.max_gap_at,
        issues: issues.items,
        flags,
        days,
    })
}

fn check_len(name: &str, got: usize, want: usize) -> PyResult<()> {
    if got != want {
        return Err(PyValueError::new_err(format!("{name} has {got} rows, ts has {want}")));
    }
    Ok(())
}

fn issues_to_py<'py>(py: Python<'py>, issues: &[Issue]) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for i in issues {
        let d = PyDict::new(py);
        d.set_item("rule", i.rule)?;
        d.set_item("ts", i.ts)?;
        d.set_item("n", i.n)?;
        d.set_item("detail", &i.detail)?;
        list.append(d)?;
    }
    Ok(list)
}

/// Validates OHLCV bars. Returns a dict: rows, missing, dupes, unsorted, ohlc_bad, zero_vol, nonpositive, stuck_runs,
/// max_gap_ns, max_gap_at (first missing slot of the largest gap, or None), issues (list of dicts rule/ts/n/detail)
/// and flags (uint32 per row, bits as in `flag_bits()`). `spike_k` and `spike_window` are the spike-rule parameters.
#[pyfunction]
#[pyo3(signature = (ts, o, h, l, c, v, interval_ns, stuck_limit=30, spike_k=SPIKE_K, spike_window=SPIKE_WINDOW))]
#[allow(clippy::too_many_arguments)]
pub fn validate_bars<'py>(
    py: Python<'py>,
    ts: PyReadonlyArray1<'py, i64>,
    o: PyReadonlyArray1<'py, f64>,
    h: PyReadonlyArray1<'py, f64>,
    l: PyReadonlyArray1<'py, f64>,
    c: PyReadonlyArray1<'py, f64>,
    v: PyReadonlyArray1<'py, f64>,
    interval_ns: i64,
    stuck_limit: usize,
    spike_k: f64,
    spike_window: usize,
) -> PyResult<Bound<'py, PyDict>> {
    if interval_ns <= 0 {
        return Err(PyValueError::new_err("interval_ns must be positive"));
    }
    if stuck_limit == 0 {
        return Err(PyValueError::new_err("stuck_limit must be at least 1"));
    }
    if spike_window < 2 || !(spike_k > 0.0) {
        return Err(PyValueError::new_err("spike_window must be at least 2 and spike_k positive"));
    }
    let ts_s = ts.as_slice().map_err(|e| PyValueError::new_err(format!("ts: {e}")))?;
    let (o_s, h_s, l_s, c_s, v_s) = (
        o.as_slice().map_err(|e| PyValueError::new_err(format!("o: {e}")))?,
        h.as_slice().map_err(|e| PyValueError::new_err(format!("h: {e}")))?,
        l.as_slice().map_err(|e| PyValueError::new_err(format!("l: {e}")))?,
        c.as_slice().map_err(|e| PyValueError::new_err(format!("c: {e}")))?,
        v.as_slice().map_err(|e| PyValueError::new_err(format!("v: {e}")))?,
    );
    let n = ts_s.len();
    check_len("o", o_s.len(), n)?;
    check_len("h", h_s.len(), n)?;
    check_len("l", l_s.len(), n)?;
    check_len("c", c_s.len(), n)?;
    check_len("v", v_s.len(), n)?;
    let scan = py.detach(|| {
        scan_bars(ts_s, o_s, h_s, l_s, c_s, v_s, interval_ns, stuck_limit, spike_k, spike_window)
    });
    let d = PyDict::new(py);
    d.set_item("rows", scan.rows)?;
    d.set_item("missing", scan.missing)?;
    d.set_item("dupes", scan.dupes)?;
    d.set_item("unsorted", scan.unsorted)?;
    d.set_item("ohlc_bad", scan.ohlc_bad)?;
    d.set_item("zero_vol", scan.zero_vol)?;
    d.set_item("nonpositive", scan.nonpositive)?;
    d.set_item("stuck_runs", scan.stuck_runs)?;
    d.set_item("max_gap_ns", scan.max_gap_ns)?;
    d.set_item("max_gap_at", scan.max_gap_at)?;
    d.set_item("issues", issues_to_py(py, &scan.issues)?)?;
    d.set_item("flags", PyArray1::from_vec(py, scan.flags))?;
    Ok(d)
}

/// Validates one value series (funding, mark, OI). Returns a dict: name, rows, dupes, unsorted, missing, nan,
/// stuck_runs, max_gap_ns, max_gap_at, issues, flags, and day_completeness when expected_step_ns is given: one dict
/// per UTC day across the observed span with day, day_ns, observed (distinct timestamps), expected (DAY_NS //
/// step) and completeness (observed / expected).
#[pyfunction]
#[pyo3(signature = (ts, x, expected_step_ns=None, stuck_limit=30, name="series"))]
pub fn validate_series<'py>(
    py: Python<'py>,
    ts: PyReadonlyArray1<'py, i64>,
    x: PyReadonlyArray1<'py, f64>,
    expected_step_ns: Option<i64>,
    stuck_limit: usize,
    name: &str,
) -> PyResult<Bound<'py, PyDict>> {
    if let Some(step) = expected_step_ns {
        if step <= 0 || step > DAY_NS {
            return Err(PyValueError::new_err("expected_step_ns must be in (0, 1 day] or None"));
        }
    }
    if stuck_limit == 0 {
        return Err(PyValueError::new_err("stuck_limit must be at least 1"));
    }
    let ts_s = ts.as_slice().map_err(|e| PyValueError::new_err(format!("ts: {e}")))?;
    let x_s = x.as_slice().map_err(|e| PyValueError::new_err(format!("x: {e}")))?;
    check_len("x", x_s.len(), ts_s.len())?;
    let scan = py
        .detach(|| scan_series(ts_s, x_s, expected_step_ns, stuck_limit))
        .map_err(PyValueError::new_err)?;
    let d = PyDict::new(py);
    d.set_item("name", name)?;
    d.set_item("rows", scan.rows)?;
    d.set_item("dupes", scan.dupes)?;
    d.set_item("unsorted", scan.unsorted)?;
    d.set_item("missing", scan.missing)?;
    d.set_item("nan", scan.nan)?;
    d.set_item("stuck_runs", scan.stuck_runs)?;
    d.set_item("max_gap_ns", scan.max_gap_ns)?;
    d.set_item("max_gap_at", scan.max_gap_at)?;
    d.set_item("issues", issues_to_py(py, &scan.issues)?)?;
    d.set_item("flags", PyArray1::from_vec(py, scan.flags))?;
    let days = PyList::empty(py);
    for day in &scan.days {
        let (y, m, dd) = civil_from_days(day.day_ns.div_euclid(DAY_NS));
        let e = PyDict::new(py);
        e.set_item("day", format!("{y:04}-{m:02}-{dd:02}"))?;
        e.set_item("day_ns", day.day_ns)?;
        e.set_item("observed", day.observed)?;
        e.set_item("expected", day.expected)?;
        e.set_item("completeness", day.observed as f64 / day.expected as f64)?;
        days.append(e)?;
    }
    d.set_item("day_completeness", days)?;
    Ok(d)
}

/// The rule ids and their flag bits, e.g. {"gap": 4, "spike": 512, ...}. A row's `flags` value is the OR of the bits
/// of the rules it breaks.
#[pyfunction]
pub fn flag_bits<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    for (rule, bit) in RULES {
        d.set_item(rule, bit)?;
    }
    Ok(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn uniform(&mut self) -> f64 {
            ((self.next_u64() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
        }

        fn normal(&mut self) -> f64 {
            let (u1, u2) = (self.uniform(), self.uniform());
            (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
        }
    }

    const MIN_NS: i64 = 60_000_000_000;

    #[derive(Clone)]
    struct Bars {
        ts: Vec<i64>,
        o: Vec<f64>,
        h: Vec<f64>,
        l: Vec<f64>,
        c: Vec<f64>,
        v: Vec<f64>,
    }

    /// Gaussian random walk with 1e-3 log sigma; OHLC consistent, volume positive, every bar aligned.
    fn clean_walk(n: usize, seed: u64) -> Bars {
        let mut rng = Rng(seed);
        let mut b = Bars { ts: vec![], o: vec![], h: vec![], l: vec![], c: vec![], v: vec![] };
        let mut px = 100.0f64;
        for i in 0..n {
            let open = px;
            px *= (1e-3 * rng.normal()).exp();
            let hi = open.max(px) * (1.0 + 2e-4 * rng.uniform());
            let lo = open.min(px) * (1.0 - 2e-4 * rng.uniform());
            b.ts.push(1_700_000_000_000_000_000 / MIN_NS * MIN_NS + i as i64 * MIN_NS);
            b.o.push(open);
            b.h.push(hi);
            b.l.push(lo);
            b.c.push(px);
            b.v.push(10.0 + 90.0 * rng.uniform());
        }
        b
    }

    fn scan(b: &Bars, stuck_limit: usize) -> BarScan {
        scan_bars(&b.ts, &b.o, &b.h, &b.l, &b.c, &b.v, MIN_NS, stuck_limit, SPIKE_K, SPIKE_WINDOW)
    }

    fn rules_at(s: &BarScan, row: usize) -> Vec<&'static str> {
        RULES.iter().filter(|(_, bit)| s.flags[row] & bit != 0).map(|(r, _)| *r).collect()
    }

    fn total_flags(s: &BarScan) -> usize {
        s.flags.iter().filter(|&&f| f != 0).count()
    }

    #[test]
    fn clean_walk_fires_nothing() {
        let b = clean_walk(100_000, 7);
        let s = scan(&b, 30);
        assert_eq!(total_flags(&s), 0, "issues: {:?}", &s.issues[..s.issues.len().min(5)]);
        assert!(s.issues.is_empty());
        assert_eq!((s.missing, s.dupes, s.unsorted, s.ohlc_bad, s.zero_vol, s.nonpositive, s.stuck_runs), (0, 0, 0, 0, 0, 0, 0));
        assert_eq!(s.max_gap_at, None);
    }

    #[test]
    fn unsorted_positive_and_negative() {
        let mut b = clean_walk(20, 1);
        b.ts.swap(9, 10);
        let s = scan(&b, 30);
        assert_eq!(s.unsorted, 1);
        assert_eq!(rules_at(&s, 10), vec!["unsorted"]);
        assert_eq!(s.dupes, 0);
    }

    #[test]
    fn dupe_positive() {
        let mut b = clean_walk(20, 2);
        b.ts[5] = b.ts[4];
        let s = scan(&b, 30);
        assert_eq!(s.dupes, 1);
        assert!(rules_at(&s, 5).contains(&"dupe"));
        assert_eq!(s.unsorted, 0);
    }

    #[test]
    fn gap_positive_counts_missing_and_reports_start() {
        let mut b = clean_walk(20, 3);
        for t in b.ts.iter_mut().skip(10) {
            *t += 3 * MIN_NS; // step 4 bars between rows 9 and 10: 3 missing
        }
        let s = scan(&b, 30);
        assert_eq!(s.missing, 3);
        assert_eq!(s.max_gap_ns, 4 * MIN_NS);
        assert_eq!(s.max_gap_at, Some(b.ts[9] + MIN_NS));
        let gap: Vec<_> = s.issues.iter().filter(|i| i.rule == "gap").collect();
        assert_eq!(gap.len(), 1);
        assert_eq!((gap[0].ts, gap[0].n), (b.ts[9] + MIN_NS, 3));
        assert!(rules_at(&s, 10).contains(&"gap"));
    }

    #[test]
    fn misaligned_positive() {
        let mut b = clean_walk(20, 4);
        b.ts[7] += 1_000;
        let s = scan(&b, 30);
        assert!(rules_at(&s, 7).contains(&"misaligned"));
        assert_eq!(rules_at(&s, 6), Vec::<&str>::new());
    }

    #[test]
    fn ohlc_bad_positive() {
        let mut b = clean_walk(20, 5);
        b.h[3] = b.l[3] - 1.0; // h < l and h < o/c
        let s = scan(&b, 30);
        assert!(rules_at(&s, 3).contains(&"ohlc_bad"));
        assert!(s.ohlc_bad >= 1);
    }

    #[test]
    fn nonpositive_positive() {
        let mut b = clean_walk(20, 6);
        b.l[4] = 0.0;
        b.o[4] = 0.0;
        let s = scan(&b, 30);
        assert!(rules_at(&s, 4).contains(&"nonpositive"));
        assert_eq!(s.nonpositive, 1);
    }

    #[test]
    fn nan_positive() {
        let mut b = clean_walk(20, 8);
        b.c[12] = f64::NAN;
        let s = scan(&b, 30);
        assert!(rules_at(&s, 12).contains(&"nan"));
    }

    #[test]
    fn zero_vol_positive() {
        let mut b = clean_walk(20, 9);
        b.v[8] = 0.0;
        let s = scan(&b, 30);
        assert!(rules_at(&s, 8).contains(&"zero_vol"));
        assert_eq!(s.zero_vol, 1);
    }

    #[test]
    fn stuck_positive_and_limit_boundary() {
        let mut b = clean_walk(200, 10);
        for i in 100..=140 {
            // 41 identical rows: 40 repeats of row 100 > limit 30
            b.o[i] = b.c[100];
            b.h[i] = b.c[100];
            b.l[i] = b.c[100];
            b.c[i] = b.c[100];
            b.v[i] = 5.0;
        }
        let s = scan(&b, 30);
        assert_eq!(s.stuck_runs, 1);
        assert!(rules_at(&s, 120).contains(&"stuck"));
        assert!(!rules_at(&s, 99).contains(&"stuck"));

        // exactly 30 bars equal to the first one is not more than the limit
        let mut b = clean_walk(200, 11);
        for i in 100..130 {
            b.o[i] = b.c[100];
            b.h[i] = b.c[100];
            b.l[i] = b.c[100];
            b.c[i] = b.c[100];
            b.v[i] = 5.0;
        }
        let s = scan(&b, 30);
        assert_eq!(s.stuck_runs, 0, "a 30-row run must not trip a limit of 30");
    }

    #[test]
    fn spike_positive_and_negative() {
        let mut b = clean_walk(400, 12);
        // +10% log move at row 300, fully reversed at row 301
        let base = b.c[299];
        let up = base * 0.1f64.exp();
        b.c[300] = up;
        b.h[300] = up;
        b.o[300] = base;
        b.l[300] = base;
        b.c[301] = base;
        b.o[301] = up;
        b.h[301] = up;
        b.l[301] = base;
        let s = scan(&b, 30);
        assert!(rules_at(&s, 300).contains(&"spike"), "spike not flagged");

        // the same move that does not reverse is a level shift, not a bad print
        let mut b = clean_walk(400, 13);
        for i in 300..400 {
            b.c[i] *= 1.1;
            b.o[i] *= 1.1;
            b.h[i] *= 1.1;
            b.l[i] *= 1.1;
        }
        let s = scan(&b, 30);
        assert!(!rules_at(&s, 300).contains(&"spike"));
    }

    #[test]
    fn spike_indices_flags_a_reverting_spike() {
        let b = clean_walk(3_000, 14);
        let mut c = b.c.clone();
        let base = c[1_999];
        c[2_000] = base * 1.2;
        c[2_001] = base;
        let found = spike_indices(&c, 12.0, 1440);
        assert_eq!(found, vec![2_000]);
    }

    #[test]
    fn sliding_median_matches_brute_force() {
        let mut rng = Rng(99);
        for &window in &[2usize, 3, 37, 1440] {
            let keys: Vec<u64> = (0..6_000).map(|_| (rng.uniform() * 5.0).floor().to_bits()).collect();
            let mut sm = SlidingMedian::new(window);
            for (t, &k) in keys.iter().enumerate() {
                sm.push(k);
                if sm.len() > window {
                    sm.pop_oldest();
                }
                let mut live: Vec<f64> = keys[(t + 1).saturating_sub(window)..=t].iter().map(|&k| f64::from_bits(k)).collect();
                live.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let m = live.len();
                let want = if m % 2 == 1 { live[m / 2] } else { 0.5 * (live[m / 2 - 1] + live[m / 2]) };
                assert_eq!(sm.median(), want, "window {window} step {t}");
            }
        }
    }

    #[test]
    fn day_completeness_counts_per_utc_day() {
        let day = DAY_NS;
        let step = 8 * 3_600 * 1_000_000_000i64; // 3 slots per day
        // day 0: 3 observed; day 1: 2 observed (one duplicate); day 2: 1 observed
        let ts = vec![0, step, 2 * step, day + 5, day + step, day + step, 2 * day + 7];
        let days = day_completeness(&ts, step).unwrap();
        let got: Vec<(i64, u64)> = days.iter().map(|d| (d.day_ns, d.observed)).collect();
        assert_eq!(got, vec![(0, 3), (day, 2), (2 * day, 1)]);
        assert!(days.iter().all(|d| d.expected == 3));
    }

    #[test]
    fn civil_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
        assert_eq!(civil_from_days(19_783), (2024, 3, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
    }

    #[test]
    fn series_rules() {
        let ts: Vec<i64> = (0..40).map(|i| i as i64 * MIN_NS).collect();
        let mut x: Vec<f64> = (0..40).map(|i| (i as f64).sin()).collect();
        for v in x.iter_mut().take(12).skip(5) {
            *v = 0.25; // 7 identical rows: below the limit of 10
        }
        for v in x.iter_mut().take(38).skip(22) {
            *v = 0.5; // 16 identical rows; row 35 is removed below, leaving 15
        }
        x[2] = f64::NAN;
        let mut ts2 = ts.clone();
        ts2.remove(35); // gap of 2 at row 35
        x.remove(35);
        let s = scan_series(&ts2, &x, Some(MIN_NS), 10).unwrap();
        assert_eq!(s.nan, 1);
        assert_eq!(s.missing, 1);
        assert_eq!(s.stuck_runs, 1, "one run of 15 > 10");
        assert_eq!(s.dupes, 0);
        assert!(s.days.len() == 1);
        let s_none = scan_series(&ts2, &x, None, 10).unwrap();
        assert_eq!(s_none.missing, 0);
        assert!(s_none.days.is_empty());
    }

    #[test]
    fn flag_bits_are_distinct_powers_of_two() {
        let mut seen = 0u32;
        for (_, bit) in RULES {
            assert!(bit.is_power_of_two());
            assert_eq!(seen & bit, 0);
            seen |= bit;
        }
    }
}
