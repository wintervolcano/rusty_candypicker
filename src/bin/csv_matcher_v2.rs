use anyhow::{anyhow, Context, Result};
use clap::{Arg, ArgAction, Command};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Extract numeric with tolerant parsing (empty -> None).
fn parse_f64_opt(s: &str) -> Option<f64> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    if t.eq_ignore_ascii_case("nan") {
        return None;
    }
    t.parse::<f64>().ok()
}

/// Case-insensitive header -> index map; header names are normalized (trim + lowercase + strip leading '#')
fn header_index_map(header: &StringRecord) -> HashMap<String, usize> {
    let mut m = HashMap::new();
    for (i, f) in header.iter().enumerate() {
        let mut s = f.trim().to_string();
        if let Some(stripped) = s.strip_prefix('#') {
            s = stripped.to_string();
        }
        m.insert(s.to_ascii_lowercase(), i);
    }
    m
}

/// Find first present header among candidates (case-insensitive)
fn find_col<'a>(hmap: &'a HashMap<String, usize>, candidates: &[&str]) -> Option<usize> {
    for c in candidates {
        if let Some(&idx) = hmap.get(&c.to_ascii_lowercase()) {
            return Some(idx);
        }
    }
    None
}

/// Period extraction: prefer explicit period fields; if absent, use 1/f0_*.
fn extract_period(hmap: &HashMap<String, usize>, row: &StringRecord) -> Option<f64> {
    let period_cols = ["p0_new", "period", "p0", "p", "p_sec", "per", "per_s"];
    if let Some(idx) = find_col(hmap, &period_cols) {
        if let Some(v) = row.get(idx).and_then(parse_f64_opt) {
            return Some(v);
        }
    }
    let f0_cols = ["f0_opt", "f0_new", "f0", "freq", "frequency_hz"];
    if let Some(idx) = find_col(hmap, &f0_cols) {
        if let Some(v) = row.get(idx).and_then(parse_f64_opt) {
            if v != 0.0 {
                return Some(1.0 / v);
            }
        }
    }
    None
}

fn extract_dm(hmap: &HashMap<String, usize>, row: &StringRecord) -> Option<f64> {
    let dm_cols = ["dm_new", "dm_opt", "dm", "refdm"];
    find_col(hmap, &dm_cols).and_then(|idx| row.get(idx).and_then(parse_f64_opt))
}

fn extract_acc(hmap: &HashMap<String, usize>, row: &StringRecord) -> Option<f64> {
    let a_cols = ["acc_new", "acc_opt", "acc", "acceleration"];
    find_col(hmap, &a_cols).and_then(|idx| row.get(idx).and_then(parse_f64_opt))
}

/// PNG/plot path passthrough, kept as a string (not numeric).
fn extract_png(hmap: &HashMap<String, usize>, row: &StringRecord) -> Option<String> {
    let png_cols = ["png_path", "png_file", "pngfile", "png", "plot_file", "plot", "filename"];
    find_col(hmap, &png_cols).and_then(|idx| {
        let v = row.get(idx)?.trim();
        if v.is_empty() {
            None
        } else {
            Some(v.to_string())
        }
    })
}

/// A parsed row with cached numeric fields for matching and file provenance.
#[derive(Clone)]
struct RowRef {
    file_id: usize,
    row_idx: usize,
    period: Option<f64>,
    dm: Option<f64>,
    acc: Option<f64>,
    png: Option<String>,
}

struct FileData {
    path: PathBuf,
    header: StringRecord,
    rows: Vec<StringRecord>,
    hmap: HashMap<String, usize>,
}

fn read_csv(path: &Path) -> Result<FileData> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("opening CSV {}", path.display()))?;

    let header = rdr
        .headers()
        .with_context(|| format!("reading header from {}", path.display()))?
        .clone();

    let hmap = header_index_map(&header);

    let mut rows = Vec::new();
    for rec in rdr.records() {
        rows.push(rec?);
    }

    Ok(FileData {
        path: path.to_path_buf(),
        header,
        rows,
        hmap,
    })
}

/// DM/ACC absolute tolerance check; if tol None -> ignore dimension.
fn dim_match_abs(a: Option<f64>, b: Option<f64>, tol: &Option<f64>) -> bool {
    match tol {
        None => true,
        Some(t) => match (a, b) {
            (Some(x), Some(y)) => (x - y).abs() <= *t,
            _ => false,
        },
    }
}

/// Exact bounds [lo, hi] such that any q in that range satisfies
/// |center - q| / mean(center, q) <= frac_tol.
/// Derived algebraically (not approximated) from the symmetric-mean fractional metric,
/// so — like the old additive bucket range — this is a safe, exact superset: nothing
/// that could actually match is ever outside these bounds.
fn frac_bounds(center: f64, frac_tol: f64) -> (f64, f64) {
    let lo_ratio = (1.0 - frac_tol / 2.0) / (1.0 + frac_tol / 2.0);
    let hi_ratio = (1.0 + frac_tol / 2.0) / (1.0 - frac_tol / 2.0);
    (center * lo_ratio, center * hi_ratio)
}

/// Log-space bucket: fractional tolerance is additive in log-period space, so bucketing
/// on ln(period) (rather than on period itself, as the old additive-tolerance code did)
/// is the natural equivalent of `bucket_abs` for a multiplicative tolerance.
fn log_bucket(p: f64, bin_width: f64) -> i64 {
    (p.ln() / bin_width).floor() as i64
}

fn push_log_bucket_range(out: &mut Vec<i64>, lo: f64, hi: f64, bin_width: f64) {
    if lo <= 0.0 || hi <= 0.0 || !lo.is_finite() || !hi.is_finite() {
        return;
    }
    let b_lo = log_bucket(lo, bin_width);
    let b_hi = log_bucket(hi, bin_width);
    let mut b = b_lo;
    while b <= b_hi {
        out.push(b);
        b += 1;
    }
}

/// All buckets that could contain a direct or harmonic fractional match for period `p`.
fn candidate_buckets(p: f64, frac_tol: f64, harmonics: bool, hmax: u32, bin_width: f64) -> Vec<i64> {
    let mut out = Vec::new();
    let (lo, hi) = frac_bounds(p, frac_tol);
    push_log_bucket_range(&mut out, lo, hi, bin_width);
    if harmonics {
        for k in 2..=hmax {
            let kf = k as f64;
            // other row could be ~ p/k  (covers p ~= k * other)
            let (lo_div, hi_div) = frac_bounds(p / kf, frac_tol);
            push_log_bucket_range(&mut out, lo_div, hi_div, bin_width);
            // other row could be ~ p*k  (covers other ~= k * p)
            let (lo_mul, hi_mul) = frac_bounds(p * kf, frac_tol);
            push_log_bucket_range(&mut out, lo_mul, hi_mul, bin_width);
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// How two periods matched, kept so it can be reported in the pairs output.
#[derive(Clone, Copy)]
enum MatchKind {
    Direct,
    AisKTimesB(u32), // period_a ~= k * period_b
    BisKTimesA(u32), // period_b ~= k * period_a
}

impl MatchKind {
    fn label(&self) -> String {
        match self {
            MatchKind::Direct => "direct".to_string(),
            MatchKind::AisKTimesB(k) => format!("a={}x*b", k),
            MatchKind::BisKTimesA(k) => format!("b={}x*a", k),
        }
    }
    fn k(&self) -> u32 {
        match self {
            MatchKind::Direct => 1,
            MatchKind::AisKTimesB(k) | MatchKind::BisKTimesA(k) => *k,
        }
    }
}

/// Symmetric fractional difference: |x-y| / mean(x,y). Symmetric under swapping x and y,
/// so it doesn't matter which row is labelled "a" and which is "b".
fn frac_diff(x: f64, y: f64) -> f64 {
    let mean = 0.5 * (x + y);
    if mean <= 0.0 {
        f64::INFINITY
    } else {
        (x - y).abs() / mean
    }
}

/// Exact match check (bucket lookups above are only ever used to shortlist candidates;
/// this function is the actual source of truth for whether two rows match).
/// Returns the MatchKind plus the fractional difference of the two quantities that were
/// actually compared (e.g. for a harmonic match, of pa vs k*pb -- not of the raw pa vs pb).
fn classify_period_match(
    pa: f64,
    pb: f64,
    frac_tol: f64,
    harmonics: bool,
    hmax: u32,
) -> Option<(MatchKind, f64)> {
    let d = frac_diff(pa, pb);
    if d <= frac_tol {
        return Some((MatchKind::Direct, d));
    }
    if harmonics {
        for k in 2..=hmax {
            let kf = k as f64;
            let d1 = frac_diff(pa, kf * pb);
            if d1 <= frac_tol {
                return Some((MatchKind::AisKTimesB(k), d1));
            }
            let d2 = frac_diff(pb, kf * pa);
            if d2 <= frac_tol {
                return Some((MatchKind::BisKTimesA(k), d2));
            }
        }
    }
    None
}

/// Simple disjoint-set / union-find so that a chain of matches (e.g. row A matches row B,
/// row B also matches row C in a third file) is tracked as one connected group. Two rows
/// are placed in the same group if there is *any* path of direct pairwise matches between
/// them; the group is reported via `match_group`, while the exact pairwise evidence for
/// *why* they were linked is kept separately in the pairs table.
struct UnionFind {
    parent: Vec<usize>,
}
impl UnionFind {
    fn new(n: usize) -> Self {
        UnionFind {
            parent: (0..n).collect(),
        }
    }
    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            let root = self.find(self.parent[x]);
            self.parent[x] = root;
        }
        self.parent[x]
    }
    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent[ra] = rb;
        }
    }
}

/// A single verified cross-file correspondence, kept for the pairs output.
struct PairRecord {
    gidx_a: usize,
    gidx_b: usize,
    kind: MatchKind,
    frac_diff: f64,
}

fn out_path_for(input: &Path, out_suffix: &str) -> PathBuf {
    let stem = input
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("out.csv"))
        .to_string_lossy()
        .to_string();
    let base = stem.rsplit_once('.').map(|(b, _)| b).unwrap_or(&stem);
    let out_name = if out_suffix.contains('.') {
        // out_suffix already carries its own extension (e.g. "_matched.csv") -- don't add another.
        format!("{}{}", base, out_suffix)
    } else {
        format!("{}{}.csv", base, out_suffix)
    };
    input.with_file_name(out_name)
}

fn fmt_opt(v: Option<f64>) -> String {
    match v {
        Some(x) => format!("{}", x),
        None => "NA".to_string(),
    }
}

fn main() -> Result<()> {
    let matches = Command::new("csv_matcher")
        .about("Find rows that match ACROSS CSV files (period matched fractionally, i.e. |ΔP|/mean(P); optional DM/ACC absolute gates; optional harmonics), write only matched rows per input preserving original headers/columns, and emit an explicit pairwise correspondence table linking rows between files.")
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .num_args(1..)
                .required(true)
                .help("Input CSV files (shell globs like -i 'fold*.csv' expand in your shell)."),
        )
        .arg(
            Arg::new("ptol_frac")
                .long("ptol-frac")
                .num_args(1)
                .required(true)
                .help("Fractional period tolerance: |Pa-Pb| / mean(Pa,Pb), e.g. 1e-4 for 0.01%. Replaces the old absolute --ptol (which compared a fixed number of seconds regardless of period, biasing against slow pulsars) -- this scales with period instead."),
        )
        .arg(
            Arg::new("dmtol")
                .long("dmtol")
                .num_args(1)
                .required(false)
                .help("Absolute DM tolerance (optional). If not set, DM is ignored."),
        )
        .arg(
            Arg::new("acctol")
                .long("acctol")
                .num_args(1)
                .required(false)
                .help("Absolute acceleration tolerance (optional). If not set, acc is ignored."),
        )
        .arg(
            Arg::new("harmonics")
                .long("harmonics")
                .action(ArgAction::SetTrue)
                .help("Enable harmonic period matching (k*P, P/k with k=2..hmax)."),
        )
        .arg(
            Arg::new("hmax")
                .long("hmax")
                .num_args(1)
                .default_value("8")
                .help("Max harmonic factor k when --harmonics is enabled (default 8)."),
        )
        .arg(
            Arg::new("out_suffix")
                .long("out-suffix")
                .num_args(1)
                .default_value("_matched.csv")
                .help("Suffix appended to each input filename for its matched output."),
        )
        .arg(
            Arg::new("pairs_out")
                .long("pairs-out")
                .num_args(1)
                .default_value("matched_pairs.csv")
                .help("Path for the pairwise correspondence table linking rows across files."),
        )
        .get_matches();

    let inputs: Vec<String> = matches
        .get_many::<String>("input")
        .unwrap()
        .map(|s| s.to_string())
        .collect();

    let ptol_frac: f64 = matches
        .get_one::<String>("ptol_frac")
        .unwrap()
        .parse()
        .context("parsing --ptol-frac")?;
    if !(ptol_frac > 0.0) {
        return Err(anyhow!("--ptol-frac must be a positive number"));
    }
    if ptol_frac >= 2.0 {
        return Err(anyhow!(
            "--ptol-frac must be < 2 (it's a fractional tolerance on |Pa-Pb|/mean(Pa,Pb), typically 1e-6..1e-2)"
        ));
    }
    // Bin width for the log-space bucket index. Any consistent positive value works here --
    // it only affects prefiltering efficiency, never correctness (classify_period_match is
    // the actual source of truth). Using ptol_frac directly keeps bin count roughly matched
    // to the tolerance scale.
    let bin_width = (1.0 + ptol_frac).ln();

    let dmtol: Option<f64> = matches
        .get_one::<String>("dmtol")
        .map(|s| s.parse().context("parsing --dmtol"))
        .transpose()?;

    let acctol: Option<f64> = matches
        .get_one::<String>("acctol")
        .map(|s| s.parse().context("parsing --acctol"))
        .transpose()?;

    let harmonics = matches.get_flag("harmonics");
    let hmax: u32 = matches
        .get_one::<String>("hmax")
        .unwrap()
        .parse()
        .context("parsing --hmax")?;
    if harmonics && hmax < 2 {
        return Err(anyhow!("--hmax must be >= 2 when --harmonics is set"));
    }

    let out_suffix = matches.get_one::<String>("out_suffix").unwrap();
    let pairs_out = matches.get_one::<String>("pairs_out").unwrap();

    if inputs.len() < 2 {
        return Err(anyhow!(
            "Provide at least two CSV inputs for cross-file matching (use -i file1.csv file2.csv ...)."
        ));
    }

    // Read all files
    let mut files = Vec::<FileData>::new();
    for p in &inputs {
        let path = Path::new(p);
        let fd = read_csv(path)?;
        println!("[INFO] Loaded {} rows from {}", fd.rows.len(), path.display());
        files.push(fd);
    }

    // Build global list of row refs.
    let mut all_rows = Vec::<RowRef>::new();
    for (fid, f) in files.iter().enumerate() {
        for (idx, rec) in f.rows.iter().enumerate() {
            all_rows.push(RowRef {
                file_id: fid,
                row_idx: idx,
                period: extract_period(&f.hmap, rec),
                dm: extract_dm(&f.hmap, rec),
                acc: extract_acc(&f.hmap, rec),
                png: extract_png(&f.hmap, rec),
            });
        }
    }

    // Bucket index: bucket -> list of global indices
    let mut buckets: HashMap<i64, Vec<usize>> = HashMap::new();
    for (gidx, rr) in all_rows.iter().enumerate() {
        if let Some(p) = rr.period {
            let b = log_bucket(p, bin_width);
            buckets.entry(b).or_default().push(gidx);
        }
    }

    let mut uf = UnionFind::new(all_rows.len());
    let mut pairs: Vec<PairRecord> = Vec::new();
    let mut seen_pairs: HashSet<(usize, usize)> = HashSet::new();

    for (gidx, rr) in all_rows.iter().enumerate() {
        let Some(p1) = rr.period else { continue };
        let neigh = candidate_buckets(p1, ptol_frac, harmonics, hmax, bin_width);
        for nb in neigh {
            let Some(list) = buckets.get(&nb) else { continue };
            for &other_gidx in list {
                if other_gidx == gidx {
                    continue;
                }
                let oo = &all_rows[other_gidx];
                if oo.file_id == rr.file_id {
                    continue; // only across files
                }
                let Some(p2) = oo.period else { continue };

                let Some((kind, d)) = classify_period_match(p1, p2, ptol_frac, harmonics, hmax)
                else {
                    continue;
                };
                if !dim_match_abs(rr.dm, oo.dm, &dmtol) {
                    continue;
                }
                if !dim_match_abs(rr.acc, oo.acc, &acctol) {
                    continue;
                }

                let key = (gidx.min(other_gidx), gidx.max(other_gidx));
                if seen_pairs.insert(key) {
                    // report consistently in (lower gidx, higher gidx) order
                    let (a_idx, _b_idx) = key;
                    let kind_reported = if a_idx == gidx {
                        kind
                    } else {
                        // flip a/b labelling of the MatchKind to match canonical order
                        match kind {
                            MatchKind::Direct => MatchKind::Direct,
                            MatchKind::AisKTimesB(k) => MatchKind::BisKTimesA(k),
                            MatchKind::BisKTimesA(k) => MatchKind::AisKTimesB(k),
                        }
                    };
                    pairs.push(PairRecord {
                        gidx_a: key.0,
                        gidx_b: key.1,
                        kind: kind_reported,
                        frac_diff: d,
                    });
                }
                uf.union(gidx, other_gidx);
            }
        }
    }

    // Group rows by union-find root; only components touching >=2 distinct files count as matched.
    let mut root_members: HashMap<usize, Vec<usize>> = HashMap::new();
    for gidx in 0..all_rows.len() {
        let root = {
            // temporary mutable borrow scope
            let r = uf.find(gidx);
            r
        };
        root_members.entry(root).or_default().push(gidx);
    }

    // Assign sequential, stable match_group ids (ordered by smallest gidx in each group).
    let mut qualifying_roots: Vec<(usize, Vec<usize>)> = root_members
        .into_iter()
        .filter(|(_, members)| {
            let mut file_set = HashSet::new();
            for &g in members {
                file_set.insert(all_rows[g].file_id);
            }
            file_set.len() >= 2
        })
        .collect();
    qualifying_roots.sort_by_key(|(_, members)| *members.iter().min().unwrap());

    let mut group_id_of: HashMap<usize, u32> = HashMap::new();
    for (gid, (_, members)) in qualifying_roots.iter().enumerate() {
        let id = (gid + 1) as u32;
        for &g in members {
            group_id_of.insert(g, id);
        }
    }

    // Collect matched rows per file (with their group id) and write outputs.
    let mut per_file_selected: Vec<Vec<(usize, u32)>> = vec![Vec::new(); files.len()];
    for (gidx, rr) in all_rows.iter().enumerate() {
        if let Some(&gid) = group_id_of.get(&gidx) {
            per_file_selected[rr.file_id].push((rr.row_idx, gid));
        }
    }

    for (fid, f) in files.iter().enumerate() {
        let out_path = out_path_for(&f.path, out_suffix);

        let mut w = WriterBuilder::new()
            .from_path(&out_path)
            .with_context(|| format!("creating {}", out_path.display()))?;

        let mut header_out: Vec<String> = f.header.iter().map(|s| s.to_string()).collect();
        header_out.push("match_group".to_string());
        w.write_record(&header_out)?;

        for &(rid, gid) in &per_file_selected[fid] {
            let mut rec: Vec<String> = f.rows[rid].iter().map(|s| s.to_string()).collect();
            rec.push(gid.to_string());
            w.write_record(&rec)?;
        }
        w.flush()?;
        println!(
            "[INFO] Wrote {} matched rows -> {}",
            per_file_selected[fid].len(),
            out_path.display()
        );
    }

    // Write the explicit pairwise correspondence table.
    {
        let mut w = WriterBuilder::new()
            .from_path(pairs_out)
            .with_context(|| format!("creating {}", pairs_out))?;
        w.write_record([
            "match_group",
            "file_a",
            "row_a",
            "period_a",
            "dm_a",
            "acc_a",
            "png_a",
            "file_b",
            "row_b",
            "period_b",
            "dm_b",
            "acc_b",
            "png_b",
            "match_type",
            "harmonic_k",
            "period_frac_diff",
            "period_diff_s",
        ])?;
        for p in &pairs {
            let ra = &all_rows[p.gidx_a];
            let rb = &all_rows[p.gidx_b];
            let gid = group_id_of
                .get(&p.gidx_a)
                .copied()
                .unwrap_or_default();
            let pa = ra.period.unwrap();
            let pb = rb.period.unwrap();
            w.write_record([
                gid.to_string(),
                files[ra.file_id].path.file_name().unwrap().to_string_lossy().to_string(),
                (ra.row_idx + 1).to_string(), // 1-based data row number in the ORIGINAL input file
                format!("{}", pa),
                fmt_opt(ra.dm),
                fmt_opt(ra.acc),
                ra.png.clone().unwrap_or_else(|| "NA".to_string()),
                files[rb.file_id].path.file_name().unwrap().to_string_lossy().to_string(),
                (rb.row_idx + 1).to_string(),
                format!("{}", pb),
                fmt_opt(rb.dm),
                fmt_opt(rb.acc),
                rb.png.clone().unwrap_or_else(|| "NA".to_string()),
                p.kind.label(),
                p.kind.k().to_string(),
                format!("{}", p.frac_diff), // the actual criterion value: |compared_a - compared_b| / mean, accounting for k
                format!("{}", (pa - pb).abs()), // raw |Pa-Pb| in seconds, NOT harmonic-corrected -- informational only
            ])?;
        }
        w.flush()?;
        println!(
            "[INFO] Wrote {} matched pairs -> {}",
            pairs.len(),
            pairs_out
        );
    }

    Ok(())
}
