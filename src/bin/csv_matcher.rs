use anyhow::{anyhow, Context, Result};
use clap::{Arg, ArgAction, Command};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Extract numeric with tolerant parsing (empty -> None).
fn parse_f64_opt(s: &str) -> Option<f64> {
    let t = s.trim();
    if t.is_empty() { return None; }
    // handle common "nan" / "NaN"
    if t.eq_ignore_ascii_case("nan") { return None; }
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
/// Returns (period_seconds, index_of_source_column_used) if found.
fn extract_period_indices(hmap: &HashMap<String, usize>, row: &StringRecord) -> Option<(f64, usize)> {
    // Try period-like columns
    let period_cols = ["p0_new","period","p0","p","p_sec","per","per_s"];
    if let Some(idx) = find_col(hmap, &period_cols) {
        if let Some(v) = row.get(idx).and_then(parse_f64_opt) {
            return Some((v, idx));
        }
    }
    // Try f0 columns (frequency Hz) -> period = 1/f0
    let f0_cols = ["f0_opt","f0_new","f0","freq","frequency_hz"];
    if let Some(idx) = find_col(hmap, &f0_cols) {
        if let Some(v) = row.get(idx).and_then(parse_f64_opt) {
            if v != 0.0 { return Some((1.0 / v, idx)); }
        }
    }
    None
}

/// DM extraction: return (dm, idx) if present
fn extract_dm(hmap: &HashMap<String, usize>, row: &StringRecord) -> Option<(f64, usize)> {
    let dm_cols = ["dm_new","dm_opt","dm","refdm"];
    if let Some(idx) = find_col(hmap, &dm_cols) {
        return row.get(idx).and_then(parse_f64_opt).map(|v| (v, idx));
    }
    None
}

/// ACC extraction: return (acc, idx) if present
fn extract_acc(hmap: &HashMap<String, usize>, row: &StringRecord) -> Option<(f64, usize)> {
    let a_cols = ["acc_new","acc_opt","acc","acceleration"];
    if let Some(idx) = find_col(hmap, &a_cols) {
        return row.get(idx).and_then(parse_f64_opt).map(|v| (v, idx));
    }
    None
}

/// A parsed row with cached numeric fields for matching and file provenance.
#[derive(Clone)]
struct RowRef {
    file_id: usize,         // which input file
    row_idx: usize,         // index in that file's data vector
    period: Option<f64>,    // seconds
    dm: Option<f64>,
    acc: Option<f64>,
}

/// Holding the original CSV content for a file
struct FileData {
    path: PathBuf,
    header: StringRecord,
    rows: Vec<StringRecord>,
    hmap: HashMap<String, usize>,
}

fn read_csv(path: &Path) -> Result<FileData> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true) // tolerate different row lengths
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

/// Absolute tolerance check with optional harmonics.
/// Returns true if |p1 - p2| <= ptol OR there exists k in [2..=hmax] with
/// |p1 - k*p2| <= ptol OR |p2 - k*p1| <= ptol (when harmonics=true).
fn periods_match_abs(p1: f64, p2: f64, ptol: f64, harmonics: bool, hmax: u32) -> bool {
    if (p1 - p2).abs() <= ptol {
        return true;
    }
    if harmonics {
        for k in 2..=hmax {
            let kf = k as f64;
            if (p1 - kf * p2).abs() <= ptol { return true; }
            if (p2 - kf * p1).abs() <= ptol { return true; }
        }
    }
    false
}

/// DM/ACC absolute tolerance check; if tol None -> ignore dimension.
/// If tol Some(t), both sides must be present and |Δ| <= t.
fn dim_match_abs(a: Option<f64>, b: Option<f64>, tol: &Option<f64>) -> bool {
    match tol {
        None => true,
        Some(t) => match (a, b) {
            (Some(x), Some(y)) => (x - y).abs() <= *t,
            _ => false,
        },
    }
}

/// Build buckets for absolute tolerance to prune comparisons.
/// bucket = floor(p / ptol)
fn bucket_abs(p: f64, ptol: f64) -> i64 {
    (p / ptol).floor() as i64
}

fn main() -> Result<()> {
    let matches = Command::new("csv_matcher")
        .about("Find rows that match ACROSS CSV files (period/DM/ACC, optional harmonics), and write only matched rows per input, preserving original headers/columns.")
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .num_args(1..)
                .required(true)
                .help("Input CSV files (shell globs like -i 'fold*.csv' expand in your shell)."),
        )
        .arg(
            Arg::new("ptol")
                .long("ptol")
                .num_args(1)
                .required(true)
                .help("Absolute period tolerance in seconds (e.g., 1e-6)."),
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
                .help("Enable harmonic period matching (k·P, P/k with k=2..hmax)."),
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
        .get_matches();

    let inputs: Vec<String> = matches
        .get_many::<String>("input")
        .unwrap()
        .map(|s| s.to_string())
        .collect();

    let ptol: f64 = matches
        .get_one::<String>("ptol")
        .unwrap()
        .parse()
        .context("parsing --ptol")?;

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

    let out_suffix = matches.get_one::<String>("out_suffix").unwrap();

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

    // Build global list of row refs + bucket index on period to limit comparisons.
    // We only index rows that have a valid period value.
    let mut all_rows = Vec::<RowRef>::new();
    for (fid, f) in files.iter().enumerate() {
        for (idx, rec) in f.rows.iter().enumerate() {
            let (period_opt, _, dm_opt, acc_opt) = {
                let p = extract_period_indices(&f.hmap, rec).map(|(v, _)| v);
                let d = extract_dm(&f.hmap, rec).map(|(v, _)| v);
                let a = extract_acc(&f.hmap, rec).map(|(v, _)| v);
                (p, (), d, a)
            };
            all_rows.push(RowRef {
                file_id: fid,
                row_idx: idx,
                period: period_opt,
                dm: dm_opt,
                acc: acc_opt,
            });
        }
    }

    // Bucket index: bucket -> list of global indices
    let mut buckets: HashMap<i64, Vec<usize>> = HashMap::new();
    for (gidx, rr) in all_rows.iter().enumerate() {
        if let Some(p) = rr.period {
            let b = bucket_abs(p, ptol);
            buckets.entry(b).or_default().push(gidx);
        }
    }

    // For each row, test against candidates from other files in relevant buckets.
    // Mark rows that have at least one match with a row from a DIFFERENT file.
    let mut matched: Vec<bool> = vec![false; all_rows.len()];

    // Helper to gather plausible neighbor indices for an absolute-ptol + harmonics scenario
    let mut neighbor_cache: HashMap<(i64, u32, bool), Vec<i64>> = HashMap::new();
    let mut neighbors_for = |b0: i64, hmax: u32, harmonics: bool| -> Vec<i64> {
        // Cache by (bucket, hmax, harmonics)
        if let Some(v) = neighbor_cache.get(&(b0, hmax, harmonics)) {
            return v.clone();
        }
        let mut out = vec![b0 - 1, b0, b0 + 1]; // same bucket +/- 1 for boundary effects
        if harmonics {
            for k in 2..=hmax {
                let kf = k as f64;
                // buckets for b0*k and b0/k are not strictly integer transforms,
                // so compute representative centers:
                // center period ≈ (b0 + 0.5) * ptol
                let center = (b0 as f64 + 0.5) * ptol;
                let hk = center * kf;
                let hk_b = bucket_abs(hk, ptol);
                out.extend_from_slice(&[hk_b - 1, hk_b, hk_b + 1]);

                let hk_div = center / kf;
                let hk_div_b = bucket_abs(hk_div, ptol);
                out.extend_from_slice(&[hk_div_b - 1, hk_div_b, hk_div_b + 1]);
            }
        }
        out.sort_unstable();
        out.dedup();
        neighbor_cache.insert((b0, hmax, harmonics), out.clone());
        out
    };

    for (gidx, rr) in all_rows.iter().enumerate() {
        let Some(p1) = rr.period else { continue; };
        let b0 = bucket_abs(p1, ptol);
        let neigh = neighbors_for(b0, hmax, harmonics);
        for nb in neigh {
            if let Some(list) = buckets.get(&nb) {
                for &other_gidx in list {
                    if other_gidx == gidx { continue; }
                    let oo = &all_rows[other_gidx];
                    if oo.file_id == rr.file_id { continue; } // only across files
                    if let Some(p2) = oo.period {
                        if !periods_match_abs(p1, p2, ptol, harmonics, hmax) {
                            continue;
                        }
                        if !dim_match_abs(rr.dm,  oo.dm,  &dmtol)  { continue; }
                        if !dim_match_abs(rr.acc, oo.acc, &acctol) { continue; }
                        matched[gidx] = true;
                        matched[other_gidx] = true;
                        // keep scanning to mark more matches for the same rr;
                        // If you want to stop after first, uncomment next line:
                        // break 'outer;
                    }
                }
            }
        }
    }

    // Collect matched rows per file and write outputs preserving headers and column order.
    let mut per_file_selected: Vec<Vec<usize>> = vec![Vec::new(); files.len()];
    for (gidx, rr) in all_rows.iter().enumerate() {
        if matched[gidx] {
            per_file_selected[rr.file_id].push(rr.row_idx);
        }
    }

    for (fid, f) in files.iter().enumerate() {
        let count = per_file_selected[fid].len();
        let out_path = {
            let p = &f.path;
            let stem = p.file_name().unwrap_or_else(|| std::ffi::OsStr::new("out.csv")).to_string_lossy();
            let stem_s = stem.to_string();
            // naive suffix add before extension
            let out = if let Some((base, ext)) = stem_s.rsplit_once('.') {
                format!("{}{}.{ext}", base, out_suffix)
            } else {
                format!("{}{}", stem_s, out_suffix)
            };
            p.with_file_name(out)
        };

        let mut w = WriterBuilder::new().from_path(&out_path)
            .with_context(|| format!("creating {}", out_path.display()))?;
        w.write_record(&f.header)?;
        for &rid in &per_file_selected[fid] {
            w.write_record(&f.rows[rid])?;
        }
        w.flush()?;
        println!(
            "[INFO] Wrote {} matched rows -> {}",
            count,
            out_path.display()
        );
    }

    Ok(())
}

