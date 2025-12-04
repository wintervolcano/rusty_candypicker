// src/csv_cluster.rs
use anyhow::{anyhow, Context, Result};
use csv::{ReaderBuilder, StringRecord, Writer};
use std::cmp::Ordering;
use std::fs::File;
use std::path::Path;

const SPEED_OF_LIGHT: f64 = 299_792_458.0;

#[derive(Clone, Debug)]
struct RowView {
    /// The complete, original row (all columns, in order).
    row: Vec<String>,
    /// Source filename (optional column in output).
    source: String,
    /// Extracted fields for clustering:
    period_s: f64,
    dm: f64,
    acc: f64,
    snr: f64,
}

/// Which column set we’re using.
#[derive(Clone, Copy, Debug)]
enum Schema {
    FoldSearch, // (#id, dm_new, p0_new, acc_new, S/N_new, ...)
    Pics,       // (dm_opt, f0_opt, acc_opt, sn_fold, ...)
}

#[derive(Clone, Debug)]
struct ColMap {
    #[allow(dead_code)]
    schema: Schema,
    idx_period_like: usize, // p0_new or f0_opt
    idx_dm: usize,          // dm_new or dm_opt
    idx_acc: usize,         // acc_new or acc_opt
    idx_snr: usize,         // S/N_new or sn_fold
    // Whether idx_period_like is already a period (true) or a frequency f0 (false).
    is_period: bool,
}

fn find_col(header: &StringRecord, name: &str) -> Option<usize> {
    header.iter().position(|h| h.trim() == name)
}

fn detect_schema(header: &StringRecord) -> Result<ColMap> {
    // Try FoldSearch first
    if let (Some(i_p0), Some(i_dm), Some(i_acc), Some(i_snr)) = (
        find_col(header, "p0_new"),
        find_col(header, "dm_new"),
        find_col(header, "acc_new"),
        find_col(header, "S/N_new"),
    ) {
        return Ok(ColMap {
            schema: Schema::FoldSearch,
            idx_period_like: i_p0,
            idx_dm: i_dm,
            idx_acc: i_acc,
            idx_snr: i_snr,
            is_period: true,
        });
    }

    // Then PICS / TRAPUM style
    if let (Some(i_f0), Some(i_dm), Some(i_acc), Some(i_snr)) = (
        find_col(header, "f0_opt"),
        find_col(header, "dm_opt"),
        find_col(header, "acc_opt"),
        find_col(header, "sn_fold"),
    ) {
        return Ok(ColMap {
            schema: Schema::Pics,
            idx_period_like: i_f0,
            idx_dm: i_dm,
            idx_acc: i_acc,
            idx_snr: i_snr,
            is_period: false, // it's f0; convert to period = 1/f0
        });
    }

    Err(anyhow!(
        "Unsupported CSV header: could not find either \
         (p0_new, dm_new, acc_new, S/N_new) or (f0_opt, dm_opt, acc_opt, sn_fold)."
    ))
}

fn parse_row(cols: &ColMap, rec: &StringRecord, src: &str) -> Option<RowView> {
    // Defensive: ensure row has enough columns
    let get = |i: usize| rec.get(i).unwrap_or("").trim();

    let dm = get(cols.idx_dm).parse::<f64>().ok()?;
    let acc = get(cols.idx_acc).parse::<f64>().ok()?;
    let snr = get(cols.idx_snr).parse::<f64>().ok()?;

    let period_s = if cols.is_period {
        let p = get(cols.idx_period_like).parse::<f64>().ok()?;
        if p <= 0.0 || !p.is_finite() {
            return None;
        }
        p
    } else {
        // f0 → period
        let f0 = get(cols.idx_period_like).parse::<f64>().ok()?;
        if f0 <= 0.0 || !f0.is_finite() {
            return None;
        }
        1.0 / f0
    };

    // Keep entire row as Vec<String>
    let row: Vec<String> = rec.iter().map(|s| s.to_string()).collect();

    Some(RowView {
        row,
        source: src.to_string(),
        period_s,
        dm,
        acc,
        snr,
    })
}

/// Acceleration-aware period match with optional harmonics.
fn periods_match(
    a: &RowView,
    b: &RowView,
    ptol_abs: f64,
    dmtol: Option<f64>,
    acctol: Option<f64>,
    allow_harmonics: bool,
    tobs_opt: Option<f64>,
) -> bool {
    // Optional gates first
    if let Some(d) = dmtol {
        if (a.dm - b.dm).abs() > d {
            return false;
        }
    }
    if let Some(t) = acctol {
        if (a.acc - b.acc).abs() > t {
            return false;
        }
    }

    // Acceleration correction (match b to a's frame)
    let tobs_over_c = tobs_opt.unwrap_or(600.0) / SPEED_OF_LIGHT;
    let f0_b = 1.0 / b.period_s;
    let p_b_corr = 1.0 / (f0_b - (b.acc - a.acc) * f0_b * tobs_over_c);

    if !allow_harmonics {
        return (a.period_s - p_b_corr).abs() <= ptol_abs;
    }

    // Harmonic-aware: check small integer multiples up to 16
    // Test |p_a - k * p_b| <= ptol OR |k * p_a - p_b| <= ptol
    const HMAX: usize = 16;
    for k in 1..=HMAX {
        let kf = k as f64;
        if (a.period_s - kf * p_b_corr).abs() <= ptol_abs {
            return true;
        }
        if (kf * a.period_s - p_b_corr).abs() <= ptol_abs {
            return true;
        }
    }
    false
}

/// Greedy SNR-first clustering. Higher SNR rows win; all related rows are suppressed.
fn cluster_rows(
    mut rows: Vec<RowView>,
    ptol_abs: f64,
    dmtol: Option<f64>,
    acctol: Option<f64>,
    allow_harmonics: bool,
    tobs_opt: Option<f64>,
) -> Vec<RowView> {
    // Sort by SNR descending so the first time we see a cluster we keep the strongest.
    rows.sort_by(|a, b| {
        // NaNs sorted to end, otherwise descending snr
        if !a.snr.is_finite() && !b.snr.is_finite() {
            Ordering::Equal
        } else if !a.snr.is_finite() {
            Ordering::Greater
        } else if !b.snr.is_finite() {
            Ordering::Less
        } else {
            b.snr
                .partial_cmp(&a.snr)
                .unwrap_or(Ordering::Equal)
        }
    });

    let n = rows.len();
    let mut removed = vec![false; n];
    let mut picked = Vec::with_capacity(n);

    for i in 0..n {
        if removed[i] {
            continue;
        }
        // Keep this as the pivot
        picked.push(rows[i].clone());

        // Remove anything related to this pivot
        for j in (i + 1)..n {
            if removed[j] {
                continue;
            }
            if periods_match(
                &rows[i],
                &rows[j],
                ptol_abs,
                dmtol,
                acctol,
                allow_harmonics,
                tobs_opt,
            ) {
                removed[j] = true;
            }
        }
    }

    picked
}

/// Read a CSV, detect schema, return (header, rows)
fn read_one_csv(path: &str) -> Result<(Vec<String>, Vec<RowView>)> {
    let file = File::open(path).with_context(|| format!("open {}", path))?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    let hdr = rdr
        .headers()
        .with_context(|| format!("read header of {}", path))?
        .clone();
    let colmap = detect_schema(&hdr).with_context(|| format!("detect schema in {}", path))?;

    let header_vec: Vec<String> = hdr.iter().map(|s| s.to_string()).collect();

    let mut out_rows = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        if let Some(view) = parse_row(&colmap, &rec, Path::new(path).file_name().unwrap_or_default().to_string_lossy().as_ref()) {
            out_rows.push(view);
        }
    }

    Ok((header_vec, out_rows))
}

/// Write rows with the header (plus optional source_col appended).
fn write_csv(output: &str, header: &[String], rows: &[RowView], source_col: Option<&str>) -> Result<()> {
    let mut wtr = Writer::from_path(output)
        .with_context(|| format!("create output {}", output))?;

    if let Some(sc) = source_col {
        // header + source_col
        let mut hdr_out = header.to_vec();
        hdr_out.push(sc.to_string());
        let hdr_ref: Vec<&str> = hdr_out.iter().map(|s| s.as_str()).collect();
        wtr.write_record(&hdr_ref)?;
        for r in rows {
            let mut row = r.row.clone();
            row.push(r.source.clone());
            wtr.write_record(row)?;
        }
    } else {
        let hdr_ref: Vec<&str> = header.iter().map(|s| s.as_str()).collect();
        wtr.write_record(&hdr_ref)?;
        for r in rows {
            wtr.write_record(&r.row)?;
        }
    }

    wtr.flush()?;
    Ok(())
}

/// Public entry called from the bin.
///
/// - `inputs`: one or more CSV paths
/// - `output`: output CSV
/// - `ptol_abs`: absolute period tolerance (seconds)
/// - `dmtol`: optional |ΔDM| gate
/// - `acctol`: optional |ΔACC| gate
/// - `allow_harmonics`: enable/disable harmonic matching
/// - `tobs_opt`: optional TOBS seconds for acceleration correction (default 600s if None)
/// - `source_col`: optional new column name to append with the source filename
pub fn cluster_csv_multi(
    inputs: &[String],
    output: &str,
    ptol_abs: f64,
    dmtol: Option<f64>,
    acctol: Option<f64>,
    allow_harmonics: bool,
    tobs_opt: Option<f64>,
    source_col: Option<&str>,
) -> Result<()> {
    if inputs.is_empty() {
        return Err(anyhow!("No input CSVs provided"));
    }

    println!(
        "[INFO] Reading {} input CSV(s)… (ptol={}, dmtol={:?}, acctol={:?}, harmonics={}, tobs={:?})",
        inputs.len(),
        ptol_abs,
        dmtol,
        acctol,
        allow_harmonics,
        tobs_opt
    );

    let mut all_rows: Vec<RowView> = Vec::new();
    let mut first_header: Option<Vec<String>> = None;

    for (k, p) in inputs.iter().enumerate() {
        let (hdr, mut rows) = read_one_csv(p)?;
        println!(
            "[INFO]  {}. {} → {} rows",
            k + 1,
            p,
            rows.len()
        );

        // Track the first header; if subsequent headers differ in content or length, we still proceed
        // but keep the first header for output. This guarantees stable output schema.
        if let Some(prev) = first_header.as_ref() {
            let same_len = prev.len() == hdr.len();
            let same_elems = same_len && prev.iter().zip(&hdr).all(|(a, b)| a == b);
            if !same_elems {
                eprintln!(
                    "[WARN] Header of {} differs from the first file; \
                     proceeding but output header will follow the first file.",
                    p
                );
            }
        } else {
            first_header = Some(hdr);
        }

        all_rows.append(&mut rows);
    }

    if all_rows.is_empty() {
        return Err(anyhow!("No valid rows parsed from inputs"));
    }

    println!(
        "[INFO] Total rows read: {}. Clustering…",
        all_rows.len()
    );

    let picked = cluster_rows(
        all_rows,
        ptol_abs,
        dmtol,
        acctol,
        allow_harmonics,
        tobs_opt,
    );

    let header = first_header.unwrap();
    write_csv(output, &header, &picked, source_col)?;

    println!(
        "[INFO] Clustering complete. Wrote {} picked rows to {}",
        picked.len(),
        output
    );
    Ok(())
}
