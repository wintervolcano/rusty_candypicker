use anyhow::Result;
use csv::{ReaderBuilder, WriterBuilder};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

const SPEED_OF_LIGHT: f64 = 299_792_458.0;

/// Minimal fields used for clustering (we still write full rows later).
#[derive(Debug, Clone, Deserialize)]
pub struct CsvCandidate {
    pub id: usize,          // we’ll fill this from "#id" or "id"
    pub dm_new: f64,
    pub p0_new: f64,
    pub acc_new: f64,
    pub snr_new: f64,       // "S/N_new"
}

/// Absolute period tolerance with optional DM/ACC gates and optional harmonics.
fn is_related_abs(
    a: &CsvCandidate,
    b: &CsvCandidate,
    ptol_abs: f64,                   // |ΔP| tolerance (seconds)
    dm_tol: Option<f64>,             // if Some, require |ΔDM| ≤ dm_tol
    acc_tol: Option<f64>,            // if Some, require |ΔACC| ≤ acc_tol
    tobs_over_c: f64,                // TOBS / c
    allow_harmonics: bool,           // C++-like modulo catch
) -> bool {
    if let Some(dm) = dm_tol {
        if (a.dm_new - b.dm_new).abs() > dm {
            return false;
        }
    }
    if let Some(at) = acc_tol {
        if (a.acc_new - b.acc_new).abs() > at {
            return false;
        }
    }

    // acceleration-corrected period for b relative to a
    let f0_b = 1.0 / b.p0_new;
    let corrected_b_period = 1.0 / (f0_b - (b.acc_new - a.acc_new) * f0_b * tobs_over_c);

    let delta_abs = (a.p0_new - corrected_b_period).abs();
    if delta_abs <= ptol_abs {
        return true;
    }

    if allow_harmonics {
        // modulo-style harmonic capture
        let (larger, smaller) = if a.p0_new >= corrected_b_period {
            (a.p0_new, corrected_b_period)
        } else {
            (corrected_b_period, a.p0_new)
        };
        let true_period_difference = larger % smaller;
        if true_period_difference <= ptol_abs {
            return true;
        }
    }

    false
}

/// Backward-compat single-file entry point (calls the multi-file version).
pub fn cluster_csv(
    input: &str,
    output: &str,
    ptol_abs: f64,
    dm_tol: Option<f64>,
    acc_tol: Option<f64>,
    allow_harmonics: bool,
    tobs_seconds: Option<f64>,
) -> Result<()> {
    cluster_csv_multi(
        &vec![input.to_string()],
        output,
        ptol_abs,
        dm_tol,
        acc_tol,
        allow_harmonics,
        tobs_seconds,
        None, // no source column by default
    )
}

/// Multi-file clustering: cluster across ALL inputs together and write full rows.
pub fn cluster_csv_multi(
    inputs: &[String],
    output: &str,
    ptol_abs: f64,
    dm_tol: Option<f64>,
    acc_tol: Option<f64>,
    allow_harmonics: bool,
    tobs_seconds: Option<f64>,
    source_col: Option<&str>, // optionally add a column with the source filename
) -> Result<()> {
    println!("[INFO] Reading {} CSV(s)", inputs.len());

    // Union of headers across all inputs (preserve order: first-seen wins).
    let mut headers_union: Vec<String> = Vec::new();
    let mut headers_seen: HashSet<String> = HashSet::new();

    // Full rows aligned with clustering candidates (only rows we can parse).
    let mut full_rows: Vec<HashMap<String, String>> = Vec::new();
    let mut cluster_records: Vec<CsvCandidate> = Vec::new();

    // Pass over all files
    for input in inputs {
        let mut rdr = ReaderBuilder::new().has_headers(true).from_path(input)?;

        // Update union headers
        let hdr = rdr.headers()?.clone();
        for h in hdr.iter() {
            if headers_seen.insert(h.to_string()) {
                headers_union.push(h.to_string());
            }
        }
        // If user asked to add a provenance column, ensure it's in the union.
        if let Some(col) = source_col {
            if headers_seen.insert(col.to_string()) {
                headers_union.push(col.to_string());
            }
        }

        // Find required columns (support both "#id" and "id", and "S/N_new")
        let idx_id = hdr
            .iter()
            .position(|s| s == "#id" || s == "id")
            .ok_or_else(|| anyhow::anyhow!(format!("{input}: missing '#id' or 'id' column")))?;
        let idx_dm = hdr
            .iter()
            .position(|s| s == "dm_new")
            .ok_or_else(|| anyhow::anyhow!(format!("{input}: missing 'dm_new' column")))?;
        let idx_p0 = hdr
            .iter()
            .position(|s| s == "p0_new")
            .ok_or_else(|| anyhow::anyhow!(format!("{input}: missing 'p0_new' column")))?;
        let idx_acc = hdr
            .iter()
            .position(|s| s == "acc_new")
            .ok_or_else(|| anyhow::anyhow!(format!("{input}: missing 'acc_new' column")))?;
        let idx_snr = hdr
            .iter()
            .position(|s| s == "S/N_new")
            .ok_or_else(|| anyhow::anyhow!(format!("{input}: missing 'S/N_new' column")))?;

        for rec in rdr.records() {
            let rec = rec?;

            // Parse needed fields; if any parse fails, skip the row.
            let id_str = rec.get(idx_id).unwrap_or_default();
            let dm_str = rec.get(idx_dm).unwrap_or_default();
            let p0_str = rec.get(idx_p0).unwrap_or_default();
            let ac_str = rec.get(idx_acc).unwrap_or_default();
            let sn_str = rec.get(idx_snr).unwrap_or_default();

            let (id, dm, p0, ac, sn) = match (id_str.parse::<usize>(),
                                               dm_str.parse::<f64>(),
                                               p0_str.parse::<f64>(),
                                               ac_str.parse::<f64>(),
                                               sn_str.parse::<f64>()) {
                (Ok(id), Ok(dm), Ok(p0), Ok(ac), Ok(sn)) => (id, dm, p0, ac, sn),
                _ => {
                    // silently skip malformed row
                    continue;
                }
            };

            // Build the full-row map for output (with current file’s headers)
            let mut map: HashMap<String, String> = HashMap::with_capacity(headers_union.len());
            for (h, v) in hdr.iter().zip(rec.iter()) {
                map.insert(h.to_string(), v.to_string());
            }
            if let Some(col) = source_col {
                map.insert(col.to_string(), input.to_string());
            }

            // Push only if we can cluster it
            full_rows.push(map);
            cluster_records.push(CsvCandidate {
                id,
                dm_new: dm,
                p0_new: p0,
                acc_new: ac,
                snr_new: sn,
            });
        }
    }

    if cluster_records.is_empty() {
        println!("[WARN] No candidates parsed from inputs");
        return Ok(());
    }

    // TOBS/c factor
    let tobs = tobs_seconds.unwrap_or(3600.0);
    let tobs_over_c = tobs / SPEED_OF_LIGHT;

    // Greedy clustering over all inputs; pick highest S/N as pivot
    let n = cluster_records.len();
    let mut clustered = vec![false; n];
    let mut picked_idx = Vec::new();

    for i in 0..n {
        if clustered[i] { continue; }
        let mut members = vec![i];
        clustered[i] = true;

        for j in (i + 1)..n {
            if clustered[j] { continue; }
            if is_related_abs(
                &cluster_records[i],
                &cluster_records[j],
                ptol_abs,
                dm_tol,
                acc_tol,
                tobs_over_c,
                allow_harmonics,
            ) {
                clustered[j] = true;
                members.push(j);
            }
        }

        let pivot = *members
            .iter()
            .max_by(|&&a, &&b| {
                cluster_records[a]
                    .snr_new
                    .partial_cmp(&cluster_records[b].snr_new)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap();
        picked_idx.push(pivot);
    }

    // Write the union headers and full rows for pivots
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(output)?;
    wtr.write_record(headers_union.iter())?;
    for &i in &picked_idx {
        let row = &full_rows[i];
        let vals: Vec<&str> = headers_union
            .iter()
            .map(|h| row.get(h).map(|s| s.as_str()).unwrap_or(""))
            .collect();
        wtr.write_record(vals)?;
    }
    wtr.flush()?;

    println!(
        "[INFO] Clustering complete across {} file(s). Wrote {} picked candidates to {}",
        inputs.len(),
        picked_idx.len(),
        output
    );
    Ok(())
}
