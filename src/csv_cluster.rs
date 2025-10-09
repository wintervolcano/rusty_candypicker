use anyhow::Result;
use serde::Deserialize;
use csv::{ReaderBuilder, WriterBuilder};
use std::collections::HashMap;

const SPEED_OF_LIGHT: f64 = 299_792_458.0;

/// Minimal fields used for clustering; we still write full rows later.
#[derive(Debug, Clone, Deserialize)]
pub struct CsvCandidate {
    #[serde(rename = "#id")]
    pub id: usize,
    pub dm_new: f64,
    pub p0_new: f64,
    pub acc_new: f64,
    #[serde(rename = "S/N_new")]
    pub snr_new: f64,
}

/// C++-style absolute period tolerance with optional DM/ACC gates and optional harmonics.
/// - Period correction by acceleration difference is always applied (like the C++ code).
fn is_related_abs(
    a: &CsvCandidate,
    b: &CsvCandidate,
    ptol_abs: f64,                   // |ΔP| tolerance (seconds)
    dm_tol: Option<f64>,             // if Some, require |ΔDM| ≤ dm_tol; if None, ignore DM
    acc_tol: Option<f64>,            // if Some, require |ΔACC| ≤ acc_tol; if None, ignore ACC
    tobs_over_c: f64,                // TOBS / c (seconds / m/s)
    allow_harmonics: bool,           // true to use modulo-style harmonic catch (C++ behavior)
) -> bool {
    if let Some(dm) = dm_tol {
        if (a.dm_new - b.dm_new).abs() > dm {
            return false;
        }
    }
    if let Some(acc) = acc_tol {
        if (a.acc_new - b.acc_new).abs() > acc {
            return false;
        }
    }

    // Acceleration-corrected period for b relative to a (same as C++ logic)
    let f0_b = 1.0 / b.p0_new;
    let corrected_b_period = 1.0 / (f0_b - (b.acc_new - a.acc_new) * f0_b * tobs_over_c);

    let delta_abs = (a.p0_new - corrected_b_period).abs();
    if delta_abs <= ptol_abs {
        return true;
    }

    if allow_harmonics {
        // C++-style modulo check to catch near multiples.
        // true_period_difference = fmod(larger, smaller)
        let (larger, smaller) = if a.p0_new >= corrected_b_period {
            (a.p0_new, corrected_b_period)
        } else {
            (corrected_b_period, a.p0_new)
        };
        // Safe because periods are > 0
        let true_period_difference = larger % smaller;
        if true_period_difference <= ptol_abs {
            return true;
        }
    }

    false
}

/// Cluster the CSV candidates and output the **full input rows** for the pivot of each cluster.
pub fn cluster_csv(
    input: &str,
    output: &str,
    ptol_abs: f64,                // absolute |ΔP| tolerance (seconds)
    dm_tol: Option<f64>,          // None => ignore DM gate
    acc_tol: Option<f64>,         // None => ignore ACC gate
    allow_harmonics: bool,        // true => modulo-based harmonic catch enabled
    tobs_seconds: Option<f64>,    // None => default to 600 s
) -> Result<()> {
    println!("[INFO] Reading CSV: {input}");

    // First pass: parse clustering subset.
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path(input)?;
    let cluster_records: Vec<CsvCandidate> = rdr.deserialize().collect::<Result<_, _>>()?;

    if cluster_records.is_empty() {
        println!("[WARN] No candidates found in {input}");
        return Ok(());
    }

    // Second pass: capture full rows/headers so we can write everything for pivots.
    let mut rdr2 = ReaderBuilder::new().has_headers(true).from_path(input)?;
    let headers = rdr2.headers()?.clone();
    let mut full_rows: Vec<HashMap<String, String>> = Vec::with_capacity(cluster_records.len());
    for rec in rdr2.records() {
        let rec = rec?;
        let mut map = HashMap::with_capacity(headers.len());
        for (h, v) in headers.iter().zip(rec.iter()) {
            map.insert(h.to_string(), v.to_string());
        }
        full_rows.push(map);
    }

    // TOBS/c factor (if not provided, default 600 s / c)
    let tobs = tobs_seconds.unwrap_or(600.0);
    let tobs_over_c = tobs / SPEED_OF_LIGHT;

    // Cluster: greedy, earliest-as-seed; pivot picked by max S/N
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

        // pivot = highest S/N_new in the cluster
        let pivot = *members.iter()
            .max_by(|&&a, &&b| cluster_records[a].snr_new
                .partial_cmp(&cluster_records[b].snr_new)
                .unwrap_or(std::cmp::Ordering::Equal))
            .unwrap();
        picked_idx.push(pivot);
    }

    // Write full rows for pivots
    let mut wtr = WriterBuilder::new().has_headers(true).from_path(output)?;
    wtr.write_record(headers.iter())?;
    for &i in &picked_idx {
        let row = &full_rows[i];
        let vals: Vec<&str> = headers.iter()
            .map(|h| row.get(h).map(|s| s.as_str()).unwrap_or(""))
            .collect();
        wtr.write_record(vals)?;
    }
    wtr.flush()?;

    println!("[INFO] Clustering complete. Wrote {} picked candidates to {}", picked_idx.len(), output);
    Ok(())
}
