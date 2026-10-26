# RustyCandyPicker

RustyCandyPicker is a fast Rust tool for filtering and clustering pulsar candidates from peasoup search XML files.

This is a rebuild of https://github.com/erc-compact/CandyPicker/tree/main by vivek 


---

## ✨ Features

- ⚡ **High performance** — written in Rust, faster than equivalent Python scripts  
- 🧩 **DM clustering (optional)** — collapse adjacent candidates in DM space  
- 📂 **Faithful XML handling** — preserves all fields, headers, and encodings from input files  
- 🔎 **Pivot filtering** — mark candidates as picked/rejected via pivot maps  
- 🖥️ **Singularity/Apptainer support** — reproducible builds for HPC environments  

---


## 🚀 Installation

Clone the repository:

```bash
git clone https://github.com/wintervolcano/rusty_candypicker.git
cd rusty_candypicker
```

### Build in release mode:

```bash
cargo build --release
```

The binary will be available at:
```bash
target/release/candy_picker_rs
```

### Singularity/Apptainer

A definition file is included for containerized builds. To build the image:
```bash
singularity build candy_picker_rs.sif candy_picker_rs.def
```

Then run inside the container:
```bash
singularity exec candy_picker_rs.sif candy_picker_rs -h
```

## Usage
Basic Usage:
```bash
candy_picker_rs [OPTIONS] -p <period_thresh> <xml_files>...

Options:
  -p <period_thresh>      default: 1e-6
  -d <dm_thresh>          
  -n <ncpus>              [default: 8]
  --bin-dm                optional to cluster candidates within dm_thresh
  -h, --help              Print help
  -V, --version           Print version
```

Example:
```bash
candy_picker_rs -p 1e-6 --threads 8 search_results.xml
```

This will produce:
- search_results_picked.xml
- search_results_rejected.xml

---

## csv_matcher_v2: cross-match candidates between searches

`csv_matcher_v2` finds candidates that appear in two or more candidate CSV files (for example two searches of the same cluster) and writes out which candidates match which.

Build it with:
```bash
cargo build --release --bin csv_matcher_v2
```

The binary will be at `target/release/csv_matcher_v2`.

### Usage
```bash
csv_matcher_v2 -i <csv_files>... --ptol-frac <tol> [OPTIONS]

Options:
  -i, --input <files>...    Candidate CSV files to compare (at least two)
  --ptol-frac <tol>         Fractional period tolerance |Pa-Pb| / mean(Pa,Pb), e.g. 1e-4
  --dmtol <tol>             Absolute DM tolerance (optional; DM ignored if not set)
  --acctol <tol>            Absolute acceleration tolerance (optional)
  --harmonics               Also match harmonics (k*P and P/k)
  --hmax <k>                Largest harmonic k with --harmonics [default: 8]
  --bary                    Match on barycentric (demodulated) periods instead of topocentric ones
  --site-lat <deg>          Telescope latitude for --bary / period_demod [default: -30.71292, MeerKAT]
  --site-lon <deg>          Telescope east longitude [default: 21.44380, MeerKAT]
  --extra-cols <cols>       Comma-separated extra columns to copy into the pairs table
  --out-suffix <suffix>     Suffix for the per-file outputs [default: _matched.csv]
  --pairs-only              Only write the pairs table, not the per-file outputs
  --pairs-out <file>        Pairs table [default: matched_pairs.csv]
```

Example:
```bash
csv_matcher_v2 -i M79_02_candidates.csv M79_03_seg2_candidates.csv --ptol-frac 1e-4 --dmtol 1 --pairs-out M79_matches.csv
```

Period is read from `p0_new`/`period`/`p0`/... or computed as 1/`f0_opt`; DM from `dm_new`/`dm_opt`/`dm`; acceleration from `acc_new`/`acc_opt`/`acc`; the plot from `png_path`/`png_file`/`png`/...; position from `ra`/`dec` (hh:mm:ss / dd:mm:ss or degrees); the epoch from `pepoch` (else the middle of the observation from `mjd_start` + `tobs`/2).

### Barycentric (demodulated) periods

Candidate periods from a search are topocentric, so Earth's orbital motion shifts the same pulsar's period by up to ~1e-4 between observations months apart. For every candidate with a position and an epoch, the matcher also computes the barycentric period `period_demod` = P (1 + v/c), where v is the velocity of the telescope towards the source (Earth's orbit plus its rotation at the telescope, MeerKAT by default) at the epoch. This agrees with astropy's barycentric correction to ~0.01 km/s, i.e. ~1e-7 in P.

A real pulsar keeps the same `period_demod` in every observation, so with `--bary` the match is made on barycentric periods and `--ptol-frac` can be much tighter (e.g. 1e-6 for an isolated pulsar; binaries still change by v_orbit/c).

This will produce:
- `<input>_matched.csv` for each input: only its matched rows, with all original columns plus `match_group`
- the pairs table (`--pairs-out`): one line per matched pair of candidates

### The pairs table

| Column | Meaning |
|---|---|
| `match_group` | Candidates linked by any chain of matches across files share a group number (A1 matches B1 and B1 matches A5: all three are in one group) |
| `file_a`, `file_b` | The candidate files the two candidates come from |
| `path_a`, `path_b` | Full paths of those files; relative `png_*` paths are relative to their directories |
| `row_a`, `row_b` | 1-based data row of the candidate in its file (header not counted) |
| `period_*`, `dm_*`, `acc_*`, `png_*` | Period (s), DM, acceleration and plot path of each candidate |
| `snr_*` | S/N, from `sn_fold` (falling back to `snr`, `sigma`, ... `sn_fft`); `NA` if the file has none |
| `pics_*_a`, `pics_*_b` | Every PICS score column found in the inputs; `NA` where a file lacks it |
| `<col>_a`, `<col>_b` | Any columns requested with `--extra-cols` |
| `match_type`, `harmonic_k` | `direct`, or `a=kx*b` / `b=kx*a` for harmonic matches, and k |
| `period_demod_*`, `tobs_*`, `epoch_mjd_*`, `ra_*`, `dec_*` | Barycentric period (s), observation length, epoch the period refers to, and position of each candidate |
| `period_frac_diff` | The value tested against `--ptol-frac` (harmonic-corrected; on barycentric periods with `--bary`) |
| `period_diff_s` | Raw abs(Pa-Pb) in seconds (topocentric, not harmonic-corrected) |
| `period_demod_frac_diff` | Fractional difference of the barycentric periods (harmonic-corrected); stays ~0 for a real pulsar |
| `dv_kms` | Line-of-sight velocity change implied by the period change, c (Pb - Pa) / Pa (harmonic-corrected), from the barycentric periods when available |
| `dv_frame` | `barycentric` or `topocentric`: which periods `dv_kms` comes from. A topocentric dV also contains the change in Earth's velocity towards the source (up to ~60 km/s between observations months apart) |
| `ang_sep_arcsec` | Angular distance between the two candidates' positions |

The pairs table can be viewed side by side in the CandyJar match viewer (https://github.com/wintervolcano/CandyJar). CandyJar includes this repository as a submodule and builds `csv_matcher_v2` itself, so `-m a.csv b.csv` there matches and opens the pairs in one step.
