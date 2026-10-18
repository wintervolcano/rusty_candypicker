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
git clone https://github.com/yourusername/candy_picker_rs.git
cd candy_picker_rs
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
  --extra-cols <cols>       Comma-separated extra columns to copy into the pairs table
  --out-suffix <suffix>     Suffix for the per-file outputs [default: _matched.csv]
  --pairs-out <file>        Pairs table [default: matched_pairs.csv]
```

Example:
```bash
csv_matcher_v2 -i M79_02_candidates.csv M79_03_seg2_candidates.csv --ptol-frac 1e-4 --dmtol 1 --pairs-out M79_matches.csv
```

Period is read from `p0_new`/`period`/`p0`/... or computed as 1/`f0_opt`; DM from `dm_new`/`dm_opt`/`dm`; acceleration from `acc_new`/`acc_opt`/`acc`; the plot from `png_path`/`png_file`/`png`/...

This will produce:
- `<input>_matched.csv` for each input: only its matched rows, with all original columns plus `match_group`
- the pairs table (`--pairs-out`): one line per matched pair of candidates

### The pairs table

| Column | Meaning |
|---|---|
| `match_group` | Candidates linked by any chain of matches across files share a group number (A1 matches B1 and B1 matches A5: all three are in one group) |
| `file_a`, `file_b` | The candidate files the two candidates come from |
| `row_a`, `row_b` | 1-based data row of the candidate in its file (header not counted) |
| `period_*`, `dm_*`, `acc_*`, `png_*` | Period (s), DM, acceleration and plot path of each candidate |
| `snr_*` | S/N, from `sn_fold` (falling back to `snr`, `sigma`, ... `sn_fft`); `NA` if the file has none |
| `pics_*_a`, `pics_*_b` | Every PICS score column found in the inputs; `NA` where a file lacks it |
| `<col>_a`, `<col>_b` | Any columns requested with `--extra-cols` |
| `match_type`, `harmonic_k` | `direct`, or `a=kx*b` / `b=kx*a` for harmonic matches, and k |
| `period_frac_diff` | The value tested against `--ptol-frac` (harmonic-corrected) |
| `period_diff_s` | Raw abs(Pa-Pb) in seconds (not harmonic-corrected) |

The pairs table can be viewed side by side in the CandyJar match viewer (`-m`, see https://github.com/wintervolcano/CandyJar).
