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

Build in release mode:

```bash
cargo build --release
```

The binary will be available at:
```bash
target/release/candy_picker_rs
```

---
Singularity/Apptainer

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
```
Options:
  -p <period_thresh>      default: 1e-6
  -d <dm_thresh>          
  -n <ncpus>              [default: 8]
  --bin-dm                optional to cluster candidates within dm_thresh
  -h, --help              Print help
  -V, --version           Print version

Example:
```bash
candy_picker_rs -p 1e-6 --threads 8 search_results.xml
```

This will produce:
- search_results_picked.xml
- search_results_rejected.xml
