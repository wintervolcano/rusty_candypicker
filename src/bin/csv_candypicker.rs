use anyhow::Result;
use clap::{Arg, Command};
// If you have a lib target (src/lib.rs with `pub mod csv_cluster;`)
use candy_picker_rs::csv_cluster::cluster_csv_multi;
// If you *don’t* have src/lib.rs, instead do:
// use crate::csv_cluster::cluster_csv_multi;

fn main() -> Result<()> {
    let matches = Command::new("csv_candypicker")
        .version("0.3.0")
        .about("Cluster CSV candidates by period (absolute tol), with optional DM/ACC gates, harmonics, and TOBS correction")
        .arg(Arg::new("inputs").short('i').long("input").num_args(1..).required(true)
             .help("One or more CSVs. Shell globs are expanded by your shell, e.g. -i fold*.csv"))
        .arg(Arg::new("output").short('o').long("output").required(true))
        .arg(Arg::new("ptol").long("ptol").required(true).help("Absolute period tolerance in seconds"))
        .arg(Arg::new("dmtol").long("dmtol").help("Optional |ΔDM| gate"))
        .arg(Arg::new("acctol").long("acctol").help("Optional |ΔACC| gate"))
        .arg(Arg::new("no_harmonics").long("no-harmonics").action(clap::ArgAction::SetTrue)
             .help("Disable harmonic matching"))
        .arg(Arg::new("tobs").long("tobs").help("Optional TOBS (s) for acceleration correction"))
        .arg(Arg::new("source_col").long("source-col").help("Append a column with the source filename"))
        .get_matches();

    let inputs: Vec<String> = matches
        .get_many::<String>("inputs").unwrap()
        .map(|s| s.to_string())
        .collect();

    let output = matches.get_one::<String>("output").unwrap();
    let ptol = matches.get_one::<String>("ptol").unwrap().parse::<f64>()?;
    let dmtol = matches.get_one::<String>("dmtol").and_then(|s| s.parse::<f64>().ok());
    let acctol = matches.get_one::<String>("acctol").and_then(|s| s.parse::<f64>().ok());
    let allow_harmonics = !matches.get_flag("no_harmonics");
    let tobs = matches.get_one::<String>("tobs").and_then(|s| s.parse::<f64>().ok());
    let source_col = matches.get_one::<String>("source_col").map(|s| s.as_str());

    cluster_csv_multi(
        &inputs,
        output,
        ptol,
        dmtol,
        acctol,
        allow_harmonics,
        tobs,
        source_col,
    )
}
