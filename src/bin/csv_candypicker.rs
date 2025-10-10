use anyhow::Result;
use clap::{Arg, ArgAction, Command};
use candy_picker_rs::csv_cluster::{cluster_csv, cluster_csv_multi};

fn main() -> Result<()> {
    let matches = Command::new("csv_candypicker")
        .version("0.3.0")
        .about("Cluster fold candidates in CSV(s) by absolute |ΔP| with optional DM/ACC gates and harmonics. Writes full rows for picked pivots.")
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .num_args(1..)
                .action(ArgAction::Append)
                .required(true)
                .help("One or more input CSV files"),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .num_args(1)
                .required(true)
                .help("Output picked CSV"),
        )
        .arg(
            Arg::new("ptol")
                .long("ptol")
                .num_args(1)
                .required(true)
                .help("ABSOLUTE period tolerance in seconds (|ΔP| ≤ ptol), e.g. 1e-7"),
        )
        .arg(
            Arg::new("dmtol")
                .long("dmtol")
                .num_args(1)
                .required(false)
                .help("Optional DM gate: if set, require |ΔDM| ≤ dmtol. If omitted, DM gate is disabled."),
        )
        .arg(
            Arg::new("acctol")
                .long("acctol")
                .num_args(1)
                .required(false)
                .help("Optional ACC gate: if set, require |ΔACC| ≤ acctol. If omitted, ACC gate is disabled."),
        )
        .arg(
            Arg::new("no_harmonics")
                .long("no-harmonics")
                .action(ArgAction::SetTrue)
                .help("Disable harmonic catch (modulo). Default is enabled."),
        )
        .arg(
            Arg::new("tobs")
                .long("tobs")
                .num_args(1)
                .required(false)
                .help("Observation length in seconds (for acceleration correction). Default 3600."),
        )
        .arg(
            Arg::new("source_col")
                .long("source-col")
                .num_args(1)
                .required(false)
                .help("Optional: add a column with this name containing the source filename"),
        )
        .get_matches();

    let inputs: Vec<String> = matches
        .get_many::<String>("input")
        .unwrap()
        .map(|s| s.to_string())
        .collect();
    let output = matches.get_one::<String>("output").unwrap();
    let ptol_abs: f64 = matches.get_one::<String>("ptol").unwrap().parse()?;
    let dm_tol: Option<f64> = matches.get_one::<String>("dmtol").and_then(|s| s.parse().ok());
    let acc_tol: Option<f64> = matches.get_one::<String>("acctol").and_then(|s| s.parse().ok());
    let allow_harmonics = !matches.get_flag("no_harmonics");
    let tobs_seconds: Option<f64> = matches.get_one::<String>("tobs").and_then(|s| s.parse().ok());
    let source_col: Option<&str> = matches.get_one::<String>("source_col").map(|s| s.as_str());

    if inputs.len() == 1 {
        cluster_csv(
            &inputs[0],
            output,
            ptol_abs,
            dm_tol,
            acc_tol,
            allow_harmonics,
            tobs_seconds,
        )
    } else {
        cluster_csv_multi(
            &inputs,
            output,
            ptol_abs,
            dm_tol,
            acc_tol,
            allow_harmonics,
            tobs_seconds,
            source_col,
        )
    }
}
