use anyhow::Result;
use clap::{Arg, Command};
use candy_picker_rs::csv_cluster::cluster_csv;

fn main() -> Result<()> {
    let matches = Command::new("csv_candypicker")
        .version("0.3.0")
        .about("Cluster fold candidates in CSV by absolute |ΔP| with optional DM/ACC gates and harmonics.")
        .arg(Arg::new("input").short('i').long("input").num_args(1).required(true))
        .arg(Arg::new("output").short('o').long("output").num_args(1).required(true))
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
                .action(clap::ArgAction::SetTrue)
                .help("Disable harmonic catch (modulo-style). Default is ON (enabled)."),
        )
        .arg(
            Arg::new("tobs")
                .long("tobs")
                .num_args(1)
                .required(false)
                .help("Observation length in seconds (used for acceleration period correction). Default 600."),
        )
        .get_matches();

    let input = matches.get_one::<String>("input").unwrap();
    let output = matches.get_one::<String>("output").unwrap();

    // ABSOLUTE |ΔP| tolerance (seconds)
    let ptol_abs: f64 = matches.get_one::<String>("ptol").unwrap().parse()?;

    // Optional gates
    let dm_tol: Option<f64> = matches.get_one::<String>("dmtol").and_then(|s| s.parse().ok());
    let acc_tol: Option<f64> = matches.get_one::<String>("acctol").and_then(|s| s.parse().ok());

    // Harmonics toggle (default ON)
    let allow_harmonics = !matches.get_flag("no_harmonics");

    // Optional TOBS
    let tobs_seconds: Option<f64> = matches.get_one::<String>("tobs").and_then(|s| s.parse().ok());

    cluster_csv(
        input,
        output,
        ptol_abs,
        dm_tol,
        acc_tol,
        allow_harmonics,
        tobs_seconds,
    )
}
