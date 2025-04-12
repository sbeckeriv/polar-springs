use clap::Parser;
use clap_derive::Parser;
use ploars_cli::runner::run;
use tracing::{error, info};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Path to the TOML configuration file
    #[clap(short, long)]
    config: String,

    /// Path to the input data stream (e.g., file path)
    #[clap(short, long)]
    input: String,
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    info!(
        "Starting the CLI with config: {} and input: {}",
        cli.config, cli.input
    );

    if let Err(e) = run(cli.config, cli.input) {
        error!("Application error: {}", e);
        std::process::exit(1);
    }
}
