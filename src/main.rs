use clap::Parser;
use clap_derive::Parser;
use polars_cli::runner::run;
use std::fs;
use toml::Deserializer;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Path to the TOML configuration file
    #[clap(short, long)]
    config: String,

    /// Path to the input data stream (e.g., file path)
    #[clap(short, long)]
    input: String,

    /// Only parse the config and exit if successful
    #[clap(long)]
    parse: bool,
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    info!("Parsing TOML configuration from: {}", cli.config);
    let config_content = match fs::read_to_string(&cli.config) {
        Ok(content) => content,
        Err(e) => {
            error!("Failed to read config file: {}", e);
            std::process::exit(1);
        }
    };
    let d = Deserializer::new(&config_content);
    let config;
    match serde_path_to_error::deserialize::<_, polars_cli::config::Config>(d) {
        Ok(config_content) => {
            config = config_content;
        }
        Err(e) => {
            error!("Failed to parse TOML configuration: {}", e);
            std::process::exit(1);
        }
    }

    if cli.parse {
        std::process::exit(0);
    } else {
        info!(
            "Starting the CLI with config: {} and input: {}",
            cli.config, cli.input
        );

        if let Err(e) = run(config, cli.input) {
            error!("Application error: {}", e);
            std::process::exit(1);
        }
    }
}
