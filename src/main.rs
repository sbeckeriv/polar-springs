use clap::Parser;
use clap_derive::Parser;
use polars_cli::runner::{run, run_with_output};
use std::fs;
use toml::Deserializer;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Path to the TOML configuration file
    #[clap(short, long)]
    config: String,

    /// Only parse the config and exit if successful
    #[clap(long)]
    parse: bool,

    /// csv, jsonl (json per line), json (array of json), parquet, avro
    #[clap(long)]
    file_format: String,

    /// Path to the input data stream (e.g., file path)
    #[clap(short, long)]
    local_input: Option<String>,

    /// Cloud provider url (e.g., s3, gcs, azure)
    #[clap(long)]
    cloud_provider: Option<String>,
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    info!("Parsing TOML configuration from: {}", cli.config);

    // Pass cloud_options to runner::run or use as needed
    let config_content = match fs::read_to_string(&cli.config) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Failed to read config file: {}", e);
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
            eprintln!("Failed to parse TOML configuration: {}", e);
            std::process::exit(1);
        }
    }

    if cli.parse {
        std::process::exit(0);
    } else {
        if let Err(e) = run_with_output(
            config,
            cli.local_input
                .or_else(|| cli.cloud_provider.clone())
                .expect("Either local_input or cloud_provider must be provided"),
            cli.file_format,
            cli.cloud_provider.is_some(),
        ) {
            eprintln!("Application error: {:>}", e);
            std::process::exit(1);
        }
    }
}
