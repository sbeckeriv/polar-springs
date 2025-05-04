use clap::Parser;
use clap_derive::Parser;
use polars_cli::{
    configs::input::InputConfig, configs::parse::parse_config_file, runner::run_with_output,
};
use tracing::info;

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
    file_format: Option<String>,

    /// Path to the input data stream (e.g., file path)
    #[clap(short, long)]
    local_input: Option<String>,

    /// Cloud provider url (e.g., s3, gcs, azure)
    #[clap(long)]
    cloud_provider: Option<String>,
}

fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    info!("Parsing TOML configuration from: {}", cli.config);
    let mut config = parse_config_file(&cli.config);

    if cli.parse {
        std::process::exit(0);
    } else {
        if config.input.is_none() {
            let input_path = cli
                .local_input
                .or_else(|| cli.cloud_provider.clone())
                .expect("Input path is required if not present in the config file.");

            let file_format = cli
                .file_format
                .expect(
                    "File format is required unless you have an input config in the config file.",
                )
                .to_lowercase();
            config.input = Some(InputConfig::new(
                &input_path,
                &file_format,
                cli.cloud_provider.is_some(),
                false,
            ));
        }
        if let Err(e) = run_with_output(config) {
            eprintln!("Application error: {:>}", e);
            std::process::exit(1);
        }
    }
}
