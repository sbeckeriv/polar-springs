use polars_cli::config::Config;
use std::path::Path;
use std::sync::Once;

/// Parse a TOML config string into a Config struct, panicking with detailed error info on failure.
pub fn parse_config_str(s: &str) -> Config {
    let d = toml::Deserializer::new(s);
    match serde_path_to_error::deserialize::<_, Config>(d) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Failed to parse config: {e}\nPath: {}", e.path());
            panic!("Config parsing failed");
        }
    }
}

pub static START: Once = Once::new();
pub fn setup_test_logs() -> String {
    START.call_once(|| {
        tracing_subscriber::fmt::init();
    });
    let logs_path = Path::new("tests/request_logs.json");

    logs_path.to_str().unwrap().to_string()
}

#[macro_export]
macro_rules! config_string_test {
    ($test_name:ident, $config:expr) => {
        #[test]
        fn $test_name() {
            let config = $config;
            let input = test_utils::setup_test_logs();

            let result = polars_cli::runner::run(
                &test_utils::parse_config_str(config),
                &input,
                "jsonl",
                false,
            );
            assert!(
                result.is_ok(),
                "{} operation failed: {}",
                stringify!($test_name),
                result.err().unwrap()
            );
        }
    };
}
