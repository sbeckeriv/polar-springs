use std::fs;
use toml::Deserializer;

use crate::config::Config;
pub fn parse_config_file(config_path: &str) -> Config {
    let config_content = match fs::read_to_string(&config_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Failed to read config file: {}", e);
            std::process::exit(1);
        }
    };
    parse_config(&config_content)
}

pub fn parse_config(config_content: &str) -> Config {
    let d = Deserializer::new(&config_content);
    let config = match serde_path_to_error::deserialize::<_, Config>(d) {
        Ok(config_content) => config_content,
        Err(e) => {
            eprintln!("Failed to parse TOML configuration: {}", e);
            std::process::exit(1);
        }
    };
    config
}
