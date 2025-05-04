use serde::Deserialize;

use super::schema::Schema;
#[derive(Deserialize, Debug)]
pub struct InputConfig {
    // can be a file or a cloud url
    pub location: String,
    #[serde(flatten)]
    pub format: InputFormat,
    //#[serde(default)]
    //pub schema: Option<Schema>,
}
impl InputConfig {
    pub fn new(location: &str, format: &str, is_cloud: bool, skip_sample: bool) -> Self {
        let input_format = match format {
            "csv" => InputFormat::Csv {
                is_cloud,
                delimiter: ",".to_string(),
                has_header: true,
                schema: None,
            },
            "json" => InputFormat::Json,
            "parquet" => InputFormat::Parquet,
            "ipc" => InputFormat::Ipc,
            "avro" => InputFormat::Avro,
            "jsonl" => InputFormat::JsonLines {
                is_cloud,
                skip_sample,
            },
            _ => panic!("Unsupported input format: {}", format),
        };
        Self {
            location: location.to_owned(),
            format: input_format,
        }
    }
}
fn default_delimiter() -> String {
    ",".to_string()
}

fn default_true() -> bool {
    true
}
#[derive(Deserialize, Debug)]
pub enum InputFormat {
    Csv {
        #[serde(default)]
        is_cloud: bool,
        #[serde(default = "default_delimiter")]
        delimiter: String,
        #[serde(default = "default_true")]
        has_header: bool,
        #[serde(default)]
        schema: Option<Schema>,
    },
    Json,
    Parquet,
    Ipc,
    Avro,
    JsonLines {
        #[serde(default)]
        is_cloud: bool,
        #[serde(default)]
        skip_sample: bool,
    },
}
