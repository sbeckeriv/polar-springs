use serde::Deserialize;
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum OutputConfig {
    File(FileOutputConfig),
    Stdout(FormatOutputConfig),
    Stderr(FormatOutputConfig),
    Database(DatabaseOutputConfig),
    Cloud(CloudOutputConfig),
}

#[derive(Deserialize, Debug, Clone)]
pub enum OutputFormats {
    Csv,
    Parquet,
    Json,
    Jsonl,
    Avro,
    Icp {
        #[serde(default)]
        compression: Option<IcpCompressionConfig>,
    },
}
#[derive(Deserialize, Debug, Clone)]
pub enum IcpCompressionConfig {
    Lz4,
    Zstd,
}
impl Into<polars::prelude::IpcCompression> for &IcpCompressionConfig {
    fn into(self) -> polars::prelude::IpcCompression {
        match self {
            IcpCompressionConfig::Lz4 => polars::prelude::IpcCompression::LZ4,
            IcpCompressionConfig::Zstd => polars::prelude::IpcCompression::ZSTD,
        }
    }
}
#[derive(Deserialize, Debug, Clone)]
pub struct FormatOutputConfig {
    pub format: OutputFormats,
}

#[derive(Deserialize, Debug, Clone)]
pub struct FileOutputConfig {
    pub format: OutputFormats,
    pub path: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseOutputConfig {
    pub format: OutputFormats,
    pub uri: String,
    pub table: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CloudOutputConfig {
    pub format: OutputFormats,
    pub provider: String, // e.g. "aws_s3"
    pub bucket: String,
    pub key: String,
}
