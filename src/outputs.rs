use polars::prelude::{
    CsvWriter, CsvWriterOptions, DataFrame, LazyFrame, ParquetCompression, ParquetWriteOptions,
    ParquetWriter, SerializeOptions,
};
use polars_io::{
    avro::AvroWriter,
    ipc::{IpcStreamWriter, IpcWriterOptions},
    json::{JsonFormat, JsonWriter, JsonWriterOptions},
    SerWriter,
};
use std::{
    fs::File,
    io::{self, Write},
    path::{self, Path},
};

use crate::configs::output::{
    CloudOutputConfig, DatabaseOutputConfig, FileOutputConfig, FormatOutputConfig, OutputConfig,
    OutputFormats,
};

#[derive(Debug)]
pub enum OutputError {
    Io(String),
    Config(String),
    Other(String),
}
impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputError::Io(e) => write!(f, "IO error: {}", e),
            OutputError::Config(e) => write!(f, "Config error: {}", e),
            OutputError::Other(e) => write!(f, "Other error: {}", e),
        }
    }
}
impl std::error::Error for OutputError {}
impl From<std::io::Error> for OutputError {
    fn from(e: std::io::Error) -> Self {
        OutputError::Io(e.to_string())
    }
}
impl From<String> for OutputError {
    fn from(e: String) -> Self {
        OutputError::Other(e)
    }
}

pub trait OutputConnector {
    fn format(&self) -> OutputFormats;
    fn write(&self, df: LazyFrame) -> Result<(), OutputError> {
        let mut df = df
            .collect()
            .map_err(|e| OutputError::Io(format!("Failed to collect DataFrame: {}", e)))?;
        let mut file = self.file();
        match &self.format() {
            OutputFormats::Csv => {
                CsvWriter::new(&mut file)
                    .include_header(true)
                    .with_separator(b',')
                    .finish(&mut df)
                    .map_err(|e| OutputError::Io(format!("Failed to write CSV: {}", e)))?;
            }
            OutputFormats::Json => {
                JsonWriter::new(&mut file)
                    .with_json_format(JsonFormat::Json)
                    .finish(&mut df)
                    .map_err(|e| OutputError::Io(format!("Failed to write JSON: {}", e)))?;
            }
            OutputFormats::Jsonl => {
                JsonWriter::new(&mut file)
                    .with_json_format(JsonFormat::JsonLines)
                    .finish(&mut df)
                    .map_err(|e| OutputError::Io(format!("Failed to write JSONLine: {}", e)))?;
            }
            OutputFormats::Parquet => {
                ParquetWriter::new(&mut file)
                    .finish(&mut df)
                    .map_err(|e| OutputError::Io(format!("Failed to write Parquet: {}", e)))?;
            }
            OutputFormats::Avro => {
                AvroWriter::new(&mut file)
                    .finish(&mut df)
                    .map_err(|e| OutputError::Io(format!("Failed to write Arrow: {}", e)))?;
            }
            OutputFormats::Icp { compression } => {
                let compression = compression.as_ref().map(Into::into);
                IpcStreamWriter::new(&mut file)
                    .with_compression(compression)
                    .finish(&mut df)
                    .map_err(|e| OutputError::Io(format!("Failed to write ICP: {}", e)))?;
            }
        }
        Ok(())
    }
    fn file(&self) -> Box<dyn Write>;
}

impl TryFrom<&OutputConfig> for Box<dyn OutputConnector> {
    type Error = OutputError;

    fn try_from(config: &OutputConfig) -> Result<Self, Self::Error> {
        match config {
            OutputConfig::File(file_cfg) => Ok(Box::new(FileOutput {
                config: file_cfg.clone(),
            })),
            OutputConfig::Database(db_cfg) => Ok(Box::new(DatabaseOutput {
                config: db_cfg.clone(),
            })),
            OutputConfig::Cloud(cloud_cfg) => Ok(Box::new(CloudOutput {
                config: cloud_cfg.clone(),
            })),
            OutputConfig::Stdout(config) => Ok(Box::new(Stdout {
                config: config.clone(),
            })),
            OutputConfig::Stderr(config) => Ok(Box::new(Stdout {
                config: config.clone(),
            })),
        }
    }
}
pub struct Stderr {
    config: FormatOutputConfig,
}
impl OutputConnector for Stderr {
    fn file(&self) -> Box<dyn Write> {
        Box::new(io::stderr())
    }

    fn format(&self) -> OutputFormats {
        self.config.format.clone()
    }
}

pub struct Stdout {
    config: FormatOutputConfig,
}
impl OutputConnector for Stdout {
    fn file(&self) -> Box<dyn Write> {
        Box::new(io::stdout())
    }

    fn format(&self) -> OutputFormats {
        self.config.format.clone()
    }
}
pub struct FileOutput {
    pub config: FileOutputConfig,
}
impl OutputConnector for FileOutput {
    fn file(&self) -> Box<dyn Write> {
        Box::new(File::create(self.config.path.clone()).expect("could not create file"))
    }

    fn write(&self, df: LazyFrame) -> Result<(), OutputError> {
        let mut df = df;
        let mut file = self.file();
        let path = Path::new(&self.config.path);
        match &self.format() {
            OutputFormats::Csv => {
                LazyFrame::sink_csv(
                    df,
                    path,
                    CsvWriterOptions {
                        include_header: true,
                        maintain_order: true,
                        serialize_options: SerializeOptions {
                            separator: b',',
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    None,
                )
                .map_err(|e| OutputError::Io(format!("Failed to write CSV sink: {}", e)))?;
            }
            OutputFormats::Json => {
                LazyFrame::sink_json(
                    df,
                    path,
                    JsonWriterOptions {
                        maintain_order: true,
                    },
                    None,
                )
                .map_err(|e| OutputError::Io(format!("Failed to write Json sink: {}", e)))?;
            }
            OutputFormats::Jsonl => {
                let mut df = df.collect().map_err(|e| {
                    OutputError::Io(format!("Failed to collect DataFrame for JSONL: {}", e))
                })?;
                JsonWriter::new(&mut file)
                    .with_json_format(JsonFormat::JsonLines)
                    .finish(&mut df)
                    .map_err(|e| OutputError::Io(format!("Failed to write JSONLine: {}", e)))?;
            }
            OutputFormats::Parquet => {
                LazyFrame::sink_parquet(df, &path, ParquetWriteOptions::default(), None)
                    .map_err(|e| OutputError::Io(format!("Failed to write Parquet sink: {}", e)))?;
            }
            OutputFormats::Avro => {
                let mut df = df.collect().map_err(|e| {
                    OutputError::Io(format!("Failed to collect DataFrame for Avro: {}", e))
                })?;
                AvroWriter::new(&mut file)
                    .finish(&mut df)
                    .map_err(|e| OutputError::Io(format!("Failed to write Arrow: {}", e)))?;
            }
            OutputFormats::Icp { compression } => {
                LazyFrame::sink_ipc(
                    df,
                    path,
                    IpcWriterOptions {
                        compression: compression.as_ref().map(Into::into),
                        ..Default::default()
                    },
                    None,
                )
                .map_err(|e| OutputError::Io(format!("Failed to write ICP sink: {}", e)))?;
            }
        }
        Ok(())
    }
    fn format(&self) -> OutputFormats {
        self.config.format.clone()
    }
}

pub struct DatabaseOutput {
    pub config: DatabaseOutputConfig,
}
impl OutputConnector for DatabaseOutput {
    fn write(&self, _df: LazyFrame) -> Result<(), OutputError> {
        // Implement database writing logic here
        Ok(())
    }
    fn file(&self) -> Box<dyn Write> {
        todo!()
    }

    fn format(&self) -> OutputFormats {
        todo!()
    }
}

pub struct CloudOutput {
    pub config: CloudOutputConfig,
}
impl OutputConnector for CloudOutput {
    fn write(&self, _df: LazyFrame) -> Result<(), OutputError> {
        //CloudWriter
        // Implement cloud writing logic here
        Ok(())
    }
    fn file(&self) -> Box<dyn Write> {
        todo!()
    }

    fn format(&self) -> OutputFormats {
        todo!()
    }
}
