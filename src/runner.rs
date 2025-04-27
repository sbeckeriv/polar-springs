use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Read, Seek},
};

use crate::config::{self, Config};

use polars::prelude::*;
use polars_io::avro::AvroReader;
use serde::de::IntoDeserializer;
use tracing::{error, info};

/// Pure transformation logic for benchmarking and testing.
/// Applies config operations to a LazyFrame without logging or collect.
pub fn process_dataframe(
    mut df: LazyFrame,
    config: &Config,
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    for operation in &config.operations {
        match operation {
            config::Operation::Filter { .. } => {
                df = df.filter(operation.to_polars_expr()?);
            }
            config::Operation::Select { columns } => {
                let columns: Vec<_> = columns
                    .iter()
                    .map(|s: &String| col(s.to_string()))
                    .collect();
                df = df.select(columns);
            }
            config::Operation::GroupBy { columns, aggregate } => {
                let columns: Vec<_> = columns.iter().map(|s| col(s.to_string())).collect();
                let aggregate = aggregate
                    .iter()
                    .filter_map(|agg| agg.to_polars_expr().ok())
                    .collect::<Vec<_>>();
                df = df.group_by(columns).agg(aggregate);
            }
            config::Operation::Sort {
                column,
                order,
                limit,
            } => {
                let reverse = order.to_lowercase() == "desc";
                let sort_options = polars::prelude::SortMultipleOptions {
                    descending: [reverse].into(),
                    nulls_last: [true].into(),
                    limit: None,
                    maintain_order: true,
                    multithreaded: true,
                };
                df = df.sort([column], sort_options);
                if let Some(limit) = limit {
                    df = df.limit(*limit as u32);
                }
            }
            config::Operation::SelfJoin {
                left_on,
                right_on,
                how,
            } => {
                let left = left_on.iter().map(|s| col(s)).collect::<Vec<_>>();
                let right = right_on.iter().map(|s| col(s)).collect::<Vec<_>>();
                df = df.clone().join(df, left, right, JoinArgs::new(how.into()));
            }
            config::Operation::WithColumn { name, expression } => {
                fn operations(
                    df: LazyFrame,
                    config: &Config,
                ) -> Result<LazyFrame, Box<dyn std::error::Error>> {
                    info!("Applying operations from config");
                    let df = process_dataframe(df, config)?;
                    Ok(df)
                }
                let mut expression = expression.to_polars_expr()?;
                if let Some(name) = name {
                    expression = expression.alias(name);
                }
                df = df.with_column(expression);
            }
            config::Operation::Rename { mappings } => {
                let mut rename_old = Vec::new();
                let mut rename_new = Vec::new();
                for mapping in mappings {
                    rename_old.push(&mapping.old_name);
                    rename_new.push(&mapping.new_name);
                }
                df = df.rename(rename_old, rename_new, false);
            }
            config::Operation::Window { .. } => {
                df = df.lazy().with_column(operation.to_polars_expr()?);
            }
            config::Operation::GroupByTime {
                time_column,
                output_column,
                additional_groups,
                aggregate,
                ..
            } => {
                let time_bucket_col = output_column.clone().unwrap_or_else(|| time_column.clone());
                let truncate_expr = operation.to_polars_expr()?;
                let df_with_bucket = df.clone().lazy().with_column(truncate_expr).collect()?;
                let mut group_cols = vec![time_bucket_col.as_str()];
                group_cols.extend(additional_groups.iter().map(|s| s.as_str()));
                let agg_exprs = aggregate
                    .iter()
                    .flat_map(|agg| agg.to_polars_expr())
                    .collect::<Vec<_>>();
                df = df_with_bucket
                    .lazy()
                    .group_by(group_cols)
                    .agg(agg_exprs)
                    .collect()?
                    .lazy();
            }
        }
    }
    Ok(df)
}
pub fn dataframe_from_file(input_path: &str) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    info!("Processing input data from: {}", input_path);

    let file = std::fs::File::open(&input_path)?;
    // Load the input data into a Polars DataFrame
    let df = match input_path.to_lowercase().split(".").last() {
        Some("csv") => LazyCsvReader::new(input_path).finish()?.lazy(),
        Some("json") => {
            let sample_file = std::fs::File::open(&input_path)?;
            let mut reader = BufReader::new(sample_file);
            let mut buffer = [0u8; 1];
            let mut first_char = None;
            loop {
                reader.read_exact(&mut buffer)?;
                let c = buffer[0] as char;

                // Skip whitespace characters
                if !c.is_whitespace() {
                    first_char = Some(c);
                    break;
                }
            }

            reader.rewind()?;
            match first_char {
                Some('{') => {
                    let sample_data: Vec<String> =
                        reader.lines().take(10_000).filter_map(Result::ok).collect();

                    let pid = std::process::id();
                    let schema_sample = format!("schema_sample_{}.json", pid);
                    std::fs::write(&schema_sample, sample_data.join("\n"))?;

                    let df_sample = JsonLineReader::new(File::open(&schema_sample)?)
                        .infer_schema_len(Some(10_000.try_into().expect("always not 0")))
                        .finish()?;
                    fs::remove_file(schema_sample)?;

                    let schema = df_sample.schema();
                    LazyJsonLineReader::new(input_path)
                        .with_schema(Some(schema.clone()))
                        .finish()?
                        .lazy()
                }
                // can you rewrite the file to remove the [] and commas at the end of the line?
                // can rewrite logic to inlcude schema precheck here as well.
                Some('[') => JsonReader::new(reader).finish()?.lazy(),
                _ => {
                    error!("Unsupported JSON format. Expected either object or array.");
                    return Err("Unsupported JSON format".into());
                }
            }
        }
        Some("parquet") => ParquetReader::new(file).finish()?.lazy(),
        Some("ipc") => IpcReader::new(file).finish()?.lazy(),
        Some("avro") => AvroReader::new(file).finish()?.lazy(),
        _ => {
            error!(
                "Unsupported file format. Supported formats are: csv, json, parquet, ipc, avro."
            );
            return Err("Unsupported file format".into());
        }
    };
    Ok(df)
}
pub fn run(config: Config, input_path: String) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut df = dataframe_from_file(&input_path)?;
    df = operations(df, &config)?;

    let df = df.collect()?;
    info!("Final DataFrame:\n{}", df);

    Ok(df)
}

fn operations(df: LazyFrame, config: &Config) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    info!("Applying operations from config");
    let df = process_dataframe(df, config)?;
    Ok(df)
}
