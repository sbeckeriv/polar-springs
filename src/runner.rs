use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
};

use crate::config::{self, Config};

use polars::prelude::*;
use polars_io::avro::AvroReader;
use tracing::{error, info};

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
pub fn dataframe_from_file(
    input_path: &str,
    file_format: &str,
    is_cloud: bool,
) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    info!("Processing input data from: {}", input_path);

    let file = std::fs::File::open(&input_path)?;
    // Load the input data into a Polars DataFrame
    let df = match (file_format, is_cloud) {
        ("csv", _) => LazyCsvReader::new(input_path).finish()?.lazy(),
        ("jsonl", false) => {
            let sample_file = std::fs::File::open(&input_path)?;
            let reader = BufReader::new(sample_file);
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
        ("jsonl", true) => LazyJsonLineReader::new(input_path).finish()?.lazy(),
        ("json", _) => JsonReader::new(file).finish()?.lazy(),
        ("parquet", false) => LazyFrame::scan_parquet(input_path, Default::default())?,
        ("ipc", false) => LazyFrame::scan_ipc(input_path, Default::default())?,
        ("avro", false) => AvroReader::new(file).finish()?.lazy(),
        _ => {
            error!(
                "Unsupported file format. Supported formats are: csv, json, parquet, ipc, avro."
            );
            return Err(format!(
                "Unsupported file format and cloud {file_format} in the cloud {is_cloud}"
            )
            .into());
        }
    };
    Ok(df)
}

pub fn run(
    config: Config,
    input_path: String,
    file_format: String,
    is_cloud: bool,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut df = dataframe_from_file(&input_path, &file_format, is_cloud)?;
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
