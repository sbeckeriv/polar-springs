use crate::config::{self, Config};

use polars::prelude::*;
use polars_io::avro::AvroReader;
use tracing::{error, info};

pub fn run(config: Config, input_path: String) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Placeholder for TOML parsing and Polars logic
    info!("Processing input data from: {}", input_path);

    let file = std::fs::File::open(&input_path)?;
    // Load the input data into a Polars DataFrame
    let mut df = match input_path.to_lowercase().split(".").last() {
        Some("csv") => CsvReader::new(file).finish()?.lazy(),
        Some("json") => JsonLineReader::new(file).finish()?.lazy(),
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
    df = operations(df, &config)?;

    let df = df.collect()?;
    info!("Final DataFrame:\n{}", df);

    Ok(df)
}

fn operations(df: LazyFrame, config: &Config) -> Result<LazyFrame, Box<dyn std::error::Error>> {
    let mut df = df;
    // Apply operations from the configuration
    for operation in &config.operations {
        match operation {
            config::Operation::Filter { .. } => {
                df = df.filter(operation.to_polars_expr()?);
            }
            config::Operation::Select { columns } => {
                info!("Selecting columns: {:?}", columns);
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
                info!(
                    "Sorting by column '{}' in '{}' order {:?}",
                    column, order, limit
                );
                let reverse = order.to_lowercase() == "desc";
                let sort_options = polars::prelude::SortMultipleOptions {
                    descending: [reverse].into(),
                    nulls_last: [true].into(),
                    limit: None, // limit here is a panic if set. apply limit after.
                    maintain_order: true,
                    multithreaded: true,
                };

                info!(
                    "Sorting by column '{}' in '{}' order {:?}  {:?}",
                    column, order, limit, sort_options
                );
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
                // Create a DataFrame with the time bucket
                let df_with_bucket = df.clone().lazy().with_column(truncate_expr).collect()?;

                // Build the group by columns (time bucket + additional groups)
                let mut group_cols = vec![time_bucket_col.as_str()];
                group_cols.extend(additional_groups.iter().map(|s| s.as_str()));

                // Prepare the aggregation expressions
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
