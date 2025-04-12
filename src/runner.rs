use crate::config::{self, Config};

use polars::prelude::*;
use polars_io::avro::AvroReader;

use std::fs;
use tracing::{error, info};

pub fn run(
    config_path: String,
    input_path: String,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Placeholder for TOML parsing and Polars logic
    info!("Parsing TOML configuration from: {}", config_path);
    info!("Processing input data from: {}", input_path);

    // Parse the TOML configuration file
    let config_content = fs::read_to_string(&config_path)?;
    let config: Config = toml::from_str(&config_content)?;
    info!("Parsed configuration: {:?}", config);
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
                info!("Sorting by column '{}' in '{}' order", column, order);
                let reverse = order.to_lowercase() == "desc";
                let sort_options = polars::prelude::SortMultipleOptions {
                    descending: [reverse].into(),
                    nulls_last: [false].into(),
                    limit: *limit,
                    maintain_order: true,
                    multithreaded: true,
                };
                df = df.sort([column], sort_options);
            }
            config::Operation::GroupByDynamic { columns, aggregate } => todo!(),
            config::Operation::Join {
                right_df,
                left_on,
                right_on,
                how,
            } => todo!(),
            config::Operation::WithColumn { name, expression } => {
                df = df.with_column(expression.to_polars_expr()?);
            }
            config::Operation::Pivot {
                index,
                columns,
                values,
                aggregate_function,
            } => {
                // how do i use pivot?
            }
            config::Operation::Rename { mappings } => todo!(),
            config::Operation::PivotAdvanced { index, values, .. } => {
                // Create a dictionary of value column -> aggregation function
                let mut agg_exprs: Vec<Expr> = Vec::new();
                for agg in values {
                    agg_exprs.push(agg.to_polars_expr()?);
                }

                // Build the pivot operation
                let pivot_expr = df
                    .clone()
                    .lazy()
                    .group_by(index.iter().map(col).collect::<Vec<_>>())
                    .agg(agg_exprs)
                    .collect()?;

                df = pivot_expr.lazy();
            }
            config::Operation::Window { .. } => {
                df = df.lazy().with_column(operation.to_polars_expr()?);
            }
            config::Operation::GroupByTime {
                output_column,
                additional_groups,
                aggregate,
                ..
            } => {
                let time_bucket_col = output_column.as_deref().unwrap_or("time_bucket");
                let truncate_expr = operation.to_polars_expr()?;
                // Create a DataFrame with the time bucket
                let df_with_bucket = df.clone().lazy().with_column(truncate_expr).collect()?;

                // Build the group by columns (time bucket + additional groups)
                let mut group_cols = vec![time_bucket_col];
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
