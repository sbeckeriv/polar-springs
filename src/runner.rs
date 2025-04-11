use crate::{
    config::{self, AllowedGroupFunction, Config, FilterField},
    Cli,
};

use polars::{lazy::frame, prelude::*};
use polars_io::avro::AvroReader;

use std::fs;
use tracing::{error, info};

pub fn run(cli: Cli) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Placeholder for TOML parsing and Polars logic
    info!("Parsing TOML configuration from: {}", cli.config);
    info!("Processing input data from: {}", cli.input);

    // Parse the TOML configuration file
    let config_content = fs::read_to_string(&cli.config)?;
    let config: Config = toml::from_str(&config_content)?;
    info!("Parsed configuration: {:?}", config);
    let file = std::fs::File::open(&cli.input)?;
    // Load the input data into a Polars DataFrame
    let mut df = match cli.input.to_lowercase().split(".").last() {
        Some("csv") => CsvReader::new(file).finish()?.lazy(),
        Some("json") => JsonLineReader::new(file).finish()?.lazy(),
        Some("parquet") => ParquetReader::new(std::fs::File::open(&cli.input)?)
            .finish()?
            .lazy(),
        Some("ipc") => IpcReader::new(std::fs::File::open(&cli.input)?)
            .finish()?
            .lazy(),
        Some("avro") => AvroReader::new(std::fs::File::open(&cli.input)?)
            .finish()?
            .lazy(),
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
                    maintain_order: false,
                    multithreaded: true,
                };
                df = df.sort([&*column], sort_options);
            }
            config::Operation::GroupByDynamic { columns, aggregate } => todo!(),
            config::Operation::GroupByTime {
                time_column,
                every,
                unit,
                aggregate,
            } => todo!(),
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
            config::Operation::PivotAdvanced {
                index,
                columns,
                values,
            } => {
                // Create a dictionary of value column -> aggregation function
                let mut agg_exprs: Vec<Expr> = Vec::new();
                for agg in values {
                    agg_exprs.push(agg.to_polars_expr()?);
                }

                // Build the pivot operation
                let pivot_expr = df
                    .clone()
                    .lazy()
                    .group_by(index.iter().map(|s| col(s)).collect::<Vec<_>>())
                    .agg(agg_exprs)
                    .collect()?;

                df = pivot_expr.lazy();
            }
            config::Operation::Window { .. } => {
                df = df.lazy().with_column(operation.to_polars_expr()?);
            }
        }
    }
    Ok(df)
}
