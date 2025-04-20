use polars::prelude::{col, lit, when, DataType, Expr, RollingOptionsFixedWindow, NULL};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub operations: Vec<Operation>,
}

#[derive(Deserialize, Debug)]
pub enum AllowedFilterCondition {
    EQ,
    EQMISSING,
    NEQ,
    LT,
    LTE,
    GT,
    GTE,
    ISNULL,
    ISNOTNULL,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Operation {
    Filter {
        column: String,
        condition: AllowedFilterCondition,
        #[serde(default)]
        filter: Option<FilterField>,
    },
    Select {
        columns: Vec<String>,
    },
    GroupBy {
        columns: Vec<String>,
        aggregate: Vec<Aggregate>,
    },

    GroupByDynamic {
        columns: Vec<String>,
        aggregate: Vec<Aggregate>,
    },
    GroupByTime {
        time_column: String,
        every: u32,
        unit: TimeUnit,
        #[serde(default)]
        output_column: Option<String>, // Name for the resulting time bucket column
        timestamp_format: String,
        #[serde(default)]
        timestamp_timezone: Option<String>, // default UTC
        #[serde(default)]
        precision: Option<TimeUnitPrecision>,
        #[serde(default)]
        additional_groups: Vec<String>, // Additional columns to group by
        aggregate: Vec<Aggregate>,
    },
    Sort {
        column: String,
        order: String,
        limit: Option<u32>,
    },
    // support more then one input..
    Join {
        right_df: String,
        left_on: Vec<String>,
        right_on: Vec<String>,
        how: JoinType,
    },

    WithColumn {
        name: Option<String>,
        expression: Expression,
    },

    Pivot {
        index: Vec<String>,
        columns: String,
        values: String,
        aggregate_function: AllowedGroupFunction,
    },
    PivotAdvanced {
        index: Vec<String>,     // Column(s) to use as index/row labels
        columns: String,        // Column whose values will become new columns
        values: Vec<Aggregate>, // Columns with values and their aggregation functions
    },

    Window {
        column: String,            // Column to apply the function to
        function: WindowFunction,  // Window function to apply
        partition_by: Vec<String>, // Columns to partition by (optional)
        order_by: Vec<String>,     // Columns to order by (optional)
        #[serde(default)]
        descending: Vec<bool>, // Whether to sort in descending order (optional)
        #[serde(default)]
        bounds: Option<WindowBound>, // Window bounds (optional)
        name: String,              // Name for the resulting column
    },

    Rename {
        mappings: Vec<ColumnRename>,
    },
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type", content = "params")]
pub enum WindowFunction {
    Sum,
    Min,
    Max,
    Mean,
    Count,
    First,
    Last,
    Rank,
    DenseRank,
    RowNumber,
    CumSum,
    Lag {
        offset: u32,
        default_value: Option<LiteralValue>,
    },
    Lead {
        offset: u32,
        default_value: Option<LiteralValue>,
    },
    RollingMean,
}

#[derive(Deserialize, Debug, Clone)]
pub struct WindowBound {
    #[serde(default)]
    pub preceding: Option<usize>,
    #[serde(default)]
    pub following: Option<usize>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ColumnRename {
    pub old_name: String,
    pub new_name: String,
}
#[derive(Deserialize, Debug)]
#[serde(untagged)] // Important! Allows serde to try each variant
pub enum FilterField {
    SingleNumber(i64),
    NumberList(Vec<i64>),
    StringList(Vec<String>),
    SingleString(String),
    SingleFloat(f64),
    FloatList(Vec<f64>),
    Boolean(bool),
    Date(chrono::NaiveDate),
    DateTime(chrono::DateTime<chrono::Utc>),
}

#[derive(Deserialize, Debug)]
pub enum TimeUnitPrecision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
}

#[derive(Deserialize, Debug)]
pub enum TimeUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
    Months,
    Quarters,
    Years,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum LiteralValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Date(chrono::NaiveDate),
    DateTime(chrono::DateTime<chrono::Utc>),
    StringList(Vec<String>),
    IntegerList(Vec<i64>),
    FloatList(Vec<f64>),
}

#[derive(Deserialize, Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
    Cross,
    Semi,
    Anti,
}

#[derive(Deserialize, Debug)]
pub struct Aggregate {
    pub column: String,
    pub alias: Option<String>,
    pub function: AllowedGroupFunction,
}

#[derive(Deserialize, Debug)]
pub enum AllowedGroupFunction {
    MIN,
    MAX,
    SUM,
    MEAN,
    MEDIAN,
    STD(u8),
    VAR(u8),
    COUNT,
    FIRST,
    LAST,
    NUNIQUE,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Expression {
    Column {
        value: String,
    },
    Literal {
        value: LiteralValue,
    },
    BinaryOp {
        left: Box<Expression>,
        op: ExpressionOperation,
        right: Box<Expression>,
    },
    Function {
        name: ExpressionFunction,
    },
    Conditional {
        condition: Box<Expression>,
        then: Box<Expression>,
        otherwise: Box<Expression>,
    },
}

#[derive(Deserialize, Debug)]
pub enum ExpressionFunction {
    CONCAT {
        column1: String,
        column2: String,
    },
    LOWER {
        column: String,
    },
    UPPER {
        column: String,
    },
    DATEPART,
    ABS {
        column: String,
    },
    ROUND {
        column: String,
        num: u32,
    },
    TOINT {
        size: u8,
        column: String,
    },
    TRIM {
        column: String,
        chars: String,
    },
    REPLACE {
        column: String,
        pattern: String,
        replacement: String,
        literal: bool,
    },
    SUBSTRING {
        column: String,
        start: u32,
        length: u32,
    },
    ISNULL {
        column: String,
    },
    ISNOTNULL {
        column: String,
    },
    YEAR {
        column: String,
    },
    MONTH {
        column: String,
    },
    DAY {
        column: String,
    },
    HOUR {
        column: String,
    },
    MINUTE {
        column: String,
    },
    SECOND {
        column: String,
    },
    FLOOR {
        column: String,
    },
    CEIL {
        column: String,
    },
    SQRT {
        column: String,
    },
    CONTAINS {
        column: String,
        value: String,
    },
    REGEX_MATCH {
        column: String,
        pattern: String,
    },
}
#[derive(Deserialize, Debug)]
pub enum ExpressionOperation {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    MODULO,
    CONCAT,
    AND,
    OR,
    EQ,
    NEQ,
    LT,
    LTE,
    GT,
    GTE,
}

impl Aggregate {
    pub fn to_polars_expr(&self) -> Result<polars::prelude::Expr, String> {
        let col = col(&self.column);

        let col = match self.function {
            AllowedGroupFunction::MIN => col.min(),
            AllowedGroupFunction::MAX => col.max(),
            AllowedGroupFunction::SUM => col.sum(),
            AllowedGroupFunction::MEAN => col.mean(),
            AllowedGroupFunction::MEDIAN => col.median(),
            AllowedGroupFunction::STD(ddof) => col.std(ddof),
            AllowedGroupFunction::VAR(ddof) => col.var(ddof),
            AllowedGroupFunction::COUNT => col.count(),
            AllowedGroupFunction::FIRST => col.first(),
            AllowedGroupFunction::LAST => col.last(),
            AllowedGroupFunction::NUNIQUE => col.n_unique(),
        };

        let col = if let Some(alias) = &self.alias {
            col.alias(alias)
        } else {
            col
        };
        Ok(col)
    }
}

impl FilterField {
    pub fn to_polars_expr(&self) -> Result<polars::prelude::Expr, String> {
        match self {
            FilterField::SingleNumber(value) => Ok(lit(*value)),
            //FilterField::NumberList(values) => Ok(lit(values.clone())),
            FilterField::SingleString(value) => Ok(lit(value.clone())),
            //FilterField::StringList(values) => Ok(lit(values.clone())),
            FilterField::SingleFloat(value) => Ok(lit(*value)),
            //FilterField::FloatList(values) => Ok(lit(values.clone())),
            FilterField::Boolean(value) => Ok(lit(*value)),
            FilterField::Date(date) => Ok(lit(*date)),
            //FilterField::DateTime(date_time) => Ok(lit(*date_time)),
            _ => Err("Unsupported filter field type".to_string()),
        }
    }
}

impl Expression {
    pub fn to_polars_expr(&self) -> Result<polars::prelude::Expr, String> {
        match self {
            Expression::Column { value } => Ok(col(value)),

            Expression::Literal { value } => match value {
                LiteralValue::String(s) => Ok(lit(s.clone())),
                LiteralValue::Integer(i) => Ok(lit(*i)),
                LiteralValue::Float(f) => Ok(lit(*f)),
                LiteralValue::Boolean(b) => Ok(lit(*b)),
                LiteralValue::Null => Ok(lit(NULL)),
                LiteralValue::Date(naive_date) => Ok(lit(naive_date.and_hms_opt(0, 0, 0).unwrap())
                    .cast(polars::prelude::DataType::Date)),
                LiteralValue::DateTime(date_time) => Ok(lit(date_time.timestamp_millis()).cast(
                    polars::prelude::DataType::Datetime(
                        polars::prelude::TimeUnit::Milliseconds,
                        None,
                    ),
                )),
                LiteralValue::StringList(items) => todo!(),
                LiteralValue::IntegerList(items) => todo!(),
                LiteralValue::FloatList(items) => todo!(),
            },

            Expression::BinaryOp { left, op, right } => {
                let left_expr = left.to_polars_expr()?;
                let right_expr = right.to_polars_expr()?;

                match op {
                    // Arithmetic operations
                    ExpressionOperation::ADD => Ok(left_expr + right_expr),
                    ExpressionOperation::SUBTRACT => Ok(left_expr - right_expr),
                    ExpressionOperation::MULTIPLY => Ok(left_expr * right_expr),
                    ExpressionOperation::DIVIDE => Ok(left_expr / right_expr),
                    ExpressionOperation::MODULO => Ok(left_expr % right_expr),

                    // Comparison operations
                    ExpressionOperation::EQ => Ok(left_expr.eq(right_expr)),
                    ExpressionOperation::NEQ => Ok(left_expr.neq(right_expr)),
                    ExpressionOperation::GT => Ok(left_expr.gt(right_expr)),
                    ExpressionOperation::LT => Ok(left_expr.lt(right_expr)),
                    ExpressionOperation::GTE => Ok(left_expr.gt_eq(right_expr)),
                    ExpressionOperation::LTE => Ok(left_expr.lt_eq(right_expr)),

                    // Boolean operations
                    ExpressionOperation::AND => Ok(left_expr.and(right_expr)),
                    ExpressionOperation::OR => Ok(left_expr.or(right_expr)),

                    // String operations
                    ExpressionOperation::CONCAT => {
                        Ok(left_expr.cast(DataType::String) + right_expr.cast(DataType::String))
                    }
                }
            }

            Expression::Function { name } => {
                match name {
                    ExpressionFunction::CONCAT { column1, column2 } => Ok(col(column1)
                        .cast(DataType::String)
                        + col(column2).cast(DataType::String)),
                    ExpressionFunction::LOWER { column } => Ok(col(column).str().to_lowercase()),
                    ExpressionFunction::UPPER { column } => Ok(col(column).str().to_uppercase()),
                    ExpressionFunction::DATEPART => {
                        //https://docs.rs/polars/latest/polars/prelude/enum.TemporalFunction.html
                        Ok(col("date").dt().year())
                    }
                    ExpressionFunction::ABS { column } => Ok(col(column).abs()),
                    ExpressionFunction::ROUND { column, num } => Ok(col(column).round(*num)),
                    ExpressionFunction::TOINT { size, column } => {
                        let col = col(column);
                        match size {
                            8 => Ok(col.cast(DataType::Int8)),
                            16 => Ok(col.cast(DataType::Int16)),
                            32 => Ok(col.cast(DataType::Int32)),
                            64 => Ok(col.cast(DataType::Int64)),
                            _ => Err("Invalid size for toint function".to_string()),
                        }
                    }
                    ExpressionFunction::TRIM { column, chars } => {
                        // confirm how this works
                        Ok(col(column).str().strip_chars(lit(chars.clone())))
                    }
                    ExpressionFunction::REPLACE {
                        column,
                        pattern,
                        replacement,
                        literal,
                    } => Ok(col(column).str().replace_all(
                        lit(pattern.clone()),
                        lit(replacement.clone()),
                        *literal,
                    )),
                    ExpressionFunction::SUBSTRING {
                        column,
                        start,
                        length,
                    } => Ok(col(column).str().slice(lit(*start), lit(*length))),
                    ExpressionFunction::ISNULL { column } => Ok(col(column).is_null()),
                    ExpressionFunction::ISNOTNULL { column } => Ok(col(column).is_not_null()),
                    ExpressionFunction::YEAR { column } => {
                        Ok(col(column).dt().year().cast(DataType::Int32))
                    }
                    ExpressionFunction::MONTH { column } => {
                        Ok(col(column).dt().month().cast(DataType::Int32))
                    }
                    ExpressionFunction::DAY { column } => {
                        Ok(col(column).dt().day().cast(DataType::Int32))
                    }
                    ExpressionFunction::HOUR { column } => {
                        Ok(col(column).dt().hour().cast(DataType::Int32))
                    }
                    ExpressionFunction::MINUTE { column } => {
                        Ok(col(column).dt().minute().cast(DataType::Int32))
                    }
                    ExpressionFunction::SECOND { column } => {
                        Ok(col(column).dt().second().cast(DataType::Int32))
                    }
                    ExpressionFunction::CONTAINS { column, value } => {
                        Ok(col(column).str().contains_literal(lit(value.clone())))
                    }

                    ExpressionFunction::REGEX_MATCH { column, pattern } => {
                        Ok(col(column).str().contains(lit(pattern.clone()), true))
                    }
                    ExpressionFunction::FLOOR { column } => Ok(col(column).floor()),
                    ExpressionFunction::CEIL { column } => Ok(col(column).ceil()),
                    ExpressionFunction::SQRT { column } => Ok(col(column).sqrt()),
                }
            }

            Expression::Conditional {
                condition,
                then,
                otherwise,
            } => {
                let cond_expr = condition.to_polars_expr()?;
                let then_expr = then.to_polars_expr()?;
                let else_expr = otherwise.to_polars_expr()?;
                Ok(when(cond_expr).then(then_expr).otherwise(else_expr))
            }
        }
    }
}
fn lit_to_expr(value: &LiteralValue) -> Expr {
    match value {
        LiteralValue::String(s) => lit(s.clone()),
        LiteralValue::Integer(i) => lit(*i),
        LiteralValue::Float(f) => lit(*f),
        LiteralValue::Boolean(b) => lit(*b),
        LiteralValue::Null => lit(NULL),
        LiteralValue::Date(naive_date) => {
            lit(naive_date.and_hms_opt(0, 0, 0).unwrap()).cast(polars::prelude::DataType::Date)
        }
        LiteralValue::DateTime(date_time) => lit(date_time.timestamp_millis()).cast(
            polars::prelude::DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None),
        ),
        LiteralValue::StringList(items) => todo!(),
        LiteralValue::IntegerList(items) => todo!(),
        LiteralValue::FloatList(items) => todo!(),
    }
}

// move to tryfrom
impl Operation {
    pub fn to_polars_expr(&self) -> Result<polars::prelude::Expr, String> {
        match self {
            Operation::GroupByTime {
                time_column,
                every,
                unit,
                precision,
                output_column,
                timestamp_format,
                timestamp_timezone,
                ..
            } => {
                // Create time bucket column
                //2023-04-01T00:01:35-07:00
                let precision = precision.as_ref().map(|f| match f {
                    TimeUnitPrecision::Nanoseconds => polars::prelude::TimeUnit::Nanoseconds,
                    TimeUnitPrecision::Microseconds => polars::prelude::TimeUnit::Microseconds,
                    TimeUnitPrecision::Milliseconds => polars::prelude::TimeUnit::Milliseconds,
                });
                let format = Some(timestamp_format.into());
                let timezone = timestamp_timezone
                    .as_ref()
                    .map(|f| polars::prelude::TimeZone::from_string(f.into()));
                let expr = col(time_column);
                let expr = expr.str().to_datetime(
                    precision,
                    timezone, // UTC timezone (or use your specific zone)
                    polars::prelude::StrptimeOptions {
                        format,        // Your datetime format
                        strict: false, // Allow some flexibility in parsing
                        exact: true,   // Require the entire string to match
                        cache: true,   // Cache parsed patterns for performance
                    },
                    lit("infer"), // How to handle ambiguous dates (options: "raise", "infer", etc.)
                );

                let mut round_expr = match unit {
                    TimeUnit::Seconds => expr.dt().round(lit(format!("{}s", every))),
                    TimeUnit::Minutes => expr.dt().round(lit(format!("{}m", every))),
                    TimeUnit::Hours => expr.dt().round(lit(format!("{}h", every))),
                    TimeUnit::Days => expr.dt().round(lit(format!("{}d", every))),
                    TimeUnit::Weeks => expr.dt().round(lit(format!("{}w", every))),
                    TimeUnit::Months => expr.dt().round(lit(format!("{}mo", every))),
                    TimeUnit::Quarters => expr.dt().round(lit(format!("{}q", every))),
                    TimeUnit::Years => expr.dt().round(lit(format!("{}y", every))),
                };
                if let Some(output_column) = output_column {
                    round_expr = round_expr.alias(output_column);
                }
                Ok(round_expr)
            }
            Operation::Select { .. } => {
                unreachable!("Select operation should be handled in the main function")
            }
            Operation::Window {
                column,
                function,
                partition_by,
                order_by,
                descending,
                bounds,
                name,
            } => {
                let partition_exprs: Vec<Expr> = partition_by.iter().map(col).collect();
                // Configure window function with appropriate options
                let window_expr = match function {
                    WindowFunction::Sum => col(column).sum().over(partition_exprs.clone()),
                    WindowFunction::Min => col(column).min().over(partition_exprs.clone()),
                    WindowFunction::Max => col(column).max().over(partition_exprs.clone()),
                    WindowFunction::RollingMean => {
                        let options = RollingOptionsFixedWindow {
                            window_size: 3,
                            min_periods: 1,
                            center: false,
                            ..Default::default()
                        };
                        col(column)
                            .rolling_mean(options)
                            .over(partition_exprs.clone())
                    }
                    WindowFunction::Mean => col(column).mean().over(partition_exprs.clone()),
                    WindowFunction::Count => col(column).count().over(partition_exprs.clone()),
                    WindowFunction::First => col(column).first().over(partition_exprs.clone()),
                    WindowFunction::Last => col(column).last().over(partition_exprs.clone()),
                    WindowFunction::CumSum => {
                        col(column).cum_sum(false).over(partition_exprs.clone())
                    }
                    WindowFunction::Lag {
                        offset,
                        default_value,
                    } => {
                        let mut expr = col(column).shift(lit(*offset));
                        if let Some(default) = default_value {
                            expr = expr.fill_null(lit_to_expr(default));
                        }
                        expr.over(partition_exprs.clone())
                    }
                    WindowFunction::Lead {
                        offset,
                        default_value,
                    } => {
                        let mut expr = col(column).shift(lit(*offset));
                        if let Some(default) = default_value {
                            expr = expr.fill_null(lit_to_expr(default));
                        }
                        expr.over(partition_exprs.clone())
                    }
                    WindowFunction::Rank => {
                        Expr::rank(col(column), polars::prelude::RankOptions::default(), None)
                            .over(partition_exprs.clone())
                    }
                    WindowFunction::DenseRank => Expr::rank(
                        col(column),
                        polars::prelude::RankOptions {
                            method: polars::prelude::RankMethod::Dense,
                            ..Default::default()
                        },
                        None,
                    )
                    .over(partition_exprs.clone()),
                    WindowFunction::RowNumber => Expr::rank(
                        col(column),
                        polars::prelude::RankOptions {
                            method: polars::prelude::RankMethod::Ordinal,
                            ..Default::default()
                        },
                        None,
                    )
                    .over(partition_exprs.clone()),
                };

                Ok(window_expr.alias(name))
            }

            Operation::Filter {
                column,
                condition,
                filter,
            } => {
                let col = col(column);
                let filter_expr = match filter {
                    Some(filter) => Ok(filter.to_polars_expr()?),
                    None => Err("Filter expression is missing".to_string()),
                };
                match condition {
                    AllowedFilterCondition::EQ => Ok(col.eq(filter_expr?)),
                    AllowedFilterCondition::EQMISSING => Ok(col.eq_missing(filter_expr?)),
                    AllowedFilterCondition::NEQ => Ok(col.neq(filter_expr?)),
                    AllowedFilterCondition::LT => Ok(col.lt(filter_expr?)),
                    AllowedFilterCondition::LTE => Ok(col.lt_eq(filter_expr?)),
                    AllowedFilterCondition::GT => Ok(col.gt(filter_expr?)),
                    AllowedFilterCondition::GTE => Ok(col.gt_eq(filter_expr?)),
                    AllowedFilterCondition::ISNULL => Ok(col.is_null()),
                    AllowedFilterCondition::ISNOTNULL => Ok(col.is_not_null()),
                }
            }
            _ => Err("Unsupported operation".to_string()),
        }
    }
}
