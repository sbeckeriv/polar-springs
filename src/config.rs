use chrono::NaiveDate;
use polars::prelude::{col, lit, DataType, NULL};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub operations: Vec<Operation>,
}

#[derive(Deserialize, Debug)]
enum AllowedFilterCondition {
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
        name: String,
        expression: Expression,
    },

    Pivot {
        index: Vec<String>,
        columns: String,
        values: String,
        aggregate_function: AllowedGroupFunction,
    },

    Window {
        column: String,
        function: WindowFunction,
        window_size: usize,
        output_column: String,
    },

    Rename {
        mappings: Vec<ColumnRename>,
    },
}

// move to tryfrom
impl Operation {
    pub fn to_polars_expr(&self) -> Result<polars::prelude::Expr, String> {
        match self {
            Operation::Select { .. } => {
                unreachable!("Select operation should be handled in the main function")
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
                    _ => return Err("Unsupported filter condition".to_string()),
                }
            }
            _ => Err("Unsupported operation".to_string()),
        }
    }
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

#[derive(Deserialize, Debug)]
pub struct Aggregate {
    pub column: String,
    pub function: AllowedGroupFunction,
}
impl Aggregate {
    pub fn to_polars_expr(&self) -> Result<polars::prelude::Expr, String> {
        let col = col(&self.column);
        match self.function {
            AllowedGroupFunction::MIN => Ok(col.min()),
            AllowedGroupFunction::MAX => Ok(col.max()),
            AllowedGroupFunction::SUM => Ok(col.sum()),
            AllowedGroupFunction::MEAN => Ok(col.mean()),
            AllowedGroupFunction::MEDIAN => Ok(col.median()),
            AllowedGroupFunction::STD(ddof) => Ok(col.std(ddof)),
            AllowedGroupFunction::VAR(ddof) => Ok(col.var(ddof)),
            AllowedGroupFunction::COUNT => Ok(col.count()),
            AllowedGroupFunction::FIRST => Ok(col.first()),
            AllowedGroupFunction::LAST => Ok(col.last()),
            AllowedGroupFunction::NUNIQUE => Ok(col.n_unique()),
        }
    }
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
    Column(String),
    Literal(LiteralValue),
    BinaryOp {
        left: Box<Expression>,
        op: ExpressionOperation,
        right: Box<Expression>,
    },
    Function {
        name: ExpressionFunction,
        args: Vec<Expression>,
    },
    Conditional {
        condition: Box<Expression>,
        then: Box<Expression>,
        otherwise: Box<Expression>,
    },
}

#[derive(Deserialize, Debug)]
pub enum ExpressionFunction {
    CONCAT,
    LOWER,
    UPPER,
    DATEPART,
    ABS,
    ROUND,
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
impl Expression {
    pub fn to_polars_expr(&self) -> Result<polars::prelude::Expr, String> {
        match self {
            Expression::Column(value) => Ok(col(value)),

            Expression::Literal(value) => match value {
                LiteralValue::String(s) => Ok(lit(s.clone())),
                LiteralValue::Integer(i) => Ok(lit(*i)),
                LiteralValue::Float(f) => Ok(lit(*f)),
                LiteralValue::Boolean(b) => Ok(lit(*b)),
                LiteralValue::Null => Ok(lit(NULL)),
                LiteralValue::Date(naive_date) => todo!(),
                LiteralValue::DateTime(date_time) => todo!(),
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

                    _ => Err(format!("Unsupported binary operation: {:?}", op)),
                }
            }

            Expression::Function { name, args } => {
                // Convert all arguments to Polars expressions
                let polars_args: Result<Vec<polars::prelude::Expr>, String> =
                    args.iter().map(|arg| arg.to_polars_expr()).collect();
                let polars_args = polars_args?;

                match name {
                    // String functions
                    ExpressionFunction::CONCAT => {
                        if polars_args.is_empty() {
                            return Err(
                                "concat function requires at least one argument".to_string()
                            );
                        }
                        let mut result = polars_args[0].clone();
                        for arg in &polars_args[1..] {
                            result = result + arg.clone();
                        }
                        Ok(result)
                    }
                    ExpressionFunction::LOWER => {
                        if polars_args.len() != 1 {
                            return Err("lower function requires exactly one argument".to_string());
                        }
                        Ok(lit(polars_args[0].to_string().to_lowercase()))
                    }
                    ExpressionFunction::UPPER => {
                        if polars_args.len() != 1 {
                            return Err("upper function requires exactly one argument".to_string());
                        }
                        Ok(lit(polars_args[0].to_string().to_uppercase()))
                    }

                    // Date functions
                    ExpressionFunction::DATEPART => {
                        if polars_args.len() != 2 {
                            return Err(
                                "date_part function requires exactly two arguments".to_string()
                            );
                        }
                        todo!();
                        // Assuming first arg is the part name (as a literal) and second is the timestamp
                        /*
                                               if let Expression::Literal {
                                                   value: LiteralValue::String(part),
                                               } = &args[0]
                                               {
                                                   Ok(polars_args[1].dt().dt_part(part))
                                               } else {
                                                   Err("date_part first argument must be a string literal".to_string())
                                               }
                        */
                    }

                    // Math functions
                    ExpressionFunction::ABS => {
                        if polars_args.len() != 1 {
                            return Err("abs function requires exactly one argument".to_string());
                        }
                        todo!();
                        //Ok(polars_args[0].abs())
                    }
                    ExpressionFunction::ROUND => {
                        if polars_args.len() != 2 {
                            return Err("round function requires exactly two arguments".to_string());
                        }
                        todo!();

                        /*
                        // Second arg should be the number of decimal places
                        if let Expression::Literal {
                            value: LiteralValue::Integer(decimals),
                        } = &args[1]
                        {
                            Ok(polars_args[0].round(*decimals as u32))
                        } else {
                            Err("round second argument must be an integer literal".to_string())
                        }
                        */
                    }

                    _ => Err(format!("Unsupported function: {:?}", name)),
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
                todo!();
                //Ok(polars::when(cond_expr).then(then_expr).otherwise(else_expr))
            }
        }
    }
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
#[derive(Deserialize, Debug)]
pub enum WindowFunction {
    RollingMean,
    RollingSum,
    CumulativeSum,
    Lag(u32),
    Lead(u32),
    Rank,
    PercentRank,
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
