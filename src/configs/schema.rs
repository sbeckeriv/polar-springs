use serde::Deserialize;

use crate::config::LiteralValue;
#[derive(Deserialize, Debug, Clone)]
pub struct Schema {
    pub columns: Vec<SchemaColumn>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    pub dtype: SchemaDtype,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub allow: Option<Vec<LiteralValue>>,
    #[serde(default)]
    pub min: Option<LiteralValue>,
    #[serde(default)]
    pub max: Option<LiteralValue>,
}
impl Schema {
    /// Enforce schema on a DataFrame (eager). Returns Result<(), String> on failure.
    pub fn validate_dataframe(&self, df: &polars::prelude::DataFrame) -> Result<(), String> {
        use polars::prelude::*;
        for col in &self.columns {
            // Check required
            if col.required
                && !df
                    .get_column_names()
                    .contains(&&PlSmallStr::from_str(&col.name))
            {
                return Err(format!("Missing required column: {}", col.name));
            }

            // If column exists, check type and constraints
            if let Ok(series) = df.column(&col.name) {
                // Type check
                let dtype_matches = match (&col.dtype, series.dtype()) {
                    (SchemaDtype::Int8, DataType::Int8)
                    | (SchemaDtype::Int16, DataType::Int16)
                    | (SchemaDtype::Int32, DataType::Int32)
                    | (SchemaDtype::Int64, DataType::Int64)
                    | (SchemaDtype::UInt8, DataType::UInt8)
                    | (SchemaDtype::UInt16, DataType::UInt16)
                    | (SchemaDtype::UInt32, DataType::UInt32)
                    | (SchemaDtype::UInt64, DataType::UInt64)
                    | (SchemaDtype::Float32, DataType::Float32)
                    | (SchemaDtype::Float64, DataType::Float64)
                    | (SchemaDtype::Boolean, DataType::Boolean)
                    | (SchemaDtype::Utf8, DataType::String)
                    | (SchemaDtype::Date, DataType::Date)
                    | (SchemaDtype::DateTime, DataType::Datetime(_, _)) => true,
                    _ => false,
                };
                if !dtype_matches {
                    return Err(format!(
                        "Column '{}' type mismatch: expected {:?}, got {:?}",
                        col.name,
                        col.dtype,
                        series.dtype()
                    ));
                }
                // Allowed values
                if let Some(allowed) = &col.allow {
                    todo!("allow values check");
                }
                // Min/max (only for numeric types)
                if let Some(min) = &col.min {
                    if let Ok(ca) = series.f64() {
                        if ca
                            .into_iter()
                            .any(|v| v.map(|vv| vv < min.as_f64().unwrap()).unwrap_or(false))
                        {
                            return Err(format!("Column '{}' has values below min", col.name));
                        }
                    }
                }
                if let Some(max) = &col.max {
                    if let Ok(ca) = series.f64() {
                        if ca
                            .into_iter()
                            .any(|v| v.map(|vv| vv > max.as_f64().unwrap()).unwrap_or(false))
                        {
                            return Err(format!("Column '{}' has values above max", col.name));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// Helper for LiteralValue to f64 (for min/max)
impl LiteralValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            LiteralValue::Float(f) => Some(*f),
            LiteralValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "params")]
pub enum SchemaDtype {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    Utf8,
    Date,
    DateTime,
}
