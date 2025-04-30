//! Data Format Inference Program
//! Reads stdin (CSV, JSON array, or JSON Lines), infers types, outputs JSON schema.
// cat tests/request_logs.json|  cargo run --bin file_schema

use std::collections::HashMap;
use std::io::{self, Read};

use serde_json::Value;

trait FormatParser {
    /// Returns a vector of records, each as a map from key to value (as string or null).
    fn parse_records(&self, input: &str) -> Vec<HashMap<String, Option<String>>>;
}

struct CsvParser;
struct JsonArrayParser;
struct JsonLinesParser;

impl FormatParser for CsvParser {
    fn parse_records(&self, input: &str) -> Vec<HashMap<String, Option<String>>> {
        let mut rdr = csv::Reader::from_reader(input.as_bytes());
        let headers = rdr.headers().unwrap().clone();
        let mut records = Vec::new();
        for result in rdr.records() {
            let record = result.unwrap();
            let mut map = HashMap::new();
            for (i, field) in record.iter().enumerate() {
                let key = headers.get(i).unwrap().to_string();
                map.insert(
                    key,
                    if field.is_empty() {
                        None
                    } else {
                        Some(field.to_string())
                    },
                );
            }
            records.push(map);
        }
        records
    }
}

impl FormatParser for JsonArrayParser {
    fn parse_records(&self, input: &str) -> Vec<HashMap<String, Option<String>>> {
        let v: Value = serde_json::from_str(input).unwrap();
        let arr = v.as_array().unwrap();
        arr.iter()
            .map(|obj| {
                obj.as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), value_to_opt_string(v)))
                    .collect()
            })
            .collect()
    }
}

impl FormatParser for JsonLinesParser {
    fn parse_records(&self, input: &str) -> Vec<HashMap<String, Option<String>>> {
        input
            .lines()
            .filter_map(|line| {
                if line.trim().is_empty() {
                    None
                } else {
                    let v: Value = serde_json::from_str(line).ok()?;
                    Some(
                        v.as_object()
                            .unwrap()
                            .iter()
                            .map(|(k, v)| (k.clone(), value_to_opt_string(v)))
                            .collect(),
                    )
                }
            })
            .collect()
    }
}

fn value_to_opt_string(v: &Value) -> Option<String> {
    if v.is_null() {
        None
    } else if v.is_string() {
        Some(v.as_str().unwrap().to_string())
    } else {
        Some(v.to_string())
    }
}

enum DetectedFormat {
    Csv,
    JsonArray,
    JsonLines,
}

fn detect_format(input: &str) -> Option<DetectedFormat> {
    // Try JSON array
    if let Ok(Value::Array(_)) = serde_json::from_str::<Value>(input) {
        return Some(DetectedFormat::JsonArray);
    }
    // Try JSON lines (first non-empty line parses as JSON object)
    if input
        .lines()
        .filter(|l| !l.trim().is_empty())
        .next()
        .and_then(|line| serde_json::from_str::<Value>(line).ok())
        .map(|v| v.is_object())
        .unwrap_or(false)
    {
        return Some(DetectedFormat::JsonLines);
    }
    // Try CSV (has at least one comma in header)
    if input.lines().next().map_or(false, |l| l.contains(',')) {
        return Some(DetectedFormat::Csv);
    }
    None
}

fn main() {
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer).unwrap();

    let format = detect_format(&buffer).expect("Could not detect input format");

    let parser: Box<dyn FormatParser> = match format {
        DetectedFormat::Csv => Box::new(CsvParser),
        DetectedFormat::JsonArray => Box::new(JsonArrayParser),
        DetectedFormat::JsonLines => Box::new(JsonLinesParser),
    };

    let records = parser.parse_records(&buffer);

    let mut columns: HashMap<String, Vec<Option<String>>> = HashMap::new();
    for record in &records {
        for (k, v) in record {
            columns.entry(k.clone()).or_default().push(v.clone());
        }
    }

    let mut schema = serde_json::Map::new();
    for (key, values) in columns {
        let mut has_null = false;
        let mut is_int = true;
        let mut is_float = true;
        let mut is_datetime = true;
        for v in &values {
            match v {
                None => has_null = true,
                Some(s) => {
                    if s.parse::<i64>().is_err() {
                        is_int = false;
                    }
                    if s.parse::<f64>().is_err() {
                        is_float = false;
                    }
                    if chrono::DateTime::parse_from_rfc3339(s).is_err()
                        && chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").is_err()
                        && chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_err()
                    {
                        is_datetime = false;
                    }
                }
            }
        }
        let typ = if is_int {
            "integer"
        } else if is_float {
            "float"
        } else if is_datetime {
            "datetime"
        } else {
            "string"
        };
        schema.insert(
            key,
            serde_json::json!({
                "type": typ,
                "nullable": has_null
            }),
        );
    }
    println!("{}", serde_json::to_string_pretty(&schema).unwrap());
}
