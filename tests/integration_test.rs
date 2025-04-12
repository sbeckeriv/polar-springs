use std::fs;
use std::path::Path;

use ploars_cli::runner::run;

fn setup_test_config(name: &str, content: &str) -> String {
    let dir_path = Path::new("tests/test_configs");
    if !dir_path.exists() {
        fs::create_dir_all(dir_path).expect("Failed to create test directory");
    }

    let file_path = dir_path.join(format!("{}.toml", name));
    fs::write(&file_path, content).expect("Failed to write test config file");

    file_path.to_str().unwrap().to_string()
}

fn setup_test_logs() -> String {
    let logs_path = Path::new("tests/request_logs.json");

    logs_path.to_str().unwrap().to_string()
}

#[test]
fn test_filter_status_code() {
    let config = setup_test_config(
        "filter_status_code",
        r#"
[[operations]]
type = "Filter"
column = "status_code"
condition = "GTE"
filter = 400
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Filter operation failed");
    // Add specific assertions about the result if needed
}

#[test]
fn test_filter_error() {
    let config = setup_test_config(
        "filter_error",
        r#"
[[operations]]
type = "Filter"
column = "is_error"
condition = "EQ"
filter = true
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Filter error operation failed");
}

#[test]
fn test_filter_not_null() {
    let config = setup_test_config(
        "filter_not_null",
        r#"
[[operations]]
type = "Filter"
column = "error_type"
condition = "ISNOTNULL"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Filter not null operation failed");
}

#[test]
fn test_select_columns() {
    let config = setup_test_config(
        "select_columns",
        r#"
[[operations]]
type = "Select"
columns = ["timestamp", "service_name", "endpoint", "status_code", "response_time_ms"]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Select columns operation failed");
}

#[test]
fn test_group_by() {
    let config = setup_test_config(
        "group_by",
        r#"
[[operations]]
type = "GroupBy"
columns = ["service_name", "endpoint"]
aggregate = [
  { column = "response_time_ms", function = "MEAN" },
  { column = "status_code", function = "COUNT" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Group by operation failed");
}

#[test]
fn test_group_by_time() {
    let config = setup_test_config(
        "group_by_time",
        r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 1
unit = "Hours"
output_column = "hour_bucket"
additional_groups = ["service_name"]
aggregate = [
  { column = "response_time_ms", function = "MEAN" },
  { column = "response_time_ms", function = "MAX" },
  { column = "status_code", function = "COUNT" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Group by time operation failed");
}

#[test]
fn test_sort() {
    let config = setup_test_config(
        "sort",
        r#"
[[operations]]
type = "Sort"
column = "response_time_ms"
order = "desc"
limit = 10
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Sort operation failed");
}

#[test]
fn test_with_column_binary_op() {
    let config = setup_test_config(
        "with_column_binary_op",
        r#"
[[operations]]
type = "WithColumn"
name = "is_slow_response"
expression = { 
  type = "BinaryOp", 
  left = { type = "Column", value = "response_time_ms" }, 
  op = "GT", 
  right = { type = "Literal", value = 100 }
}
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "WithColumn binary op failed");
}

#[test]
fn test_with_column_function() {
    let config = setup_test_config(
        "with_column_function",
        r#"
[[operations]]
type = "WithColumn"
name = "total_processing_time"
expression = { 
  type = "BinaryOp", 
  left = { type = "Column", value = "response_time_ms" }, 
  op = "ADD", 
  right = { 
    type = "Function", 
    name = "ABS", 
    args = [{ type = "Column", value = "external_call_time_ms" }]
  }
}
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "WithColumn function failed");
}

#[test]
fn test_pivot() {
    let config = setup_test_config(
        "pivot",
        r#"
[[operations]]
type = "Pivot"
index = ["geo_region"]
columns = "service_name"
values = "response_time_ms"
aggregate_function = "MEAN"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Pivot operation failed");
}

#[test]
fn test_pivot_advanced() {
    let config = setup_test_config(
        "pivot_advanced",
        r#"
[[operations]]
type = "PivotAdvanced"
index = ["geo_region", "endpoint"]
columns = "service_name"
values = [
  { column = "response_time_ms", function = "MEAN" },
  { column = "cpu_utilization", function = "MAX" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "PivotAdvanced operation failed");
}

#[test]
fn test_window_cumsum() {
    let config = setup_test_config(
        "window_cumsum",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
function = "CumSum"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "cumulative_response_time"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Window CumSum operation failed");
}

#[test]
fn test_window_lag() {
    let config = setup_test_config(
        "window_lag",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
function = { 
  Lag = { 
    offset = 1, 
    default_value = { Integer = 0 } 
  }
}
partition_by = ["service_name"]
order_by = ["timestamp"]
bounds = { preceding = 3, following = 0 }
name = "prev_response_time"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Window Lag operation failed");
}

#[test]
fn test_rename() {
    let config = setup_test_config(
        "rename",
        r#"
[[operations]]
type = "Rename"
mappings = [
  { old_name = "response_time_ms", new_name = "latency_ms" },
  { old_name = "status_code", new_name = "http_status" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Rename operation failed");
}

#[test]
fn test_complex_workflow() {
    let config = setup_test_config(
        "complex_workflow",
        r#"
[[operations]]
type = "Filter"
column = "timestamp"
condition = "GTE"
filter = "2023-04-01T00:00:00-07:00"

[[operations]]
type = "Select"
columns = ["timestamp", "service_name", "endpoint", "status_code", "response_time_ms", "geo_region"]

[[operations]]
type = "WithColumn"
name = "is_error_response"
expression = { 
  type = "BinaryOp", 
  left = { type = "Column", value = "status_code" }, 
  op = "GTE", 
  right = { type = "Literal", value = 400 }
}

[[operations]]
type = "GroupBy"
columns = ["service_name", "geo_region", "is_error_response"]
aggregate = [
  { column = "response_time_ms", function = "MEAN" },
  { column = "response_time_ms", function = "MAX" },
  { column = "status_code", function = "COUNT" }
]

[[operations]]
type = "Sort"
column = "COUNT(status_code)"
order = "desc"
limit = 5
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "Complex workflow failed");
}
