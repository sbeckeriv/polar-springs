use std::fs;
use std::path::Path;

use ploars_cli::runner::run;
use std::sync::Once;

static START: Once = Once::new();

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
    START.call_once(|| {
        tracing_subscriber::fmt::init();
    });
    let logs_path = Path::new("tests/request_logs.json");

    logs_path.to_str().unwrap().to_string()
}

#[test]
fn test_window_cumsum() {
    let config = setup_test_config(
        "window_cumsum",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "cumulative_response_time"

[operations.function]
type = "cumsum"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window CumSum operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_lag() {
    let config = setup_test_config(
        "window_lag",
        r#"

[[operations]]
type = "Sort"
column = "endpoint"
order = "desc"


[[operations]]
type = "Window"
column = "response_time_ms"
name = "lagged_response_time"
partition_by = ["endpoint"]
order_by = ["endpoint"]
descending = [false]

[operations.function]
type = "lag"
params = { offset = 2 }

[[operations]]
type = "Select"
columns = ["timestamp", "service_name", "endpoint", "status_code", "response_time_ms", "lagged_response_time"]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window Lag operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_sum_with_bounds() {
    let config = setup_test_config(
        "window_sum_bounds",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "sum_response_time"
bounds = { preceding = 1, following = 1 }

[operations.function]
type = "sum"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window Sum with bounds operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_rank_descending() {
    let config = setup_test_config(
        "window_rank_desc",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
descending = [true]
name = "rank_response_time"

[operations.function]
type = "rank"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window Rank with descending order failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_lead_with_default() {
    let config = setup_test_config(
        "window_lead_default",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "lead_response_time"

[operations.function]
type = "lead"
params = { offset = 1, default_value = 0 }
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window Lead with default value failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_min() {
    let config = setup_test_config(
        "window_min",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "min_response_time"

[operations.function]
type = "min"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window Min operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_max() {
    let config = setup_test_config(
        "window_max",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "max_response_time"

[operations.function]
type = "max"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window Max operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_count() {
    let config = setup_test_config(
        "window_count",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "count_response_time"

[operations.function]
type = "count"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window Count operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_first_last() {
    let config = setup_test_config(
        "window_first_last",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "first_response_time"

[operations.function]
type = "first"

[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "last_response_time"

[operations.function]
type = "last"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window First/Last operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_dense_rank_row_number() {
    let config = setup_test_config(
        "window_dense_rank_row_number",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "dense_rank_response_time"

[operations.function]
type = "denserank"

[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "row_number_response_time"

[operations.function]
type = "rownumber"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window DenseRank/RowNumber operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_lag_lead_bounds_desc() {
    let config = setup_test_config(
        "window_lag_lead_bounds_desc",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name", "endpoint"]
order_by = ["timestamp", "endpoint"]
descending = [true, false]
bounds = { preceding = 2 }
name = "lag_response_time"

[operations.function]
type = "lag"
params = { offset = 1, default_value = 123 }

[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name", "endpoint"]
order_by = ["timestamp", "endpoint"]
descending = [false, true]
bounds = { following = 2 }
name = "lead_response_time"

[operations.function]
type = "lead"
params = { offset = 1, default_value = 456 }
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window Lag/Lead with bounds and descending failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_window_rollingmean() {
    let config = setup_test_config(
        "window_rollingmean",
        r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "rollingmean_response_time"

[operations.function]
type = "rollingmean"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "Window RollingMean operation failed {}",
        result.err().unwrap()
    );
}
