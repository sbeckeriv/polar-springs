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
/*


{"timestamp":"2023-04-01T00:01:35-07:00","request_id":"a006c36e-7925-464b-8c9a-17bc49bb31dd","service_name":"api-gateway",
"endpoint":"/v1/gateway","method":"PUT","status_code":302,"response_time_ms":170,"user_id":"user_142","client_ip":"us-201.98.52",
"user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124","request_size_bytes":578,
"response_size_bytes":988,"content_type":"text/html","is_error":false,"error_type":null,"geo_region":"us-east","has_external_call":true,
"external_service":"payment-gateway","external_endpoint":"/process","external_call_time_ms":39,"external_call_status":200,"db_query":null,
"db_name":null,"db_execution_time_ms":null,"cpu_utilization":83.48413,"memory_utilization":38.242134,"disk_io":54.512756,"network_io":149.16632}
*/

// 1. Request Count Over Time
#[test]
fn readme_request_count_over_time() {
    let config = setup_test_config(
        "readme_request_count_over_time",
        r#"

[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 1
unit = "Minutes"
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
output_column = "minute_bucket"
aggregate = [ { column = "response_time_ms", function = "MEAN" },  { column = "request_id", function = "COUNT" } ]

[[operations]]
type = "Sort"
column = "minute_bucket"
order = "desc"

"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 1 failed {}", result.err().unwrap());
}

// 2. Average Response Time by Endpoint
#[test]
fn readme_avg_response_time_by_endpoint() {
    let config = setup_test_config(
        "readme_avg_response_time_by_endpoint",
        r#"
[[operations]]
type = "GroupBy"
columns = ["endpoint"]
aggregate = [
  { column = "response_time_ms", function = "MEAN", alias = "response_time_mean" },
  { column = "response_time_ms", function = "MAX", alias = "response_time_max" },
  { column = "response_time_ms", function = "MIN", alias = "response_time_min" },
  { column = "request_id", function = "COUNT", alias = "request_count" }
]


[[operations]]
type = "Sort"
column = "response_time_mean"
order = "DESC"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 2 failed {}", result.err().unwrap());
}

// 3. Error Rate Over Time
#[test]
fn readme_error_rate_over_time() {
    let config = setup_test_config(
        "readme_error_rate_over_time",
        r#"
[[operations]]
type = "WithColumn"
name = "is_error"
expression = { type = "BinaryOp", left = { type = "Column", value ="status_code" }, op = "GTE", right = { type = "Literal", value=400 } }

[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
unit = "Seconds"
aggregate = [
  { column = "request_id", function = "COUNT", alias = "request_id_COUNT" },
  { column = "is_error", function = "SUM", alias = "is_error_SUM" }
]

[[operations]]
type = "WithColumn"
name = "error_rate"
expression = { type = "BinaryOp", left = { type = "BinaryOp", left = { type = "Column", value = "is_error_SUM" }, op = "MULTIPLY", right = { type = "Literal", value = 100.0 } }, op = "DIVIDE", right = { type = "Column", value= "request_id_COUNT" } }
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 3  failed {}", result.err().unwrap());
}

// 4. Status Code Distribution
#[test]
fn readme_status_code_distribution() {
    let config = setup_test_config(
        "readme_status_code_distribution",
        r#"

[[operations]]
type = "WithColumn"
name = "status_code_hundredths"
[operations.expression]
type = "BinaryOp"
op = "DIVIDE"
    [operations.expression.left]
    type = "Column"
    value = "status_code"
    [operations.expression.right]
    type = "Literal"
    value =  100 

[[operations]]
type = "GroupBy"
columns = ["status_code_hundredths"]
aggregate = [
  { column = "request_id", function = "COUNT", alias = "request_id_COUNT" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "request_id_COUNT", new_name = "count" }
]

[[operations]]
type = "Sort"
column = "count"
order = "DESC"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 4  failed {}", result.err().unwrap());
}

// 5. Top Endpoints by Request Volume
#[test]
fn readme_top_endpoints_by_request_volume() {
    let config = setup_test_config(
        "readme_top_endpoints_by_request_volume",
        r#"
[[operations]]
type = "GroupBy"
columns = ["endpoint"]
aggregate = [
  { column = "request_id", function = "COUNT", alias = "request_count" }
]

[[operations]]
type = "Sort"
column = "request_count"
order = "DESC"
limit = 5
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 5  failed {}", result.err().unwrap());
}

// 6. P90 Response Time with Window Function
#[test]
fn readme_p90_response_time_with_window_function() {
    let config = setup_test_config(
        "readme_p90_response_time_with_window_function",
        r#"
[[operations]]
type = "GroupByTime"
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
time_column = "timestamp"
every = 60
unit = "Seconds"
additional_groups = ["endpoint"]
aggregate = [
  { column = "response_time_ms", function = "MEAN", alias = "response_time_MEAN" },
]

[[operations]]
type = "Window"
name ="p90_response_time"
partition_by = ["endpoint"]
order_by = ["endpoint","timestamp"]
column = "response_time_MEAN"
function = {type ="rollingmean"}
window_size = 5
output_column = "p90_response_time"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 6  failed {}", result.err().unwrap());
}

// 7. Latency Heatmap by Hour and Endpoint
#[test]
fn readme_latency_heatmap_by_hour_and_endpoint() {
    let config = setup_test_config(
        "readme_latency_heatmap_by_hour_and_endpoint",
        r#"
[[operations]]
type = "WithColumn" 
name = "hour_of_day"
expression = { type = "Function", name = {HOUR =  { column ="timestamp", timestamp_format = "%Y-%m-%dT%H:%M:%S%z" } } }

[[operations]]
type = "Pivot"
index = ["endpoint"]
columns = "hour_of_day"
values = "response_time"
aggregate_function = "MEAN"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 7  failed {}", result.err().unwrap());
}

// 8. Throughput (Requests Per Second)
#[test]
fn readme_throughput_requests_per_second() {
    let config = setup_test_config(
        "readme_throughput_requests_per_second",
        r#"
[[operations]]
type = "GroupByTime"
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
time_column = "timestamp"
every = 60
unit = "Seconds"
aggregate = [
  { column = "request_id", function = "COUNT", alias = "request_id_COUNT" }
]
# silly when we can group by second above
[[operations]]
type = "WithColumn"
name = "requests_per_second"
expression = { type = "BinaryOp", left = { type = "Column", value = "request_id_COUNT" }, op = "DIVIDE", right = { type = "Literal", value= 60 } }
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 8  failed {}", result.err().unwrap());
}

// 9. Apdex Score (Application Performance Index)
#[test]
fn readme_apdex_score() {
    let config = setup_test_config(
        "readme_apdex_score",
        r#"

[[operations]]
type = "WithColumn"
name = "satisfaction"
output_column = "satisfaction"
expression = { type = "Conditional", condition = { type = "BinaryOp", left = { type = "Column", value = "response_time_ms" }, op = "LTE", right = { type = "Literal", value= 300 } }, then = { type = "Literal", value = 1 }, otherwise = { type = "Conditional", condition = { type = "BinaryOp", left = { type = "Column",value= "response_time_ms" }, op = "LTE", right = { type = "Literal",value= 1200 } }, then = { type = "Literal",value= 0.5 }, otherwise = { type = "Literal", value=0 } } }


[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
unit = "Seconds"
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
additional_groups = ["endpoint"]
aggregate = [
  { column = "satisfaction", function = "MEAN", alias = "apdex_score"},
  { column = "request_id", function = "COUNT", alias = "request_count" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 9  failed {}", result.err().unwrap());
}

// 10. Latency Percentiles (P50, P95, P99)
#[test]
fn readme_latency_percentiles() {
    let config = setup_test_config(
        "readme_latency_percentiles",
        r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
unit = "Seconds"
additional_groups = ["endpoint"]
aggregate = [
  { column = "response_time_ms", function = "MEDIAN", alias="p50"}
]

[[operations]]
type = "GroupBy"
columns = ["endpoint"]
aggregate = [
  { column = "response_time_ms", function = "MEAN" },
  { column = "response_time_ms", function = "MEDIAN" }
]

# Note: For P95 and P99, we'd typically use quantile functions
# which aren't directly represented in the current schema but
# could be added as extensions to AllowedGroupFunction
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "README 10  failed {}",
        result.err().unwrap()
    );
}

// 11. Request Method Distribution
#[test]
fn readme_request_method_distribution() {
    let config = setup_test_config(
        "readme_request_method_distribution",
        r#"
[[operations]]
type = "GroupBy"
columns = ["method"]

aggregate = [
  { column = "request_id", function = "COUNT", alias = "request_count" }
]

[[operations]]
type = "WithColumn"
name = "percentage"
expression = { type = "BinaryOp", left = { type = "BinaryOp", left = { type = "Column", value= "request_count" }, op = "MULTIPLY", right = { type = "Literal", value=100 } }, op = "DIVIDE", right = { type = "Function", name = {"SUM" = { column= "request_count" }} } }
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "README 11  failed {}",
        result.err().unwrap()
    );
}

// 12. Endpoint Availability/Success Rate
#[test]
fn readme_endpoint_availability_success_rate() {
    let config = setup_test_config(
        "readme_endpoint_availability_success_rate",
        r#"
[[operations]]
type = "WithColumn"
name = "is_success"
expression = { type = "BinaryOp", left = { type = "Column", value ="status_code" }, op = "LT", right = { type = "Literal", value = 400 } }

[[operations]]
type = "GroupBy"
columns = ["endpoint"]
aggregate = [
  { column = "request_id", function = "COUNT", alias = "request_id_COUNT" },
  { column = "is_success", function = "SUM", alias = "is_success_SUM" }
]

[[operations]]
type = "WithColumn"
name = "availability"
expression = { type = "BinaryOp", left = { type = "BinaryOp", left = { type = "Column", value = "is_success_SUM" }, op = "MULTIPLY", right = { type = "Literal",  value =100 } }, op = "DIVIDE", right = { type = "Column", value= "request_id_COUNT" } }
 "#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "README 12  failed {}",
        result.err().unwrap()
    );
}

// 13. Error Types Distribution
#[test]
fn readme_error_types_distribution() {
    let config = setup_test_config(
        "readme_error_types_distribution",
        r#"
[[operations]]
type = "Filter"
column = "status_code"
condition = "GTE"
filter = 400

[[operations]]
type = "GroupBy"
columns = ["error_type", "error_message"]
aggregate = [
  { column = "request_id", function = "COUNT", alias = "error_count" }
]

[[operations]]
type = "Sort"
column = "error_count"
order = "DESC"
limit = 10
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "README 13  failed {}",
        result.err().unwrap()
    );
}

// 14. Client/User Agent Analysis
#[test]
fn readme_client_user_agent_analysis() {
    let config = setup_test_config(
        "readme_client_user_agent_analysis",
        r#"

[[operations]]
type = "WithColumn"
name = "browser_type"
expression = { type = "Conditional", condition = { type = "Function", name = {"CONTAINS" = {column = "user_agent", value = "Chrome"}} }, then = { type = "Literal", value = "Chrome" }, otherwise = { type = "Conditional", condition = { type = "Function", name = {"CONTAINS" = {column = "user_agent", value = "Safari"}} }, then = { type = "Literal", value = "Safari" }, otherwise = { type = "Literal", value = "Other" } } }


[[operations]]
type = "GroupBy"
columns = ["browser_type"]
aggregate = [
  { column = "request_id", function = "COUNT", alias = "request_count" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "README 14  failed {}",
        result.err().unwrap()
    );
}

// 15. Response Size Analysis
#[test]
fn readme_response_size_analysis() {
    let config = setup_test_config(
        "readme_response_size_analysis",
        r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
unit = "Seconds"
aggregate = [
  { column = "response_size", function = "MEAN" , alias = "avg_response_size"},
  { column = "response_size", function = "MAX" , alias = "max_response_size"},
  { column = "response_size", function = "SUM" , alias = "total_bandwidth"}
]

"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "README 15  failed {}",
        result.err().unwrap()
    );
}

// 16. Anomaly Detection (Detecting Unusual Patterns)
#[test]
fn readme_anomaly_detection() {
    // im nto sure this is correct data
    let config = setup_test_config(
        "readme_anomaly_detection",
        r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 60
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
unit = "Seconds"
additional_groups = ["endpoint"]
aggregate = [
  { column = "request_id", function = "COUNT", alias = "request_id_COUNT" }
]

[[operations]]
name = "average_requests"
type = "Window"
column = "request_id_COUNT"
function = {type = "rollingmean"}
partition_by = ["endpoint"]
window_size = 30  # 30-minute rolling average
output_column = "average_requests"
order_by = ["endpoint", "timestamp"]

[[operations]]
type = "WithColumn"
name = "deviation"
expression = { type = "BinaryOp", left = { type = "Column", value ="request_id_COUNT" }, op = "SUBTRACT", right = { type = "Column",value= "average_requests" } }

"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "README 16  failed {}",
        result.err().unwrap()
    );
}
