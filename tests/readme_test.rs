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
  { column = "response_time", function = "MEAN" },
  { column = "response_time", function = "MAX" },
  { column = "response_time", function = "MIN" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "response_time_MEAN", new_name = "avg_response_time" },
  { old_name = "response_time_MAX", new_name = "max_response_time" },
  { old_name = "response_time_MIN", new_name = "min_response_time" }
]

[[operations]]
type = "Sort"
column = "avg_response_time"
order = "DESC"
limit = 10
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 2 failed");
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
expression = { 
  type = "BinaryOp", 
  left = { type = "Column", "status_code" }, 
  op = "GTE", 
  right = { type = "Literal", 400 }
}

[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
unit = "Seconds"
aggregate = [
  { column = "request_id", function = "COUNT" },
  { column = "is_error", function = "SUM" }
]

[[operations]]
type = "WithColumn"
name = "error_rate"
expression = { 
  type = "BinaryOp", 
  left = { 
    type = "BinaryOp", 
    left = { type = "Column", "is_error_SUM" }, 
    op = "MULTIPLY", 
    right = { type = "Literal", 100.0 }
  }, 
  op = "DIVIDE", 
  right = { type = "Column", "request_id_COUNT" }
}
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 3 failed");
}

// 4. Status Code Distribution
#[test]
fn readme_status_code_distribution() {
    let config = setup_test_config(
        "readme_status_code_distribution",
        r#"
[[operations]]
type = "WithColumn"
name = "status_category"
expression = {
  type = "Function",
  name = "CONCAT",
  args = [
    {
      type = "Function",
      name = "ROUND",
      args = [
        {
          type = "BinaryOp",
          left = { type = "Column", "status_code" },
          op = "DIVIDE",
          right = { type = "Literal", 100 }
        }
      ]
    },
    { type = "Literal", "xx" }
  ]
}

[[operations]]
type = "GroupBy"
columns = ["status_category"]
aggregate = [
  { column = "request_id", function = "COUNT" }
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
    assert!(result.is_ok(), "README 4 failed");
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
  { column = "request_id", function = "COUNT" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "request_id_COUNT", new_name = "request_count" }
]

[[operations]]
type = "Sort"
column = "request_count"
order = "DESC"
limit = 10
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 5 failed");
}

// 6. P90 Response Time with Window Function
#[test]
fn readme_p90_response_time_with_window_function() {
    let config = setup_test_config(
        "readme_p90_response_time_with_window_function",
        r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 60
unit = "Seconds"
aggregate = [
  { column = "response_time", function = "MEAN" }
]

[[operations]]
type = "Window"
column = "response_time_MEAN"
function = "RollingMean"
window_size = 5
output_column = "p90_response_time"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 6 failed");
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
expression = {
  type = "Function",
  name = "DATEPART",
  args = [
    { type = "Literal", "hour" },
    { type = "Column", "timestamp" }
  ]
}

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
    assert!(result.is_ok(), "README 7 failed");
}

// 8. Throughput (Requests Per Second)
#[test]
fn readme_throughput_requests_per_second() {
    let config = setup_test_config(
        "readme_throughput_requests_per_second",
        r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 60
unit = "Seconds"
aggregate = [
  { column = "request_id", function = "COUNT" }
]

[[operations]]
type = "WithColumn"
name = "requests_per_second"
expression = { 
  type = "BinaryOp", 
  left = { type = "Column", "request_id_COUNT" }, 
  op = "DIVIDE", 
  right = { type = "Literal", 60 }
}
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 8 failed");
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
expression = {
  type = "Conditional",
  condition = {
    type = "BinaryOp",
    left = { type = "Column", "response_time" },
    op = "LTE",
    right = { type = "Literal", 300 }
  },
  then = { type = "Literal", 1 },
  otherwise = {
    type = "Conditional",
    condition = {
      type = "BinaryOp",
      left = { type = "Column", "response_time" },
      op = "LTE",
      right = { type = "Literal", 1200 }
    },
    then = { type = "Literal", 0.5 },
    otherwise = { type = "Literal", 0 }
  }
}

[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
unit = "Seconds"
aggregate = [
  { column = "satisfaction", function = "MEAN" },
  { column = "request_id", function = "COUNT" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "satisfaction_MEAN", new_name = "apdex_score" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 9 failed");
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
unit = "Seconds"
aggregate = [
  { column = "response_time", function = "MEDIAN" }
]

[[operations]]
type = "GroupBy"
columns = ["endpoint"]
aggregate = [
  { column = "response_time", function = "MEAN" },
  { column = "response_time", function = "MEDIAN" }
]

# Note: For P95 and P99, we'd typically use quantile functions
# which aren't directly represented in the current schema but
# could be added as extensions to AllowedGroupFunction
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 10 failed");
}

// 11. Request Method Distribution
#[test]
fn readme_request_method_distribution() {
    let config = setup_test_config(
        "readme_request_method_distribution",
        r#"
[[operations]]
type = "GroupBy"
columns = ["http_method"]
aggregate = [
  { column = "request_id", function = "COUNT" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "request_id_COUNT", new_name = "request_count" }
]

[[operations]]
type = "WithColumn"
name = "percentage"
expression = {
  type = "BinaryOp",
  left = {
    type = "BinaryOp",
    left = { type = "Column", "request_count" },
    op = "MULTIPLY",
    right = { type = "Literal", 100 }
  },
  op = "DIVIDE",
  right = {
    type = "Function",
    name = "SUM",
    args = [{ type = "Column", "request_count" }]
  }
}
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 11 failed");
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
expression = {
  type = "BinaryOp",
  left = { type = "Column", "status_code" },
  op = "LT",
  right = { type = "Literal", 400 }
}

[[operations]]
type = "GroupBy"
columns = ["endpoint"]
aggregate = [
  { column = "request_id", function = "COUNT" },
  { column = "is_success", function = "SUM" }
]

[[operations]]
type = "WithColumn"
name = "availability"
expression = {
  type = "BinaryOp",
  left = {
    type = "BinaryOp",
    left = { type = "Column", "is_success_SUM" },
    op = "MULTIPLY",
    right = { type = "Literal", 100 }
  },
  op = "DIVIDE",
  right = { type = "Column", "request_id_COUNT" }
}
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 12 failed");
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
  { column = "request_id", function = "COUNT" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "request_id_COUNT", new_name = "error_count" }
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
    assert!(result.is_ok(), "README 13 failed");
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
expression = {
  type = "Function",
  name = "CONCAT",
  args = [
    {
      type = "Conditional",
      condition = {
        type = "Function",
        name = "CONCAT",
        args = [
          { type = "Column", "user_agent" },
          { type = "Literal", "Chrome" }
        ]
      },
      then = { type = "Literal", "Chrome" },
      otherwise = {
        type = "Conditional",
        condition = {
          type = "Function",
          name = "CONCAT",
          args = [
            { type = "Column", "user_agent" },
            { type = "Literal", "Firefox" }
          ]
        },
        then = { type = "Literal", "Firefox" },
        otherwise = { type = "Literal", "Other" }
      }
    }
  ]
}

[[operations]]
type = "GroupBy"
columns = ["browser_type"]
aggregate = [
  { column = "request_id", function = "COUNT" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "request_id_COUNT", new_name = "request_count" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 14 failed");
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
unit = "Seconds"
aggregate = [
  { column = "response_size", function = "MEAN" },
  { column = "response_size", function = "MAX" },
  { column = "response_size", function = "SUM" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "response_size_MEAN", new_name = "avg_response_size" },
  { old_name = "response_size_MAX", new_name = "max_response_size" },
  { old_name = "response_size_SUM", new_name = "total_bandwidth" }
]
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 15 failed");
}

// 16. Anomaly Detection (Detecting Unusual Patterns)
#[test]
fn readme_anomaly_detection() {
    let config = setup_test_config(
        "readme_anomaly_detection",
        r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 60
unit = "Seconds"
aggregate = [
  { column = "request_id", function = "COUNT" }
]

[[operations]]
type = "Window"
column = "request_id_COUNT"
function = "RollingMean"
window_size = 30  # 30-minute rolling average
output_column = "average_requests"

[[operations]]
type = "WithColumn"
name = "deviation"
expression = {
  type = "BinaryOp",
  left = { type = "Column", "request_id_COUNT" },
  op = "SUBTRACT",
  right = { type = "Column", "average_requests" }
}
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(result.is_ok(), "README 16 failed");
}
