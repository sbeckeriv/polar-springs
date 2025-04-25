mod test_utils;

config_string_test!(
    readme_request_count_over_time,
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
"#
);

config_string_test!(
    readme_avg_response_time_by_endpoint,
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
"#
);

config_string_test!(
    readme_error_rate_over_time,
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
"#
);

config_string_test!(
    readme_status_code_distribution,
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
"#
);

config_string_test!(
    readme_top_endpoints_by_request_volume,
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
"#
);

config_string_test!(
    readme_p90_response_time_with_window_function,
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
"#
);

config_string_test!(
    readme_throughput_requests_per_second,
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
"#
);

config_string_test!(
    readme_apdex_score,
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
"#
);

config_string_test!(
    readme_latency_percentiles,
    r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
unit = "Seconds"
additional_groups = ["endpoint"]
aggregate = [
  { column = "response_time_ms", function = "MEDIAN", alias="p50"},
  { column = "response_time_ms", function = {"PERCENTILE" =  0.99}, alias="p99"},
  { column = "response_time_ms", function = {"PERCENTILE" = 0.95}, alias="p95"},
  { column = "request_id", function = "COUNT", alias = "request_count" }
]
"#
);

config_string_test!(
    readme_request_method_distribution,
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
"#
);

config_string_test!(
    readme_endpoint_availability_success_rate,
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
"#
);

config_string_test!(
    readme_error_types_distribution,
    r#"
[[operations]]
type = "Filter"
column = "is_error"
condition = "EQ"
filter = true

[[operations]]
type = "GroupBy"
columns = ["error_type"]
aggregate = [
  { column = "request_id", function = "COUNT", alias = "error_count" }
]

[[operations]]
type = "Sort"
column = "error_count"
order = "DESC"
limit = 10
"#
);

config_string_test!(
    readme_client_user_agent_analysis,
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
"#
);

config_string_test!(
    readme_response_size_analysis,
    r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
unit = "Seconds"
aggregate = [
  { column = "request_size_bytes", function = "MEAN" , alias = "avg_response_size"},
  { column = "request_size_bytes", function = "MAX" , alias = "max_response_size"},
  { column = "request_size_bytes", function = "SUM" , alias = "total_bandwidth"}
]
"#
);

config_string_test!(
    readme_anomaly_detection,
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
"#
);
