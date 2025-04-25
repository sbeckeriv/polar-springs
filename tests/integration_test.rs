mod test_utils;

config_string_test!(
    filter_status_code,
    r#"
[[operations]]
type = "Filter"
column = "status_code"
condition = "GTE"
filter = 400
"#
);

config_string_test!(
    filter_error,
    r#"
[[operations]]
type = "Filter"
column = "is_error"
condition = "EQ"
filter = true
"#
);

config_string_test!(
    filter_not_null,
    r#"
[[operations]]
type = "Filter"
column = "error_type"
condition = "ISNOTNULL"
"#
);

config_string_test!(
    select_columns,
    r#"
[[operations]]
type = "Select"
columns = ["timestamp", "service_name", "endpoint", "status_code", "response_time_ms"]
"#
);

config_string_test!(
    group_by,
    r#"
[[operations]]
type = "GroupBy"
columns = ["service_name", "endpoint"]
aggregate = [
  { column = "response_time_ms", function = "MEAN" },
  { column = "status_code", function = "COUNT" }
]
"#
);

config_string_test!(
    group_by_time,
    r#"
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 1
unit = "Minutes"
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
output_column = "hour_bucket"
additional_groups = ["service_name"]
aggregate = [ { column = "response_time_ms", function = "MEAN" },  { column = "status_code", function = "COUNT" } ]
"#
);

config_string_test!(
    sort,
    r#"
[[operations]]
type = "Sort"
column = "response_time_ms"
order = "desc"
"#
);

config_string_test!(
    with_column_binary_op,
    r#"
[[operations]]
type = "WithColumn"
name = "is_slow_response"
expression = { type = "BinaryOp", left = { type = "Column", value = "response_time_ms" }, op = "GT", right = { type = "Literal", value = 100 } }

[[operations]]
type = "Select"
columns = ["timestamp", "is_slow_response",  "endpoint", "status_code", "response_time_ms", "geo_region"]
"#
);

config_string_test!(
    with_column_literal,
    r#"
[[operations]]
type = "WithColumn"
name = "total_processing_time"
expression = { type = "BinaryOp", left = { type = "Column", value = "response_time_ms" }, op = "ADD", right = { type = "Function", name = { ABS = { column = "external_call_time_ms" } } }} 

[[operations]]
type = "Select"
columns = ["timestamp", "total_processing_time",  "endpoint", "status_code", "response_time_ms", "external_call_time_ms"]
"#
);

config_string_test!(
    window_cumsum,
    r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "cumulative_response_time"

[operations.function]
type = "cumsum"
"#
);

config_string_test!(
    window_lag,
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
"#
);

config_string_test!(
    rename,
    r#"
[[operations]]
type = "Rename"
mappings = [
  { old_name = "timestamp", new_name = "timestamp_test" },
  { old_name = "status_code", new_name = "http_status" }
]
"#
);

config_string_test!(
    complex_workflow,
    r#"
[[operations]]
type = "Select"
columns = ["timestamp", "service_name", "endpoint", "status_code",  "response_time_ms", "geo_region"]

[[operations]]
type = "Filter"
column = "timestamp"
condition = "GTE"
filter = "2023-04-01T00:00:00-07:00"

[[operations]]
type = "GroupBy"
columns = [ "status_code","service_name", "geo_region"]
aggregate = [
  { column = "response_time_ms", function = "MEAN" },
  { column = "status_code", function = "COUNT", alias = "status_code_count" }
]

[[operations]]
type = "Sort"
column = "status_code_count"
order = "desc"

"#
);
