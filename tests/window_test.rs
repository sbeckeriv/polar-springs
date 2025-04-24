mod test_utils;
use test_utils::parse_config_str;

config_string_test!(
    window_sum,
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
    window_avg,
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
    window_sum_bounds,
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
"#
);

config_string_test!(
    window_rank_desc,
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
"#
);

config_string_test!(
    window_lead_default,
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
"#
);
config_string_test!(
    window_min,
    r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "min_response_time"

[operations.function]
type = "min"
"#
);

config_string_test!(
    window_max,
    r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "max_response_time"

[operations.function]
type = "max"
"#
);

config_string_test!(
    window_count,
    r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "count_response_time"

[operations.function]
type = "count"
"#
);

config_string_test!(
    window_first_last,
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
"#
);

config_string_test!(
    window_dense_rank_row_number,
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
"#
);

config_string_test!(
    window_lag_lead_bounds_desc,
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
"#
);

config_string_test!(
    window_rollingmean,
    r#"
[[operations]]
type = "Window"
column = "response_time_ms"
partition_by = ["service_name"]
order_by = ["timestamp"]
name = "rollingmean_response_time"

[operations.function]
type = "rollingmean"
"#
);
