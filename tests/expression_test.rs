mod test_utils;
/*
{"timestamp":"2023-04-01T00:01:35-07:00","request_id":"a006c36e-7925-464b-8c9a-17bc49bb31dd","service_name":"api-gateway",
"endpoint":"/v1/gateway","method":"PUT","status_code":302,"response_time_ms":170,"user_id":"user_142","client_ip":"us-201.98.52",
"user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124","request_size_bytes":578,
"response_size_bytes":988,"content_type":"text/html","is_error":false,"error_type":null,"geo_region":"us-east","has_external_call":true,
"external_service":"payment-gateway","external_endpoint":"/process","external_call_time_ms":39,"external_call_status":200,"db_query":null,
"db_name":null,"db_execution_time_ms":null,"cpu_utilization":83.48413,"memory_utilization":38.242134,"disk_io":54.512756,"network_io":149.16632}
*/

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
name = "constant_value"
expression = { type = "Literal", value = 42 }
[[operations]]
type = "Select"
columns = ["timestamp", "constant_value", "endpoint", "status_code", "response_time_ms", "geo_region"]
"#
);

config_string_test!(
    with_column_conditional_and_binaryop,
    r#"
[[operations]]
type = "WithColumn"
name = "is_success_and_fast"
 
[operations.expression]
type = "Conditional"
 
[operations.expression.condition]
type = "BinaryOp"
op = "AND"
 
[operations.expression.condition.left]
type = "BinaryOp"
op = "EQ"
 
[operations.expression.condition.left.left]
type = "Column"
value = "status_code"
 
[operations.expression.condition.left.right]
type = "Literal"
value = 200
 
[operations.expression.condition.right]
type = "BinaryOp"
op = "LT"
 
[operations.expression.condition.right.left]
type = "Column"
value = "response_time_ms"
 
[operations.expression.condition.right.right]
type = "Literal"
value = 100
 
[operations.expression.then]
type = "Literal"
value = true
 
[operations.expression.otherwise]
type = "Literal"
value = false
 
[[operations]]
type = "Select"
columns = ["timestamp", "status_code", "response_time_ms", "is_success_and_fast"]
"#
);

config_string_test!(
    with_column_nested_conditional,
    r#"
[[operations]]
type = "WithColumn"
name = "response_label"
 
[operations.expression]
type = "Conditional"
 
[operations.expression.condition]
type = "BinaryOp"
op = "EQ"
 
[operations.expression.condition.left]
type = "Column"
value = "status_code"
 
[operations.expression.condition.right]
type = "Literal"
value = 200
 
[operations.expression.then]
type = "Conditional"
 
[operations.expression.then.condition]
type = "BinaryOp"
op = "LT"
 
[operations.expression.then.condition.left]
type = "Column"
value = "response_time_ms"
 
[operations.expression.then.condition.right]
type = "Literal"
value = 50
 
[operations.expression.then.then]
type = "Literal"
value = "FAST_OK"
 
[operations.expression.then.otherwise]
type = "Literal"
value = "SLOW_OK"
 
[operations.expression.otherwise]
type = "Literal"
value = "NOT_OK"
 
[[operations]]
type = "Select"
columns = ["timestamp", "status_code", "response_time_ms", "response_label"]
"#
);

config_string_test!(
    with_column_complex_binaryop,
    r#"
[[operations]]
type = "WithColumn"
name = "is_extreme"
 
[operations.expression]
type = "BinaryOp"
op = "OR"
 
[operations.expression.left]
type = "BinaryOp"
op = "GT"
 
[operations.expression.left.left]
type = "Column"
value = "response_time_ms"
 
[operations.expression.left.right]
type = "Literal"
value = 50
 
[operations.expression.right]
type = "BinaryOp"
op = "EQ"
 
[operations.expression.right.left]
type = "Column"
value = "status_code"
 
[operations.expression.right.right]
type = "Literal"
value = 500
 
[[operations]]
type = "Select"
columns = ["timestamp", "status_code", "response_time_ms", "is_extreme"]
"#
);

config_string_test!(
    with_column_regex_match,
    r#"
[[operations]]
type = "WithColumn"
name = "is_api_endpoint"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.REGEXMATCH]
column = "endpoint"
pattern = "^/v1/.*"
 
[[operations]]
type = "Select"
columns = ["timestamp", "endpoint", "is_api_endpoint"]
"#
);

config_string_test!(
    with_column_contains,
    r#"
[[operations]]
type = "WithColumn"
name = "has_html"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.CONTAINS]
column = "content_type"
value = "html"
 
[[operations]]
type = "Select"
columns = ["timestamp", "content_type", "has_html"]
"#
);

config_string_test!(
    with_column_sqrt,
    r#"
[[operations]]
type = "WithColumn"
name = "sqrt_response_time"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.SQRT]
column = "response_time_ms"
 
[[operations]]
type = "Select"
columns = ["timestamp", "response_time_ms", "sqrt_response_time"]
"#
);

config_string_test!(
    with_column_minute,
    r#"
[[operations]]
type = "WithColumn"
name = "minute_of_request"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.MINUTE]
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
column = "timestamp"
 
[[operations]]
type = "Select"
columns = ["timestamp", "minute_of_request"]
"#
);

config_string_test!(
    with_column_hour,
    r#"
[[operations]]
type = "WithColumn"
name = "hour_of_request"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.HOUR]
column = "timestamp"
timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
 
[[operations]]
type = "Select"
columns = ["timestamp", "hour_of_request"]
"#
);
config_string_test!(
    with_column_substring,
    r#"
[[operations]]
type = "WithColumn"
name = "short_user_id"

[operations.expression]
type = "Function"

[operations.expression.name.SUBSTRING]
column = "user_id"
start = 0
length = 5

[[operations]]
type = "Select"
columns = ["user_id", "short_user_id"]
"#
);

config_string_test!(
    with_column_replace,
    r#"
[[operations]]
type = "WithColumn"
name = "endpoint_replaced"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.REPLACE]
column = "endpoint"
pattern = "/v1/"
replacement = "/api/"
literal = true
 
[[operations]]
type = "Select"
columns = ["endpoint", "endpoint_replaced"]
"#
);

config_string_test!(
    with_column_trim,
    r#"
[[operations]]
type = "WithColumn"
name = "trimmed_user_agent"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.TRIM]
column = "user_agent"
chars = " "
 
[[operations]]
type = "Select"
columns = ["user_agent", "trimmed_user_agent"]
"#
);

config_string_test!(
    with_column_toint,
    r#"
[[operations]]
type = "WithColumn"
name = "status_code_int"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.TOINT]
size = 32
column = "status_code"
 
[[operations]]
type = "Select"
columns = ["status_code", "status_code_int"]
"#
);

config_string_test!(
    with_column_concat,
    r#"
[[operations]]
type = "WithColumn"
name = "user_and_region"
 
[operations.expression]
type = "Function"
 
[operations.expression.name.CONCAT]
column1 = "user_id"
column2 = "geo_region"
 
[[operations]]
type = "Select"
columns = ["user_id", "geo_region", "user_and_region"]
"#
);
