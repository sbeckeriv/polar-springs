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
    filter_gte,
    r#"
[[operations]]
type = "Filter"
column = "timestamp"
condition = "GTE"
filter = "2023-04-01T00:00:00-07:00"
"#
);

config_string_test!(
    filter_single_number,
    r#"
[[operations]]
type = "Filter"
column = "status_code"
condition = "EQ"
filter = 200
"#
);
config_string_test!(
    filter_single_string,
    r#"
[[operations]]
type = "Filter"
column = "method"
condition = "EQ"
filter = "GET"
"#
);

config_string_test!(
    filter_single_float,
    r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "GTE"
filter = 0.5
"#
);

config_string_test!(
    filter_boolean,
    r#"
[[operations]]
type = "Filter"
column = "is_error"
condition = "EQ"
filter = true
"#
);

config_string_test!(
    filter_date,
    r#"
[[operations]]
type = "Filter"
column = "timestamp"
condition = "GTE"
filter = "2020-04-01"
"#
);

config_string_test!(
    filter_datetime,
    r#"
[[operations]]
type = "Filter"
column = "timestamp"
condition = "EQ"
filter = "2023-04-01T01:35Z"
"#
);

config_string_test!(
    filter_lt,
    r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "LT"
filter = 1.0
"#
);

config_string_test!(
    filter_lte,
    r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "LTE"
filter = 1.0
"#
);

config_string_test!(
    filter_gt,
    r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "GT"
filter = 0.1
"#
);

config_string_test!(
    filter_neq,
    r#"
[[operations]]
type = "Filter"
column = "method"
condition = "NEQ"
filter = "POST"
"#
);

config_string_test!(
    filter_isnull,
    r#"
[[operations]]
type = "Filter"
column = "error_type"
condition = "ISNULL"
"#
);

config_string_test!(
    filter_isnotnull,
    r#"
[[operations]]
type = "Filter"
column = "error_type"
condition = "ISNOTNULL"
"#
);

config_string_test!(
    filter_eqmissing,
    r#"
[[operations]]
type = "Filter"
column = "external_service"
condition = "EQMISSING"
filter = "payment-gateway"
"#
);
