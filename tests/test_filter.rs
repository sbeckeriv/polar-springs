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

#[test]
fn test_filter_gte() {
    let config = setup_test_config(
        "filter_gte",
        r#"
[[operations]]
type = "Filter"
column = "timestamp"
condition = "GTE"
filter = "2023-04-01T00:00:00-07:00"
"#,
    );
    let input = setup_test_logs();

    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter gte operation failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_single_number() {
    let config = setup_test_config(
        "filter_single_number",
        r#"
[[operations]]
type = "Filter"
column = "status_code"
condition = "EQ"
filter = 200
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter single number failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_number_list() {
    todo!("support list");
    let config = setup_test_config(
        "filter_number_list",
        r#"
[[operations]]
type = "Filter"
column = "status_code"
condition = "IN"
filter = [200, 404]
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter number list failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_string_list() {
    todo!("support list");
    let config = setup_test_config(
        "filter_string_list",
        r#"
[[operations]]
type = "Filter"
column = "method"
condition = "IN"
filter = ["GET", "POST"]
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter string list failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_single_string() {
    let config = setup_test_config(
        "filter_single_string",
        r#"
[[operations]]
type = "Filter"
column = "method"
condition = "EQ"
filter = "GET"
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter single string failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_single_float() {
    let config = setup_test_config(
        "filter_single_float",
        r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "GTE"
filter = 0.5
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter single float failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_float_list() {
    todo!("support list");
    let config = setup_test_config(
        "filter_float_list",
        r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "IN"
filter = [0.1, 0.5, 1.0]
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter float list failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_boolean() {
    let config = setup_test_config(
        "filter_boolean",
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
    assert!(
        result.is_ok(),
        "filter boolean failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_date() {
    let config = setup_test_config(
        "filter_date",
        r#"
[[operations]]
type = "Filter"
column = "timestamp"
condition = "GTE"
filter = "2020-04-01"
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter date failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_datetime() {
    let config = setup_test_config(
        "filter_datetime",
        r#"
[[operations]]
type = "Filter"
column = "timestamp"
condition = "EQ"
filter = "2023-04-01T01:35Z"
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter datetime failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_lt() {
    let config = setup_test_config(
        "filter_lt",
        r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "LT"
filter = 1.0
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter less than failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_lte() {
    let config = setup_test_config(
        "filter_lte",
        r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "LTE"
filter = 1.0
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter less than or equal failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_gt() {
    let config = setup_test_config(
        "filter_gt",
        r#"
[[operations]]
type = "Filter"
column = "response_time_ms"
condition = "GT"
filter = 0.1
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter greater than failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_neq() {
    let config = setup_test_config(
        "filter_neq",
        r#"
[[operations]]
type = "Filter"
column = "method"
condition = "NEQ"
filter = "POST"
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter not equal failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_isnull() {
    let config = setup_test_config(
        "filter_isnull",
        r#"
[[operations]]
type = "Filter"
column = "error_type"
condition = "ISNULL"
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter isnull failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_isnotnull() {
    let config = setup_test_config(
        "filter_isnotnull",
        r#"
[[operations]]
type = "Filter"
column = "error_type"
condition = "ISNOTNULL"
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter isnotnull failed {}",
        result.err().unwrap()
    );
}

#[test]
fn test_filter_eqmissing() {
    let config = setup_test_config(
        "filter_eqmissing",
        r#"
[[operations]]
type = "Filter"
column = "external_service"
condition = "EQMISSING"
filter = "payment-gateway"
"#,
    );
    let input = setup_test_logs();
    let result = run(config, input);
    assert!(
        result.is_ok(),
        "filter eqmissing failed {}",
        result.err().unwrap()
    );
}
