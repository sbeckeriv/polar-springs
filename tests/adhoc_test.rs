use polars_cli::runner::run;
use std::path::Path;
use std::sync::Once;

mod test_utils;
use test_utils::parse_config_str;

static START: Once = Once::new();

fn setup_test_logs() -> String {
    START.call_once(|| {
        tracing_subscriber::fmt::init();
    });
    let logs_path = Path::new("tests/request_logs.json");

    logs_path.to_str().unwrap().to_string()
}

// manual testing a config
#[test]
fn test_adhoc() {
    let config = r#"
[[operations]]
type = "Filter"
column = "status_code"
condition = "GTE"
filter = 400
"#;
    let input = setup_test_logs();

    let result = run(parse_config_str(config), input, "jsonl".to_string(), false);
    dbg!("Result: {:?}", &result);
    assert!(result.is_ok(), "Filter operation failed");
}
