use polars_cli::runner::run;
use std::path::Path;
use std::sync::Once;

mod test_utils;

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
    let mut config = polars_cli::configs::parse::parse_config(config);
    let input_config = polars_cli::configs::input::InputConfig::new(&input, "jsonl", false, false);
    config.input = Some(input_config);

    let result = run(&config);
    let result = result.and_then(|df| df.collect().map_err(polars_cli::runner::RunnerError::from));
    dbg!("Result: {:?}", &result);
    assert!(result.is_ok(), "Filter operation failed");
}
