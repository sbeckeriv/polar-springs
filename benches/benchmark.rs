use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polars_cli::configs::parse::parse_config;
use polars_cli::runner::{dataframe_from_file, process_dataframe};

fn bench_process_dataframe(c: &mut Criterion) {
    let s = r#"
[[input]]
type = "Jsonl"
path = "expanded_big_test.json"

[[operations]]
type = "WithColumn"
name = "total_processing_time"
expression = { type = "BinaryOp", left = { type = "Column", value = "response_time_ms" }, op = "ADD", right = { type = "Function", name = { ABS = { column = "external_call_time_ms" } } }} 

[[operations]]
type = "Select"
columns = ["timestamp", "total_processing_time",  "endpoint", "status_code", "response_time_ms", "external_call_time_ms"]
"#;
    let config = parse_config(s);

    let df = dataframe_from_file(&config)
        .expect("Failed to read file: expaned_big_test.json run `just` to generate it");
    c.bench_function("process_dataframe", |b| {
        b.iter(|| {
            let result = process_dataframe(black_box(df.clone()), black_box(&config));
            let _ = criterion::black_box(result).expect("msg");
        })
    });
}

criterion_group!(benches, bench_process_dataframe);
criterion_main!(benches);
