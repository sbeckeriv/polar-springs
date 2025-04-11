
## 1. Request Count Over Time

```toml
[[operations]]
type = "Filter"
column = "type"
condition = "EQ"
filter = "request"

[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 60
unit = "Seconds"
aggregate = [
  { column = "request_id", function = "COUNT" }
]

[[operations]]
type = "Rename"
mappings = [
  { old_name = "request_id_COUNT", new_name = "request_count" }
]
```

## 2. Average Response Time by Endpoint

```toml
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
```

## 3. Error Rate Over Time

```toml
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
```

## 4. Status Code Distribution

```toml
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
```

## 5. Top Endpoints by Request Volume

```toml
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
```

## 6. P90 Response Time with Window Function

```toml
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
```

## 7. Latency Heatmap by Hour and Endpoint

```toml
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
```

## 8. Throughput (Requests Per Second)

```toml
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
```

## 9. Apdex Score (Application Performance Index)

```toml
[[operations]]
type = "WithColumn"
name = "satisfaction"
expression = {
  type = "Conditional",
  condition = {
    type = "BinaryOp",
    left = { type = "Column", "response_time" },
    op = "LTE",
    right = { type = "Literal", 300 }  # Satisfied threshold (300ms)
  },
  then = { type = "Literal", 1 },
  otherwise = {
    type = "Conditional",
    condition = {
      type = "BinaryOp",
      left = { type = "Column", "response_time" },
      op = "LTE",
      right = { type = "Literal", 1200 }  # Tolerating threshold (1.2s)
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
```

## 10. Latency Percentiles (P50, P95, P99)

```toml
[[operations]]
type = "GroupByTime"
time_column = "timestamp"
every = 300
unit = "Seconds"
aggregate = [
  { column = "response_time", function = "MEDIAN" }  # P50
]

[[operations]]
type = "GroupBy"
columns = ["endpoint"]
aggregate = [
  { column = "response_time", function = "MEAN" },
  { column = "response_time", function = "MEDIAN" }  # P50
]

# Note: For P95 and P99, we'd typically use quantile functions
# which aren't directly represented in the current schema but
# could be added as extensions to AllowedGroupFunction
```

## 11. Request Method Distribution

```toml
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
```

## 12. Endpoint Availability/Success Rate

```toml
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
```

## 13. Error Types Distribution

```toml
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
```

## 14. Client/User Agent Analysis

```toml
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
```

## 15. Response Size Analysis

```toml
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
```

## 16. Anomaly Detection (Detecting Unusual Patterns)

```toml
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

[[operations]]
type = "WithColumn"
name = "is_anomaly"
expression = {
  type = "BinaryOp",
  left = {
    type = "Function",
    name = "ABS",
    args = [{ type = "Column", "deviation" }]
  },
  op = "GT",
  right = {
    type = "BinaryOp",
    left = { type = "Column", "average_requests" },
    op = "MULTIPLY",
    right = { type = "Literal", 0.3 }  # 30% threshold
  }
}
```