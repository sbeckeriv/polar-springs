# Config Structure

```mermaid
classDiagram
    Config "1" --> "*" Operation
    Operation <|-- Filter
    Operation <|-- Select
    Operation <|-- GroupBy
    Operation <|-- GroupByTime
    Operation <|-- Sort
    Operation <|-- SelfJoin
    Operation <|-- WithColumn
    Operation <|-- Window
    Operation <|-- Rename

    Filter --> AllowedFilterCondition
    Filter --> FilterField
    GroupBy --> Aggregate
    GroupByTime --> Aggregate
    GroupByTime --> TimestampFormat
    GroupByTime --> TimeUnit
    GroupByTime --> TimeUnitPrecision
    Sort --> SortOrder
    SelfJoin --> JoinType
    WithColumn --> Expression
    Window --> WindowFunction
    Window --> WindowBound
    Rename --> ColumnRename
    Aggregate --> AllowedGroupFunction
    Expression --> LiteralValue
    Expression --> ExpressionFunction
    Expression --> ExpressionOperation

    class Config {
        operations: Vec~Operation~
    }
    class Filter {
        column: String
        condition: AllowedFilterCondition
        filter: Option~FilterField~
    }
    class AllowedFilterCondition {
        EQ, EQMISSING, NEQ, LT, LTE, GT, GTE, ISNULL, ISNOTNULL
    }
    class FilterField {
        SingleNumber(i64)
        NumberList(Vec~i64~)
        StringList(Vec~String~)
        SingleString(String)
        SingleFloat(f64)
        FloatList(Vec~f64~)
        Boolean(bool)
        Date(NaiveDate)
        DateTime(DateTime~Utc~)
    }
    class Select {
        columns: Vec~String~
    }
    class GroupBy {
        columns: Vec~String~
        aggregate: Vec~Aggregate~
    }
    class GroupByTime {
        time_column: String
        every: u32
        unit: TimeUnit
        output_column: Option~String~
        timestamp_format: TimestampFormat
        additional_groups: Vec~String~
        aggregate: Vec~Aggregate~
    }
    class TimestampFormat {
        timestamp_format: String
        timestamp_timezone: Option~String~
        precision: Option~TimeUnitPrecision~
    }
    class TimeUnit {
        Seconds, Minutes, Hours, Days, Weeks, Months, Quarters, Years
    }
    class TimeUnitPrecision {
        Nanoseconds, Microseconds, Milliseconds
    }
    class Aggregate {
        column: String
        alias: Option~String~
        function: AllowedGroupFunction
    }
    class AllowedGroupFunction {
        MIN, MAX, SUM, MEAN, MEDIAN, STD(u8), VAR(u8), COUNT, FIRST, LAST, NUNIQUE, PERCENTILE(f64)
    }
    class Sort {
        column: String
        order: String
        limit: Option~u32~
    }
    class SelfJoin {
        left_on: Vec~String~
        right_on: Vec~String~
        how: JoinType
    }
    class JoinType {
        Inner, Left, Right, Cross, Semi, Anti
    }
    class WithColumn {
        name: Option~String~
        expression: Expression
    }
    class Expression {
        Column, Literal, BinaryOp, Function, Conditional
    }
    class LiteralValue {
        String, Integer, Float, Boolean, Null, Date, DateTime, StringList, IntegerList, FloatList
    }
    class ExpressionFunction {
        PERCENTILE, CONCAT, LOWER, UPPER, DATEPART, SUM, ABS, ROUND, TOINT, TRIM, REPLACE, SUBSTRING,
        ISNULL, ISNOTNULL, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, FLOOR, CEIL, SQRT, CONTAINS, REGEXMATCH
    }
    class ExpressionOperation {
        ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO, CONCAT, AND, OR, EQ, NEQ, LT, LTE, GT, GTE
    }
    class Window {
        column: String
        function: WindowFunction
        partition_by: Vec~String~
        order_by: Vec~String~
        descending: Vec~bool~
        bounds: Option~WindowBound~
        name: String
    }
    class WindowFunction {
        Sum, Min, Max, Mean, Count, First, Last, Rank, DenseRank, RowNumber, CumSum, Lag, Lead, RollingMean
    }
    class WindowBound {
        preceding: Option~usize~
        following: Option~usize~
    }
    class Rename {
        mappings: Vec~ColumnRename~
    }
    class ColumnRename {
        old_name: String
        new_name: String
    }