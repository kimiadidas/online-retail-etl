from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when


def run_profiling(df: DataFrame, column_name: str) -> dict:
    profiling_results = {
        "dataset_summary": dataset_summary(df),
        "null_counts": count_nulls(df),
        "numeric_summary": describe_numeric(df),
        "distinct_values": distinct_values(df, column_name)
    }
    return profiling_results


def dataset_summary(df: DataFrame) -> dict:
    return {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": [
            {"name": col_name, "dtype": dtype}
            for col_name, dtype in df.dtypes
        ],
    }


def count_nulls(df: DataFrame) -> list:
    rows = df.select(
        [
            count(
                when(col(column).isNull(), column)
            ).alias(column)
            for column in df.columns
        ]
    ).collect()

    if not rows:
        return []

    row = rows[0]
    return [
        {"column": column, "null_count": int(row[column])}
        for column in df.columns
    ]


def describe_numeric(df: DataFrame) -> dict:
    numeric_columns = [col_name for col_name, dtype in df.dtypes if dtype in ["int", "double", "float", "long", "decimal", "bigint"]]
    if not numeric_columns:
        return {"numeric_columns": [], "summary": []}

    summary_rows = df.select(numeric_columns).describe().collect()
    return {
        "numeric_columns": numeric_columns,
        "summary": [
            {"metric": row[0], **{column: row[index] for index, column in enumerate(numeric_columns, start=1)}}
            for row in summary_rows
        ],
    }


def distinct_values(df: DataFrame, column_name: str) -> dict:
    distinct_values = df.select(column_name).distinct().limit(20).collect()
    values = [str(row[column_name]) for row in distinct_values]
    return {
        "column": column_name,
        "value_count": len(values),
        "values": values,
    }