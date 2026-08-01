from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, sum

def dataset_summary(df: DataFrame) -> str:
    summary = f"rows: {df.count()}\n"
    summary += f"columns: {len(df.columns)}\n"
    summary += "column names and types: \n" + "\n".join([f"{col_name}: {dtype}" for col_name, dtype in df.dtypes]) + "\n"
    return summary

def count_nulls(df: DataFrame) -> DataFrame:
    return df.select(
        [
            count(
                when(col(column).isNull(), column)
            ).alias(column)
            for column in df.columns
        ]
    )

def describe_numeric(df: DataFrame) -> DataFrame:
    numeric_columns = [col_name for col_name, dtype in df.dtypes if dtype in ["int", "double", "float"]]
    if not numeric_columns:
        return "No numeric columns found."
    return df.select(numeric_columns).describe()

def distinct_values(df: DataFrame, column_name: str) -> str:
    distinct_values = df.select(column_name).distinct()
    return f"Distinct values in column '{column_name}':\n" + "\n".join([str(row[column_name]) for row in distinct_values.collect()])