from pyspark.sql.functions import col, count

def dataset_summary(df: "pyspark.sql.DataFrame") -> str:
    summary = f"rows: {df.count()}\n"
    summary += f"columns: {len(df.columns)}\n"
    summary += "column names and types: \n" + "\n".join([f"{col_name}: {dtype}" for col_name, dtype in df.dtypes]) + "\n"
    return summary

def count_nulls(df: "pyspark.sql.DataFrame") -> str:
    null_counts = {column: df.filter(col(column).isNull()).count() for column in df.columns}
    null_counts_str = "Null counts per column:\n"
    null_counts_str += "\n".join([f"{column}: {count}" for column, count in null_counts.items()])
    return null_counts_str

def describe_numeric(df: "pyspark.sql.DataFrame") -> str:
    numeric_columns = [col_name for col_name, dtype in df.dtypes if dtype in ["int", "double", "float"]]
    if not numeric_columns:
        return "No numeric columns found."
    
    description = df.select(numeric_columns).describe().toPandas()
    return description.to_string(index=False)

def distinct_values(df: "pyspark.sql.DataFrame", column_name: str) -> str:
    distinct_values = df.select(column_name).distinct()
    return f"Distinct values in column '{column_name}':\n" + "\n".join([str(row[column_name]) for row in distinct_values.collect()])