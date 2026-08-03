from pyspark.sql.functions import col

import profiling

def run_quality_checks(df, business_keys):
    row_count_result = check_row_count(df)
    null_columns_result = check_null_columns(df)
    duplicates_result = check_duplicates(df, business_keys)

    return {
        "row_count": row_count_result,
        "null_columns": null_columns_result,
        "duplicates": duplicates_result
    }

def check_row_count(df):
    return df.count()

def check_null_columns(df):
    null_counts = profiling.count_nulls(df)
    for value in null_counts.first().asDict().values():
        if value > 0:
            return "Found null values in one or more columns."
    return "No null values found in any column."

def check_duplicates(df, business_keys):
    duplicate_count = df.filter("is_cancelled = false").groupBy(business_keys).count().filter("count > 1").count()
    if duplicate_count > 0:
        return f"Found {duplicate_count} duplicate rows."
    return "No duplicate rows found."