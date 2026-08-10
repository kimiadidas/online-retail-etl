import profiling


def run_quality_checks(df, business_keys):
    row_count_result = check_row_count(df)
    null_columns_result = check_null_columns(df)
    duplicates_result = check_duplicates(df, business_keys)

    return {
        "row_count": row_count_result,
        "null_columns": null_columns_result,
        "duplicates": duplicates_result,
    }


def check_row_count(df):
    return {
        "status": "passed",
        "row_count": int(df.count()),
    }


def check_null_columns(df):
    null_counts = profiling.count_nulls(df)
    null_columns = [item for item in null_counts if item["null_count"] > 0]
    if null_columns:
        return {
            "status": "failed",
            "message": "Found null values in one or more columns.",
            "null_columns": null_columns,
        }
    return {
        "status": "passed",
        "message": "No null values found in any column.",
        "null_columns": [],
    }


def check_duplicates(df, business_keys):
    duplicate_count = df.filter("is_cancelled = false").groupBy(business_keys).count().filter("count > 1").count()
    if duplicate_count > 0:
        return {
            "status": "failed",
            "message": f"Found {duplicate_count} duplicate rows.",
            "duplicate_count": int(duplicate_count),
        }
    return {
        "status": "passed",
        "message": "No duplicate rows found.",
        "duplicate_count": 0,
    }