from pyspark.sql.functions import col

def check_row_count(df):
    return df.count()

def check_null_columns(df):
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            return f"Column '{column}' has {null_count} NULL values."
    return "No null values found in any column."

def check_duplicates(df):
    duplicate_count = df.groupBy(
        "invoice_no","stock_code","customer_id"
    ).count().filter("count > 1").count()
    if duplicate_count > 0:
        return f"Found {duplicate_count} duplicate rows."
    return "No duplicate rows found."