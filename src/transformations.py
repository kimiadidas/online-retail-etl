from pyspark.sql.functions import col, month, to_timestamp, when, year

def standardize_columns(df):
    return (
        df
        .withColumnRenamed("Invoice", "invoice_no")
        .withColumnRenamed("StockCode", "stock_code")
        .withColumnRenamed("Description", "description")
        .withColumnRenamed("Quantity", "quantity")
        .withColumnRenamed("InvoiceDate", "invoice_date")
        .withColumnRenamed("Price", "unit_price")
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Country", "country")
    )

def handle_nulls(df):
    return (
        df
        .filter(col("description").isNotNull())
        .fillna({"customer_id": -1, "country": "Unknown"})
    )

def handle_invalid_data(df):
    return (
        df
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
    )

def convert_date(df):
    return (
        df
        .withColumn("invoice_date", to_timestamp(col("invoice_date"), "M/d/yy H:mm"))
    )

def derived_columns(df):
    return (
        df
        .withColumn("total_price", col("quantity") * col("unit_price"))
        .withColumn("is_cancelled", when(col("invoice_no").startswith("C"), True).otherwise(False))
        .withColumn("invoice_year", year(col("invoice_date")))
        .withColumn("invoice_month", month(col("invoice_date")))
    )