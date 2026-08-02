from pyspark.sql.types import *

def get_schema() -> StructType:
    return StructType([
        StructField("Invoice", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("Price", DecimalType(10,2), True),
        StructField("Customer ID", IntegerType(), True),
        StructField("Country", StringType(), True)
    ])