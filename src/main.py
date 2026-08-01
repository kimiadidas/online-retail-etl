import config as config
import utils as utils
import profiling as profiling


spark = utils.create_spark_session(config.APP_NAME)

df = spark.read.csv(config.INPUT_PATH, header=True, inferSchema=True)

profiling.dataset_summary(df)
profiling.count_nulls(df)
profiling.describe_numeric(df)
profiling.distinct_values(df, "Country")

utils.get_logger().info("ETL process completed successfully.")