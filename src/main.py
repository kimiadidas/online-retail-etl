import config as config
import utils as utils
import profiling as profiling

spark = utils.create_spark_session(config.APP_NAME)

df = spark.read.csv(config.INPUT_PATH, header=True, inferSchema=True)

print(profiling.dataset_summary(df))

print(profiling.count_nulls(df))

print(profiling.describe_numeric(df))

print(profiling.distinct_values(df, "Country"))