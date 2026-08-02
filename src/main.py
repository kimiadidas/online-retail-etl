import config
import quality
from schema import get_schema
import utils
import profiling
import transformations

logger = utils.get_logger()

logger.info("Starting ETL process")
spark = utils.create_spark_session(config.APP_NAME)

logger.info("Reading source data")
df = spark.read.csv(config.INPUT_PATH, header=True, schema=get_schema())

logger.info("Profiling source data")
logger.info(profiling.dataset_summary(df))
logger.info("Counting NULL values")
logger.info(profiling.count_nulls(df).collect())
logger.info("Generating numeric summary")
logger.info(profiling.describe_numeric(df).collect().limit(20))
logger.info(profiling.distinct_values(df, "Country"))

logger.info("Transforming data")
transformed_df = (
    df
    .transform(transformations.standardize_columns)
    .transform(transformations.handle_nulls)
    .transform(transformations.handle_invalid_data)
    .transform(transformations.convert_date)
    .transform(transformations.derived_columns)
)

logger.info("Profiling transformed data")
logger.info(profiling.dataset_summary(transformed_df))
logger.info("Counting NULL values")
logger.info(profiling.count_nulls(transformed_df).collect())
logger.info("Generating numeric summary")
logger.info(profiling.describe_numeric(transformed_df).collect().limit(20))
logger.info(profiling.distinct_values(transformed_df, "Country"))

logger.info("Data quality checks after transformations")
logger.info("Output rows")
logger.info(quality.check_row_count(transformed_df))
logger.info("NULL Value Checks")
logger.info(quality.check_null_columns(transformed_df))
logger.info("Duplicate Checks")
logger.info(quality.check_duplicates(transformed_df, config.BUSINESS_KEY_COLUMNS))

logger.info("Writing transformed data to output path")
transformed_df.write.mode("overwrite").parquet(config.OUTPUT_PATH)
logger.info("ETL process completed successfully")