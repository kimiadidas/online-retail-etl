import config
from quality import run_quality_checks
from schema import get_schema
import utils
from profiling import run_profiling
from transformations import run_transformations

logger = utils.get_logger()

logger.info("Starting ETL process")
spark = utils.create_spark_session(config.APP_NAME)

logger.info("Reading source data")
df = spark.read.csv(config.INPUT_PATH, header=True, schema=get_schema())

logger.info("Profiling source data")
source_profiling_results = run_profiling(df, "Country")
for profile_name, result in source_profiling_results.items():
    logger.info("%s: %s", profile_name.replace("_", " ").capitalize(), result)

logger.info("Transforming data")
transformed_df = df.transform(run_transformations)


logger.info("Profiling transformed data")
transformed_profiling_results = run_profiling(transformed_df, "Country")
for profile_name, result in transformed_profiling_results.items():
    logger.info("%s: %s", profile_name.replace("_", " ").capitalize(), result)


logger.info("Data quality checks after transformations")
quality_results = run_quality_checks(transformed_df, config.BUSINESS_KEY_COLUMNS)
for check_name, result in quality_results.items():
    logger.info("%s: %s", check_name.replace("_", " ").capitalize(), result)


logger.info("Writing transformed data to output path")
transformed_df.write.mode("overwrite").parquet(config.OUTPUT_PATH)
logger.info("ETL process completed successfully")