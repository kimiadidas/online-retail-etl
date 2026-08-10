import config
from quality import run_quality_checks
from schema import get_schema
import utils
from profiling import run_profiling
from transformations import run_transformations
from helper import run_step
from reports import write_json_report

logger = utils.get_logger()

logger.info("Starting ETL process")

spark = run_step("Creating Spark session", lambda: utils.create_spark_session(config.APP_NAME))

df = run_step("Reading source data", lambda: spark.read.csv(config.INPUT_PATH, header=True, schema=get_schema()))

source_profiling_results = run_step("Profiling source data", lambda: run_profiling(df, "Country"))
for profile_name, result in source_profiling_results.items():
    logger.info("%s: %s", profile_name.replace("_", " ").capitalize(), result)

transformed_df = run_step("Transforming data", lambda: df.transform(run_transformations))

transformed_profiling_results = run_step("Profiling transformed data", lambda: run_profiling(transformed_df, "Country"))
for profile_name, result in transformed_profiling_results.items():
    logger.info("%s: %s", profile_name.replace("_", " ").capitalize(), result)

quality_results = run_step("Running data quality checks", lambda: run_quality_checks(transformed_df, config.BUSINESS_KEY_COLUMNS))
for check_name, result in quality_results.items():
    logger.info("%s: %s", check_name.replace("_", " ").capitalize(), result)

report = {
    "source_profiling": source_profiling_results,
    "transformed_profiling": transformed_profiling_results,
    "quality_checks": quality_results,
}

run_step("Writing quality and profiling reports", lambda: write_json_report("data/curated/reports/etl_report.json", report))
run_step("Writing transformed data", lambda: transformed_df.write.mode("overwrite").parquet(config.OUTPUT_PATH))

logger.info("ETL process completed successfully")