import config as config
import utils as utils
import profiling as profiling

logger = utils.get_logger()

logger.info("Starting ETL process")
spark = utils.create_spark_session(config.APP_NAME)

logger.info("Reading source data")
df = spark.read.csv(config.INPUT_PATH, header=True, inferSchema=True)

profiling.dataset_summary(df)

logger.info("Counting NULL values")
profiling.count_nulls(df)

logger.info("Generating numeric summary")
profiling.describe_numeric(df)

profiling.distinct_values(df, "Country")

logger.info("ETL process completed successfully")