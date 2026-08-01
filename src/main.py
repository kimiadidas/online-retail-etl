import config as config
import utils as utils
import profiling as profiling

logger = utils.get_logger()

logger.info("Starting ETL process")
spark = utils.create_spark_session(config.APP_NAME)

logger.info("Reading source data")
df = spark.read.csv(config.INPUT_PATH, header=True, inferSchema=True)

logger.info(profiling.dataset_summary(df))

logger.info("Counting NULL values")
logger.info(profiling.count_nulls(df).collect())

logger.info("Generating numeric summary")
logger.info(profiling.describe_numeric(df).collect())

logger.info(profiling.distinct_values(df, "Country"))

logger.info("ETL process completed successfully")