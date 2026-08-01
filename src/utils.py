from pyspark.sql import SparkSession
import logging

def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def get_logger():
    logger = logging.getLogger("online-retail-etl")

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # Console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File
    file_handler = logging.FileHandler("logs/app.log")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger