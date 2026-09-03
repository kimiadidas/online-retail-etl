import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from quality import check_duplicates, check_null_columns, check_row_count, run_quality_checks


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test-quality").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame(
        [(1, 10.5, None, "US"), (2, 20.0, 1, "CA")],
        ["invoice_no", "amount", "customer_id", "Country"],
    )


@pytest.fixture
def duplicate_df(spark):
    return spark.createDataFrame(
        [(1, "A", False), (1, "A", False), (2, "B", False)],
        ["invoice_no", "stock_code", "is_cancelled"],
    )


def test_row_count_check_returns_passed_status(sample_df):
    result = check_row_count(sample_df)

    assert result["status"] == "passed"
    assert result["row_count"] == 2


def test_null_column_check_flags_nulls(sample_df):
    result = check_null_columns(sample_df)

    assert result["status"] == "failed"
    assert result["null_columns"][0]["column"] == "customer_id"


def test_duplicates_check_flags_duplicate_keys(duplicate_df):
    result = check_duplicates(duplicate_df, ["invoice_no", "stock_code"])

    assert result["status"] == "failed"
    assert result["duplicate_count"] == 1


def test_run_quality_checks_returns_all_checks(duplicate_df):
    result = run_quality_checks(duplicate_df, ["invoice_no", "stock_code"])

    assert set(result.keys()) == {"row_count", "null_columns", "duplicates"}
    assert result["duplicates"]["status"] == "failed"
