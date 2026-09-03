import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from profiling import count_nulls, dataset_summary, describe_numeric, distinct_values, run_profiling


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test-profiling").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame(
        [(1, 10.5, None, "US"), (2, 20.0, 1, "CA")],
        ["invoice_no", "amount", "customer_id", "Country"],
    )


def test_dataset_summary_returns_row_and_column_counts(sample_df):
    summary = dataset_summary(sample_df)

    assert summary["row_count"] == 2
    assert summary["column_count"] == 4
    assert summary["columns"][0]["name"] == "invoice_no"


def test_count_nulls_reports_each_column(sample_df):
    nulls = count_nulls(sample_df)

    assert any(item["column"] == "customer_id" and item["null_count"] == 1 for item in nulls)
    assert any(item["column"] == "amount" and item["null_count"] == 0 for item in nulls)


def test_describe_numeric_returns_supported_numeric_columns(sample_df):
    numeric_summary = describe_numeric(sample_df)

    assert "amount" in numeric_summary["numeric_columns"]
    assert "customer_id" in numeric_summary["numeric_columns"]
    assert numeric_summary["summary"][0]["metric"] == "count"


def test_distinct_values_returns_limited_values(sample_df):
    values = distinct_values(sample_df, "Country")

    assert values["column"] == "Country"
    assert values["value_count"] == 2
    assert set(values["values"]) == {"US", "CA"}


def test_run_profiling_returns_full_structure(sample_df):
    report = run_profiling(sample_df, "Country")

    assert set(report.keys()) == {"dataset_summary", "null_counts", "numeric_summary", "distinct_values"}
    assert report["dataset_summary"]["row_count"] == 2
    assert report["distinct_values"]["value_count"] == 2
