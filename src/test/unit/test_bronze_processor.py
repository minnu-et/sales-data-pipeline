import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from main.transformations.bronze_processor import process_bronze_layer


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("test") \
        .config("spark.sql.codegen.wholeStage", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


@pytest.fixture
def sample_df(spark):
    data = [
        (1, "Alice", "alice@email.com"),
        (None, "Bob", "bob@email.com"),
        (3, None, "carol@email.com"),
    ]
    return spark.createDataFrame(data, ["customer_id", "name", "email"])


@pytest.fixture
def rules():
    return {
        "NULL_CUSTOMER_ID": {"condition": col("customer_id").isNotNull()},
        "NULL_NAME":        {"condition": col("name").isNotNull()},
    }


def test_valid_records_are_separated(sample_df, rules):
    valid_df, _ = process_bronze_layer(sample_df, rules, "customer")
    assert valid_df.count() == 1


def test_invalid_records_are_rejected(sample_df, rules):
    _, rejected_df = process_bronze_layer(sample_df, rules, "customer")
    assert rejected_df.count() == 2


def test_rejection_reason_is_correct(sample_df, rules):
    _, rejected_df = process_bronze_layer(sample_df, rules, "customer")
    reasons = {row["rejection_reason"] for row in rejected_df.select("rejection_reason").collect()}
    assert "NULL_CUSTOMER_ID" in reasons
    assert "NULL_NAME" in reasons
