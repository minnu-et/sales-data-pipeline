from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
# SparkSession → type clarity (interview-friendly)
# DataFrame → makes function intent explicit


def read_csv(spark: SparkSession, file_path: str, filter_condition: str = None) -> DataFrame:
    """
    Reads a CSV file into a Spark DataFrame with optional incremental filtering.

    Args:
        spark: Active SparkSession
        file_path: Path to the CSV file (local or S3)
        filter_condition: Optional SQL WHERE clause for incremental reads
                         Example: "sales_date > '2026-02-04 23:59:59'"

    Returns:
        Spark DataFrame

    Usage:
        # Full refresh (reads all data)
        df = read_csv(spark, "data.csv")

        # Incremental read (only new data)
        df = read_csv(spark, "data.csv", filter_condition="sales_date > '2026-02-04'")
    """
    # Read the CSV file
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

    # Apply incremental filter if provided
    if filter_condition:
        df = df.filter(filter_condition)
        # Note: filter is applied after read because CSV files don't support
        # predicate pushdown. For better performance, consider using parquet
        # or Delta Lake which support filter pushdown at storage level.

    return df
