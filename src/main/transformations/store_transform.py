
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, lower, current_date, when, row_number
)
from pyspark.sql.window import Window


def transform_store_data(df: DataFrame) -> DataFrame:
    """
    Silver Store transformation (DIMENSION).

    Assumptions:
    - Bronze validation already done
    """

    # ----------------------------
    # 1. Standardization
    # ----------------------------
    df_clean = (
        df
        .withColumn("store_name", lower(trim(col("store_name"))))
        .withColumn("city", lower(trim(col("city"))))
        .withColumn("state", lower(trim(col("state"))))
        .withColumn("store_manager_name", lower(trim(col("store_manager_name"))))
    )

    # ----------------------------
    # 2. Date corrections
    # ----------------------------
    df_dates_fixed = (
        df_clean
        .withColumn(
            "store_closing_date",
            when(
                col("store_closing_date") < col("store_opening_date"),
                None
            ).otherwise(col("store_closing_date"))
        )
    )

    # ----------------------------
    # 3. Derived column: is_active
    # ----------------------------
    df_enriched = (
        df_dates_fixed
        .withColumn(
            "is_active",
            when(
                col("store_closing_date").isNull()
                | (col("store_closing_date") >= current_date()),
                True
            ).otherwise(False)
        )
    )

    # ----------------------------
    # 4. Silver validation gate
    # ----------------------------
    silver_valid_condition = (
        col("store_id").isNotNull() &
        col("store_name").isNotNull() &
        col("store_opening_date").isNotNull() &
        (col("store_opening_date") <= current_date())
    )

    silver_valid_df = df_enriched.filter(silver_valid_condition)

    # ----------------------------
    # 5. Deduplication (latest record per store)
    # ----------------------------
    window_spec = (
        Window
        .partitionBy("store_id")
        .orderBy(col("ingestion_date").desc())
    )

    deduped_df = (
        silver_valid_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # ----------------------------
    # 6. Final schema lock
    # ----------------------------
    final_df = deduped_df.select(
        "store_id",
        "store_name",
        "address",
        "city",
        "state",
        "pincode",
        "store_manager_name",
        "store_opening_date",
        "store_closing_date",
        "is_active"
    )

    return final_df
