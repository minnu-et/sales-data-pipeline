
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, lower, current_date, when, regexp_replace, row_number
)
from pyspark.sql.window import Window


def transform_customer_data(df: DataFrame) -> DataFrame:
    """
    Silver Customer transformation (DIMENSION).

    Assumptions:
    - Bronze validation already done
    - ingestion_date already exists
    """

    # ----------------------------
    # 1. Standardization
    # ----------------------------
    df_clean = (
        df
        .withColumn("first_name", lower(trim(col("first_name"))))
        .withColumn("last_name", lower(trim(col("last_name"))))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("city", lower(trim(col("city"))))
        .withColumn("state", lower(trim(col("state"))))
    )

    # ----------------------------
    # 2. Phone cleanup (non-fatal)
    #    keep digits only; short phones -> NULL
    # ----------------------------
    df_phone = (
        df_clean
        .withColumn(
            "phone_number",
            regexp_replace(col("phone_number"), "[^0-9]", "")
        )
        .withColumn(
            "phone_number",
            when(col("phone_number").rlike("^[0-9]{10,}$"), col("phone_number"))
            .otherwise(None)
        )
    )

    # ----------------------------
    # 3. Date corrections (before validation)
    #    updated_date < created_date -> fix
    # ----------------------------
    df_dates = (
        df_phone
        .withColumn(
            "updated_date",
            when(col("updated_date") < col("created_date"), col("created_date"))
            .otherwise(col("updated_date"))
        )
    )

    # ----------------------------
    # 4. Silver validation gate (EXPLICIT)
    # ----------------------------
    silver_valid_condition = (
        col("customer_id").isNotNull() &
        col("email").isNotNull() &
        col("email").contains("@") &
        col("date_of_birth").isNotNull() &
        (col("date_of_birth") <= current_date())
    )

    silver_valid_df = df_dates.filter(silver_valid_condition)

    # ----------------------------
    # 5. Deduplication (latest record wins)
    # ----------------------------
    window_spec = (
        Window
        .partitionBy("customer_id")
        .orderBy(col("updated_date").desc(), col("ingestion_date").desc())
    )

    deduped_df = (
        silver_valid_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # ----------------------------
    # 6. Final schema (contract)
    # ----------------------------
    final_df = deduped_df.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "date_of_birth",
        "gender",
        "address",
        "city",
        "state",
        "pincode"
    )

    return final_df
