
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, lower, year, month
)


def transform_sales_data(sales_df: DataFrame, product_df: DataFrame) -> DataFrame:
    """
    Silver layer transformation for Sales:
    - Cleans and standardizes sales data
    - Resolves product_id via Product dimension
    - Enforces Silver-level data quality
    - Produces fact-ready schema
    """

    # ----------------------------
    # 1. Standardization / Cleaning
    # ----------------------------
    df_clean = (
        sales_df
        .withColumn("product_name", lower(trim(col("product_name"))))
        .withColumn("price", col("price").cast("double"))
        .withColumn("quantity", col("quantity").cast("int"))
    )

    # ----------------------------
    # 2. Resolve product_id (KEY STEP)
    # ----------------------------
    product_lookup_df = (
        product_df
        .select("product_id", "product_name")
        .withColumn("product_name", lower(trim(col("product_name"))))
    )

    df_with_product = (
        df_clean
        .join(
            product_lookup_df,
            on="product_name",
            how="inner"
        )
    )

    # ----------------------------
    # 3. Business enrichment
    # ----------------------------
    df_enriched = (
        df_with_product
        .withColumn("total_cost", col("price") * col("quantity"))
        .withColumn("sales_year", year(col("sales_date")))
        .withColumn("sales_month", month(col("sales_date")))
    )

    # ----------------------------
    # 4. Silver validation gate
    # ----------------------------
    silver_valid_condition = (
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &
        col("store_id").isNotNull() &
        col("sales_date").isNotNull() &
        (col("price") > 0) &
        (col("quantity") > 0)
    )

    silver_df = df_enriched.filter(silver_valid_condition)

    # ----------------------------
    # 5. Final Silver Sales schema
    # ----------------------------
    final_df = silver_df.select(
        "customer_id",
        "product_id",
        "store_id",
        "sales_person_id",
        "sales_date",
        "sales_year",
        "sales_month",
        "quantity",
        "price",
        "total_cost"
    )

    return final_df
