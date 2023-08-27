from pyspark.sql.functions import (
    col,
    trim,
    lower,
    current_date,
    row_number,
    when
)
from pyspark.sql.window import Window


def transform_product_data(product_bronze_df):
    """
    Silver transformation for product data.
    Applies data cleaning, validation, deduplication,
    and derives business-correct fields.
    """

    # ----------------------------
    # 1. Standardize string columns
    # ----------------------------
    df = (
        product_bronze_df
        .withColumn("product_name", lower(trim(col("product_name"))))
        .withColumn("category", lower(trim(col("category"))))
        .withColumn("brand", lower(trim(col("brand"))))
    )

    # ----------------------------
    # 2. Enforce price sanity rules
    # ----------------------------
    df = df.filter(col("current_price") > 0)

    df = df.withColumn(
        "old_price",
        when(col("old_price") < 0, None)
        .when(col("old_price") == col("current_price"), None)
        .otherwise(col("old_price"))
    )

    # ----------------------------
    # 3. Date consistency rules
    # ----------------------------
    df = df.filter(
        (col("expiry_date").isNull()) |
        (col("expiry_date") >= col("created_date"))
    )

    df = df.withColumn(
        "updated_date",
        when(col("updated_date") < col("created_date"),
             col("created_date"))
        .otherwise(col("updated_date"))
    )

    # ----------------------------
    # 4. Derive is_active (do NOT trust source)
    # ----------------------------
    df = df.withColumn(
        "is_active",
        when(
            col("expiry_date").isNull() |
            (col("expiry_date") >= current_date()),
            True
        ).otherwise(False)
    )

    # ----------------------------
    # 5. Deduplicate by product_id
    # Keep latest updated_date
    # ----------------------------
    window_spec = Window.partitionBy("product_id") \
                        .orderBy(col("updated_date").desc())

    df = df.withColumn("row_num", row_number().over(window_spec)) \
           .filter(col("row_num") == 1) \
           .drop("row_num")

    # ----------------------------
    # 6. Final column selection (schema lock)
    # ----------------------------
    product_silver_df = df.select(
        "product_id",
        "product_name",
        "category",
        "brand",
        "current_price",
        "old_price",
        "created_date",
        "updated_date",
        "expiry_date",
        "is_active",
        "ingestion_date"
    )

    return product_silver_df
