
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum as _sum,
    avg,
    min as _min,
    max as _max
)


def build_gold_customer_metrics(
    gold_sales_df: DataFrame,
    customer_df: DataFrame
) -> DataFrame:
    """
    Gold layer: Customer Metrics
    - One row per customer
    - Aggregated behavioral metrics
    """

    # ----------------------------
    # 1. Aggregate sales by customer
    # ----------------------------
    customer_sales_agg = (
        gold_sales_df
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_orders"),
            _sum("quantity").alias("total_quantity"),
            _sum("total_cost").alias("total_spent"),
            avg("total_cost").alias("avg_order_value"),
            _min("sales_date").alias("first_purchase_date"),
            _max("sales_date").alias("last_purchase_date")
        )
    )

    # ----------------------------
    # 2. Join with Customer dimension
    # ----------------------------
    customer_metrics_df = (
        customer_sales_agg
        .join(customer_df, on="customer_id", how="inner")
    )

    # ----------------------------
    # 3. Final projection (Gold schema)
    # ----------------------------
    final_df = customer_metrics_df.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("city"),
        col("state"),

        col("total_orders"),
        col("total_quantity"),
        col("total_spent"),
        col("avg_order_value"),
        col("first_purchase_date"),
        col("last_purchase_date")
    )

    return final_df
