
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws


def build_gold_sales_fact_enriched(
    sales_df: DataFrame,
    customer_df: DataFrame,
    product_df: DataFrame,
    store_df: DataFrame
) -> DataFrame:
    """
    Gold layer: Enriched Sales Fact
    - Denormalized, analytics-ready
    - Assumes Silver correctness
    """

    # -----------------
    # Alias DataFrames 
    # -----------------
    s = sales_df.alias("s")
    c = customer_df.alias("c")
    p = product_df.alias("p")
    st = store_df.alias("st")

    # ----------------------------
    # Joins (key-based, explicit)
    # ----------------------------
    enriched_df = (
        s
        .join(c, col("s.customer_id") == col("c.customer_id"), "inner")
        .join(p, col("s.product_id") == col("p.product_id"), "inner")
        .join(st, col("s.store_id") == col("st.store_id"), "inner")
    )

    # ----------------------------
    # Final projection (Gold schema)
    # ----------------------------
    gold_df = enriched_df.select(
        # Time
        col("s.sales_date"),
        col("s.sales_year"),
        col("s.sales_month"),

        # Customer
        col("s.customer_id"),
        concat_ws(" ", col("c.first_name"), col("c.last_name")).alias("customer_name"),
        col("c.city").alias("customer_city"),
        col("c.state").alias("customer_state"),

        # Product
        col("s.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.brand"),

        # Store
        col("s.store_id"),
        col("st.store_name"),
        col("st.city").alias("store_city"),
        col("st.state").alias("store_state"),

        # Metrics
        col("s.quantity"),
        col("s.price"),
        col("s.total_cost")
    )

    return gold_df
