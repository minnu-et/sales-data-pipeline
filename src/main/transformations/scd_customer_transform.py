"""
SCD Type 2 - Customer Dimension
================================
Implements Slowly Changing Dimension Type 2 for Customer data.

What SCD Type 2 does:
- Keeps FULL HISTORY of customer changes
- Old records: is_current=False, valid_to=today
- New records: is_current=True, valid_to=None

Example:
BEFORE update (customer moves city):
  id:1, city:Mumbai, valid_from:2024-01-01, valid_to:None, is_current:True

AFTER update:
  id:1, city:Mumbai, valid_from:2024-01-01, valid_to:2026-02-17, is_current:False  ← Closed
  id:1, city:Delhi,  valid_from:2026-02-17, valid_to:None,       is_current:True   ← New

Usage:
    from main.transformations.scd_customer_transform import apply_scd_type2
    updated_df = apply_scd_type2(spark, existing_df, new_df)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_date, lit, when, md5, concat_ws
)
from pyspark.sql.types import BooleanType, DateType


# Columns that trigger a new SCD record when they change
SCD_TRACKED_COLUMNS = [
    "first_name",
    "last_name", 
    "email",
    "phone_number",
    "city",
    "state",
    "address",
    "pincode"
]


def _add_scd_columns(df: DataFrame) -> DataFrame:
    """
    Add SCD Type 2 columns to a DataFrame.
    
    Adds:
    - valid_from: When this record became active
    - valid_to: When this record was superseded (None = still active)
    - is_current: Is this the latest record?
    - row_hash: Hash of tracked columns (to detect changes)
    """
    return (
        df
        .withColumn("valid_from", current_date())
        .withColumn("valid_to", lit(None).cast(DateType()))
        .withColumn("is_current", lit(True).cast(BooleanType()))
        .withColumn(
            "row_hash",
            md5(concat_ws("|", *[col(c) for c in SCD_TRACKED_COLUMNS]))
        )
    )


def apply_scd_type2(
    spark: SparkSession,
    existing_df: DataFrame,
    new_df: DataFrame
) -> DataFrame:
    """
    Apply SCD Type 2 logic.
    
    Logic:
    1. NEW customers    → Add as new current records
    2. CHANGED customers → Close old record + Add new record  
    3. UNCHANGED customers → Keep as-is
    4. DELETED customers → Keep old record (don't delete history)
    
    Args:
        spark: Active SparkSession
        existing_df: Current Silver Customer data (may already have SCD cols)
        new_df: Incoming new/updated customer data
        
    Returns:
        Updated DataFrame with full SCD Type 2 history
    """

    today = current_date()

    # -----------------------------------------------
    # STEP 1: Prepare new data with SCD columns
    # -----------------------------------------------
    new_with_scd = _add_scd_columns(new_df)

    # -----------------------------------------------
    # STEP 2: Check if existing data has SCD columns
    # If not (first run), initialize them
    # -----------------------------------------------
    existing_cols = existing_df.columns
    
    if "is_current" not in existing_cols:
        # First time applying SCD - add columns to existing data
        existing_with_scd = _add_scd_columns(existing_df)
    else:
        # Already has SCD columns
        existing_with_scd = existing_df

    # -----------------------------------------------
    # STEP 3: Find CHANGED records
    # Compare hash of tracked columns
    # -----------------------------------------------
    
    # Get current active records from existing data
    current_records = existing_with_scd.filter(col("is_current") == True)
    
    # Join new data with current records to find changes
    changed = (
        new_with_scd.alias("new")
        .join(
            current_records.alias("old"),
            on="customer_id",
            how="inner"
        )
        .filter(col("new.row_hash") != col("old.row_hash"))  # Hash changed = data changed
        .select("new.customer_id")
    )

    changed_ids = [row.customer_id for row in changed.collect()]

    # -----------------------------------------------
    # STEP 4: Close OLD records for changed customers
    # Set valid_to = today, is_current = False
    # -----------------------------------------------
    if changed_ids:
        existing_updated = existing_with_scd.withColumn(
            "is_current",
            when(
                (col("customer_id").isin(changed_ids)) & 
                (col("is_current") == True),
                lit(False)
            ).otherwise(col("is_current"))
        ).withColumn(
            "valid_to",
            when(
                (col("customer_id").isin(changed_ids)) & 
                (col("is_current") == False) &
                (col("valid_to").isNull()),
                today
            ).otherwise(col("valid_to"))
        )
    else:
        existing_updated = existing_with_scd

    # -----------------------------------------------
    # STEP 5: Find NEW customers (not in existing)
    # -----------------------------------------------
    existing_ids = existing_with_scd.select("customer_id").distinct()
    
    new_customers = (
        new_with_scd
        .join(existing_ids, on="customer_id", how="left_anti")  # Not in existing
    )

    # -----------------------------------------------
    # STEP 6: Get NEW records for CHANGED customers
    # -----------------------------------------------
    if changed_ids:
        changed_new_records = new_with_scd.filter(
            col("customer_id").isin(changed_ids)
        )
    else:
        changed_new_records = spark.createDataFrame([], new_with_scd.schema)

    # -----------------------------------------------
    # STEP 7: Combine everything
    # -----------------------------------------------
    # Select consistent columns
    final_columns = [
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
        "pincode",
        "valid_from",
        "valid_to",
        "is_current",
        "row_hash"
    ]

    # Filter to only columns that exist
    available_cols = [c for c in final_columns if c in new_with_scd.columns]

    result = (
        existing_updated.select(available_cols)
        .union(new_customers.select(available_cols))
        .union(changed_new_records.select(available_cols))
    )

    return result


def get_current_customers(scd_df: DataFrame) -> DataFrame:
    """
    Get only current (active) customer records.
    
    Use this when you need the latest customer data for joins.
    
    Args:
        scd_df: Full SCD Type 2 DataFrame with history
        
    Returns:
        DataFrame with only is_current=True records
    """
    return scd_df.filter(col("is_current") == True)


def get_customer_history(scd_df: DataFrame, customer_id: int) -> DataFrame:
    """
    Get full history for a specific customer.
    
    Useful for auditing and debugging.
    
    Args:
        scd_df: Full SCD Type 2 DataFrame
        customer_id: Customer ID to look up
        
    Returns:
        DataFrame with all historical records for this customer
    """
    return (
        scd_df
        .filter(col("customer_id") == customer_id)
        .orderBy("valid_from")
    )
