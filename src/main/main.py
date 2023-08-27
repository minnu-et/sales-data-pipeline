import os
from pyspark.sql.functions import col, current_date, when, lit
from pyspark.sql.functions import max as spark_max  
from datetime import datetime

# NEW: Import config loader
from main.utility.config_loader import load_config, print_config_summary
from main.utility.logging_config import logger
from main.utility.spark_session import get_spark_session
from main.utility.watermark_manager import WatermarkManager
from main.utility.data_validator import DataValidator, validate_bronze_data, validate_silver_data
from main.read.read_csv import read_csv
from main.transformations.sales_transform import transform_sales_data
from main.transformations.product_transform import transform_product_data
from main.transformations.store_transform import transform_store_data
from main.transformations.customer_transform import transform_customer_data
from main.transformations.scd_customer_transform import apply_scd_type2, get_current_customers,_add_scd_columns
from main.gold.sales_gold import build_gold_sales_fact_enriched
from main.gold.customer_metrics_gold import build_gold_customer_metrics

from main.write.write_bronze import write_bronze_raw, write_bronze_rejected
from main.write.write_parquet import write_parquet




def main():
    logger.info("Starting Data Engineering pipeline")

    # NEW: Load configuration
    config = load_config()
    print_config_summary()  # Print config summary for verification

    spark = None

    try:
        # STEP 1 — Spark
        spark = get_spark_session()
        logger.info("Spark session initialized successfully")

        validator = DataValidator()
        logger.info("Data validator initialized")

        # STEP 2 — Resolve CSV paths from config
        # NEW: Using config instead of manual path resolution
        sales_csv_path = config['paths']['source']['sales']
        product_csv_path = config['paths']['source']['product']
        store_csv_path = config['paths']['source']['store']
        customer_csv_path = config['paths']['source']['customer']

        # NEW: Get paths from config
        bronze_sales_raw = config['paths']['bronze']['sales']['raw']
        bronze_sales_rejected = config['paths']['bronze']['sales']['rejected']
        bronze_product_raw = config['paths']['bronze']['product']['raw']
        bronze_product_rejected = config['paths']['bronze']['product']['rejected']
        bronze_store_raw = config['paths']['bronze']['store']['raw']
        bronze_store_rejected = config['paths']['bronze']['store']['rejected']
        bronze_customer_raw = config['paths']['bronze']['customer']['raw']
        bronze_customer_rejected = config['paths']['bronze']['customer']['rejected']

        silver_sales_path = config['paths']['silver']['sales']
        silver_product_path = config['paths']['silver']['product']
        silver_store_path = config['paths']['silver']['store']
        silver_customer_path = config['paths']['silver']['customer']
        
        gold_sales_enriched_path = config['paths']['gold']['sales_enriched']
        gold_customer_metrics_path = config['paths']['gold']['customer_metrics']

        # STEP 3 — Read RAW data
        # Check if incremental mode is enabled
        pipeline_mode = config['pipeline']['mode']
        logger.info(f"Pipeline mode: {pipeline_mode}")

        if pipeline_mode == "incremental":
            # Initialize watermark manager for sales
            sales_watermark = WatermarkManager(config, spark, entity="sales")
            
            # Get filter condition for incremental read
            filter_condition = sales_watermark.get_filter_condition()
            logger.info(f"Applying incremental filter: {filter_condition}")
            
            # Read only new data
            df_raw = read_csv(spark, sales_csv_path, filter_condition=filter_condition)
            
            # Log how many new records were read
            new_records_count = df_raw.count()
            logger.info(f"Incremental read: {new_records_count} new records found")
        else:
            # Full refresh mode - read everything
            df_raw = read_csv(spark, sales_csv_path)
            logger.info("Full refresh: reading all data")


        # ================= BRONZE START =================

        
        # ==============================
        # STEP — BRONZE SALES
        # ==============================

        df_raw = df_raw.withColumn("ingestion_date", current_date())

        valid_customer = col("customer_id").isNotNull()
        valid_store = col("store_id").isNotNull()
        valid_sales_date = col("sales_date").isNotNull()
        valid_sales_price = col("price").isNotNull() & (col("price") > 0)
        valid_quantity = col("quantity").isNotNull() & (col("quantity") > 0)

        bronze_valid_condition = (
            valid_customer &
            valid_store &
            valid_sales_date &
            valid_sales_price &
            valid_quantity
        )

        # Order matters — first matching rule wins
        rejection_reason = (
            when(~valid_customer, lit("NULL_CUSTOMER_ID"))
            .when(~valid_store, lit("NULL_STORE_ID"))
            .when(~valid_sales_date, lit("NULL_SALES_DATE"))
            .when(~valid_sales_price, lit("INVALID_PRICE"))
            .when(~valid_quantity, lit("INVALID_QUANTITY"))
            .otherwise(lit("UNKNOWN_REASON"))
        )

        valid_bronze_df = df_raw.filter(bronze_valid_condition)
        rejected_bronze_df = (
            df_raw
            .filter(~bronze_valid_condition)
            .withColumn("rejection_reason", rejection_reason)
        )

        rejected_bronze_df.printSchema()

        # NEW: Using config paths
        write_bronze_raw(valid_bronze_df, bronze_sales_raw)
        write_bronze_rejected(rejected_bronze_df, bronze_sales_rejected)

        # Validate bronze sales data
        logger.info("Validating bronze sales data...")
        try:
            validation_result = validate_bronze_data(valid_bronze_df, "sales")
            if validation_result.success:
                logger.info("✓ Bronze sales validation passed")
            else:
                logger.warning(f"✗ Bronze sales validation issues: {validation_result.statistics}")
        except Exception as e:
            logger.error(f"Bronze validation error: {e}")

        logger.info("Bronze Sales ingestion completed")

                
        # ==============================
        # STEP — BRONZE PRODUCT
        # ==============================

        logger.info("Starting Bronze product ingestion")

        # NEW: Using config path
        product_df_raw = read_csv(spark, product_csv_path)

        # Add ingestion_date
        product_df_raw = product_df_raw.withColumn("ingestion_date", current_date())

        # Bronze validation rules (explicit)
        valid_product_id = col("product_id").isNotNull()
        valid_product_name = col("product_name").isNotNull()
        valid_product_price = col("current_price").isNotNull() & (col("current_price") > 0)
        valid_is_active = col("is_active").isNotNull()

        product_bronze_valid_condition = (
            valid_product_id &
            valid_product_name &
            valid_product_price &
            valid_is_active
        )

        # Rejection reason (order matters)
        product_rejection_reason = (
            when(~valid_product_id, lit("NULL_PRODUCT_ID"))
            .when(~valid_product_name, lit("NULL_PRODUCT_NAME"))
            .when(~valid_product_price, lit("INVALID_PRICE"))
            .when(~valid_is_active, lit("NULL_IS_ACTIVE"))
            .otherwise(lit("UNKNOWN_REASON"))
        )

        # Split valid / rejected
        product_bronze_valid_df = product_df_raw.filter(
            product_bronze_valid_condition
        )

        product_bronze_rejected_df = (
            product_df_raw
            .filter(~product_bronze_valid_condition)
            .withColumn("rejection_reason", product_rejection_reason)
        )

        # Debug schema (optional but recommended)
        logger.info("Rejected Product Bronze Schema:")
        product_bronze_rejected_df.printSchema()

        # NEW: Using config paths
        write_bronze_raw(product_bronze_valid_df, bronze_product_raw)
        write_bronze_rejected(product_bronze_rejected_df, bronze_product_rejected)

        # Validate bronze product data
        logger.info("Validating bronze product data...")
        try:
            validation_result = validate_bronze_data(product_bronze_valid_df, "product")
            if validation_result.success:
                logger.info("✓ Bronze product validation passed")
        except Exception as e:
            logger.error(f"Bronze product validation error: {e}")

        logger.info("Bronze Product ingestion completed")


        
        # ==============================
        # STEP — BRONZE STORE
        # ==============================

        # NEW: Using config path
        store_df_raw = read_csv(spark, store_csv_path)

        # Add ingestion_date
        store_df_raw = store_df_raw.withColumn(
            "ingestion_date", current_date())

        # Bronze validation rules
        valid_store_id = col("store_id").isNotNull()
        valid_store_name = col("store_name").isNotNull()
        valid_opening_date = col("store_opening_date").isNotNull()

        bronze_store_condition = (
            valid_store_id &
            valid_store_name &
            valid_opening_date
        )

        # Rejection reason (order matters)
        store_rejection_reason = (
            when(~valid_store_id, lit("NULL_STORE_ID"))
            .when(~valid_store_name, lit("NULL_STORE_NAME"))
            .when(~valid_opening_date, lit("NULL_OPENING_DATE"))
            .otherwise(lit("UNKNOWN_REASON"))
        )

        # Split valid / rejected
        store_bronze_valid_df = store_df_raw.filter(bronze_store_condition)

        store_bronze_rejected_df = (
            store_df_raw
            .filter(~bronze_store_condition)
            .withColumn("rejection_reason", store_rejection_reason)
        )
        logger.info("Rejected Store Bronze Schema:")
        store_bronze_rejected_df.printSchema()
       
        
        # NEW: Using config paths
        write_bronze_raw(store_bronze_valid_df, bronze_store_raw)
        write_bronze_rejected(store_bronze_rejected_df, bronze_store_rejected)

        # Validate bronze store data
        logger.info("Validating bronze store data...")
        try:
            validation_result = validate_bronze_data(store_bronze_valid_df, "store")
            if validation_result.success:
                logger.info("✓ Bronze store validation passed")
        except Exception as e:
            logger.error(f"Bronze store validation error: {e}")

        logger.info("Bronze Stores ingestion completed")

        
        
        # ==============================
        # CUSTOMER PATHS
        # ==============================

        logger.info("Starting Bronze customer ingestion")

        # NEW: Using config path
        customer_df_raw = read_csv(spark, customer_csv_path)

        # Add ingestion_date
        customer_df_raw = customer_df_raw.withColumn(
            "ingestion_date", current_date()
        )

        # Bronze validation rules
        valid_customer_id = col("customer_id").isNotNull()
        valid_email = col("email").isNotNull()
        valid_dob = col("date_of_birth").isNotNull()

        bronze_customer_condition = (
            valid_customer_id &
            valid_email &
            valid_dob
        )

        # Rejection reason (order matters)
        customer_rejection_reason = (
            when(~valid_customer_id, lit("NULL_CUSTOMER_ID"))
            .when(~valid_email, lit("NULL_EMAIL"))
            .when(~valid_dob, lit("NULL_DATE_OF_BIRTH"))
            .otherwise(lit("UNKNOWN_REASON"))
        )

        # Split valid / rejected
        customer_bronze_valid_df = customer_df_raw.filter(
            bronze_customer_condition
        )

        customer_bronze_rejected_df = (
            customer_df_raw
            .filter(~bronze_customer_condition)
            .withColumn("rejection_reason", customer_rejection_reason)
        )

        logger.info("Rejected Customer Bronze Schema:")
        customer_bronze_rejected_df.printSchema()

        # NEW: Using config paths
        write_bronze_raw(customer_bronze_valid_df, bronze_customer_raw)
        write_bronze_rejected(customer_bronze_rejected_df, bronze_customer_rejected)

        # Validate bronze customer data
        logger.info("Validating bronze customer data...")
        try:
            validation_result = validate_bronze_data(customer_bronze_valid_df, "customer")
            if validation_result.success:
                logger.info("✓ Bronze customer validation passed")
        except Exception as e:
            logger.error(f"Bronze customer validation error: {e}")

        logger.info("Bronze customer ingestion completed")

        # ================= SILVER START =================
        
        # ==============================
        # STEP — SILVER PRODUCT
        # ==============================
        logger.info("Starting Silver product transformation")

        silver_product_df = transform_product_data(product_bronze_valid_df)

        # Sanity checks
        silver_product_df.printSchema()

        bronze_product_count = product_bronze_valid_df.count()
        silver_product_count = silver_product_df.count()

        logger.info(f"Bronze product valid count: {bronze_product_count}")
        logger.info(f"Silver product count: {silver_product_count}")
        

        # NEW: Using config path and write mode
        write_mode = config['pipeline']['write_modes']['silver']
        write_parquet(
            silver_product_df,
            silver_product_path,
            mode=write_mode
        )

        logger.info("Silver product layer written successfully")

        
        
        # ==============================
        # STEP — SILVER SALES
        # ==============================

        silver_df = transform_sales_data(valid_bronze_df, silver_product_df)


        bronze_sales_count = valid_bronze_df.count()
        silver_sales_count = silver_df.count()

        silver_drop_count = bronze_sales_count - silver_sales_count
        silver_drop_percentage = (
            (silver_drop_count / bronze_sales_count) * 100
            if bronze_sales_count > 0 else 0
        )

        logger.info(f"Silver Metrics | Bronze valid count: {bronze_sales_count}")
        logger.info(f"Silver Metrics | Silver count: {silver_sales_count}")
        logger.info(f"Silver Metrics | Dropped in Silver: {silver_drop_count}")
        logger.info(
            f"Silver Metrics | Drop percentage: {silver_drop_percentage:.2f}%"
        )


        critical_nulls = {
            "customer_id": silver_df.filter(col("customer_id").isNull()).count(),
            "store_id": silver_df.filter(col("store_id").isNull()).count(),
            "sales_person_id": silver_df.filter(col("sales_person_id").isNull()).count(),
            "sales_date": silver_df.filter(col("sales_date").isNull()).count(),
        }
        # Null checks
        for column, count in critical_nulls.items():
            logger.info(f"Silver Null Check | {column}: {count}")
        # Numeric sanity checks
        invalid_price_count = silver_df.filter(col("price") <= 0).count()
        invalid_quantity_count = silver_df.filter(col("quantity") <= 0).count()

        if pipeline_mode == "incremental":
        # Calculate the max timestamp from the data we just processed
                
            max_timestamp = silver_df.agg(
                spark_max(col("sales_date")).alias("max_ts")
            ).collect()[0]["max_ts"]
            
            if max_timestamp:
                max_timestamp_str = max_timestamp.strftime("%Y-%m-%d %H:%M:%S")
                metadata = {...}
                sales_watermark.update_watermark(max_timestamp_str, metadata)
                logger.info(f"Watermark updated: {max_timestamp_str}")
            else:
                logger.info("No new records to process, watermark unchanged")


        logger.info(
            f"Silver Sanity | Invalid price count: {invalid_price_count}")
        logger.info(
            f"Silver Sanity | Invalid quantity count: {invalid_quantity_count}")

        if invalid_price_count > 0 or invalid_quantity_count > 0:
            raise Exception(
                "Silver data quality check failed: invalid numeric values detected")

        # NEW: Using config path and partitioning from config
        silver_df.printSchema()
        partition_config = config['pipeline']['partitioning']['silver']['sales']
        partition_cols = partition_config['columns'] if partition_config['enabled'] else None
        
        logger.info("Validating silver sales data...")
        try:
            validation_result = validate_silver_data(silver_df, "sales", strict=False)
            if validation_result.success:
                logger.info("✓ Silver sales validation passed")
            else:
                logger.warning(f"Silver sales validation has issues but continuing...")
                validator.print_validation_summary(validation_result)
        except Exception as e:
            logger.error(f"Silver sales validation error: {e}")

        write_parquet(
            silver_df, 
            silver_sales_path,
            mode=write_mode,
            partition_cols=partition_cols
        )

        logger.info("Silver sales layer written successfully")

        

        # ==============================
        # STEP — SILVER STORE
        # ==============================

        logger.info("Starting Silver store transformation")

        silver_store_df = transform_store_data(store_bronze_valid_df)

        logger.info("Silver Store Schema:")
        silver_store_df.printSchema()

        bronze_store_count = store_bronze_valid_df.count()
        silver_store_count = silver_store_df.count()

        logger.info(f"Bronze store valid count: {bronze_store_count}")
        logger.info(f"Silver store count: {silver_store_count}")

        # NEW: Using config path
        write_parquet(
            silver_store_df,
            silver_store_path,
            mode=write_mode
        )

        logger.info("Silver store layer written successfully")




   # ==============================
        # STEP — SILVER CUSTOMER
        # ==============================

        logger.info("Starting Silver customer transformation")

        silver_customer_df = transform_customer_data(customer_bronze_valid_df)

        bronze_customer_count = customer_bronze_valid_df.count()
        silver_customer_count = silver_customer_df.count()

        logger.info(f"Bronze customer valid count: {bronze_customer_count}")
        logger.info(f"Silver customer count: {silver_customer_count}")

        logger.info("Silver Customer Schema:")
        silver_customer_df.printSchema()

        # SCD Type 2
        try:
            existing_customer_df = spark.read.parquet(silver_customer_path)
            logger.info("Existing customer data found - applying SCD Type 2")
            scd_customer_df = apply_scd_type2(spark, existing_customer_df, silver_customer_df)
        except Exception:
            logger.info("No existing customer data - first run, initializing SCD")
            scd_customer_df = _add_scd_columns(silver_customer_df)

        write_parquet(scd_customer_df, silver_customer_path, mode="overwrite")
        logger.info(f"SCD Customer records: {scd_customer_df.count()}")
        logger.info(f"Current customers: {get_current_customers(scd_customer_df).count()}")

        # ================= SILVER END =================

        # ==============================
        # STEP — GOLD SALES (ENRICHED FACT)
        # ==============================

        logger.info("Starting Gold sales enrichment")

        # Read silver data (using config paths)
        silver_sales_df = spark.read.parquet(silver_sales_path)
        silver_customer_df = spark.read.parquet(silver_customer_path)
        silver_product_df = spark.read.parquet(silver_product_path)
        silver_store_df = spark.read.parquet(silver_store_path)


        gold_sales_df = build_gold_sales_fact_enriched(
            sales_df=silver_sales_df,
            customer_df=silver_customer_df,
            product_df=silver_product_df,
            store_df=silver_store_df
        )

        gold_sales_count = gold_sales_df.count()
        logger.info(f"Gold sales enriched count: {gold_sales_count}")

        logger.info("Gold Sales Enriched Schema:")
        gold_sales_df.printSchema()

        # NEW: Using config path and partitioning
        gold_write_mode = config['pipeline']['write_modes']['gold']
        gold_partition_config = config['pipeline']['partitioning']['gold']['sales_enriched']
        gold_partition_cols = gold_partition_config['columns'] if gold_partition_config['enabled'] else None
        
        write_parquet(
            gold_sales_df,
            gold_sales_enriched_path,
            mode=gold_write_mode,
            partition_cols=gold_partition_cols
        )

        logger.info("Gold sales enriched layer written successfully")

        # ==============================
        # STEP — GOLD CUSTOMER METRICS
        # ==============================

        logger.info("Starting Gold customer metrics")

        gold_customer_metrics_df = build_gold_customer_metrics(
            gold_sales_df,
            silver_customer_df
        )

        logger.info(
            f"Gold customer metrics count: {gold_customer_metrics_df.count()}"
        )

        logger.info("Gold Customer Metrics Schema:")
        gold_customer_metrics_df.printSchema()

        # NEW: Using config path
        write_parquet(
            gold_customer_metrics_df,
            gold_customer_metrics_path,
            mode=gold_write_mode
        )

        logger.info("Gold customer metrics layer written successfully")


    except Exception as e:
        logger.error("Pipeline failed due to error: %s", e)
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
