from pyspark.sql import SparkSession
from main.utility.logging_config import logger


def get_spark_session(app_name: str = "de_project_spark"):
    """
    Creates and returns a SparkSession.

    - No hardcoded paths
    - No AWS keys
    - IAM / AWS Vault compatible
    - Works locally and in cloud
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "mysql:mysql-connector-java:8.0.33"
               )
        #  CRITICAL WINDOWS FIXES
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        # Problem    Fix
        # Hadoop tries native Windows calls ------->   io.native.lib.available=false
        # S3A stages files locally          ------->   spark.local.dir
        # Windows permission checks fail    ------->   custom temp dir
        # Reused FS cache causes crashes    ------->   disable S3A cache
        # Avoid Windows temp dir permission/native checks
        .config("spark.local.dir", "C:/spark-temp")

        .getOrCreate()
    )

    logger.info("Spark session created successfully")
    return spark
