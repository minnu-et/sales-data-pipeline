def write_bronze_raw(df, path):
    print("BRONZE_VALID PATH:", path)
    (
        df.write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(path)
    )



def write_bronze_rejected(df, path):
    print("BRONZE_REJECTED PATH:", path)
    (
        df.write
        .mode("overwrite")
        .partitionBy("ingestion_date")
        .parquet(path)
    )
