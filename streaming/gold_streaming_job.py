import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, window, to_timestamp

#Environment

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

if not STORAGE_ACCOUNT:
    raise RuntimeError("STORAGE_ACCOUNT is not set.")

if not STORAGE_KEY:
    raise RuntimeError("AZURE_STORAGE_KEY is not set.")


#Paths

SILVER_PATH = (
    f"abfss://silver@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)

GOLD_PRODUCT_PATH = (
    f"abfss://gold@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"product_trends"
)

GOLD_PRODUCT_CHECKPOINT = (
    f"abfss://checkpoints@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"gold_product_trends"
)


#Spark Session

spark = (
    SparkSession.builder
    .appName("ClickStreamGoldProductTrends")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


#ADLS Authentication

spark.conf.set(
    f"fs.azure.account.key."
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY
)


#Read Silver

silver_df = (
    spark.readStream
    .format("delta").load(SILVER_PATH)
)


#Event-Time Processing

gold_df = (
    silver_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("product_id"),
        col("product_name"),
        col("category")
    )
    .agg(
        count(when(col("event_type") == "product_view", True))
            .alias("product_views"),
        count(when(col("event_type") == "add_to_cart", True))
            .alias("add_to_cart"),
        count(when(col("event_type") == "purchase", True))
            .alias("purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0))
            .alias("revenue")
    )
)


#Write Gold

gold_query = (
   gold_df
   .writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", GOLD_PRODUCT_CHECKPOINT)
   .start(GOLD_PRODUCT_PATH)
)


#Wait

gold_query.awaitTermination()


