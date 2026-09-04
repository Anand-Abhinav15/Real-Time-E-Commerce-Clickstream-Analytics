import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, approx_count_distinct, sum, when, window

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

GOLD_KPI_PATH = (
    f"abfss://gold@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"daily_kpis"
)

GOLD_KPI_CHECKPOINT = (
    f"abfss://checkpoints@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"gold_daily_kpis"
)


#Spark Session

spark = (
    SparkSession.builder
    .appName("ClickStreamGoldDailyKPIs")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


#ADLS Auth

spark.conf.set(
    f"fs.azure.account.key."
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)


#Read Silver

silver_df = (
    spark.readStream.format("delta").load(SILVER_PATH)
)


#Daily KPI Aggregation

daily_kpis_df = (
    silver_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "1 day"))
    .agg(
        count("*").alias("total_events"),
        count(when(col("event_type") == "product_view", True)).alias("product_views"),
        count(when(col("event_type") == "add_to_cart", True)).alias("add_to_cart"),
        count(when(col("event_type") == "purchase", True)).alias("purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("revenue"),
        approx_count_distinct("user_id").alias("unique_users"),
        approx_count_distinct("session_id").alias("unique_sessions")
    )
    .select(
        col("window.start").alias("window_start"),
        col("windo.end").alias("window_end"),
        col("total_events"),
        col("product_views"),
        col("add_to_cart"),
        col("purchases"),
        col("revenue"),
        col("unique_users"),
        col("unique_sessions")
    )
)


# Streaming Debug

print("\n===== STREAMING DEBUG =====")
print(f"Silver is streaming: {silver_df.isStreaming}")
print(f"Daily KPIs is streaming: {daily_kpis_df.isStreaming}")


#Write Gold

gold_query = (
    daily_kpis_df
    .writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", GOLD_KPI_CHECKPOINT)
    .start(GOLD_KPI_PATH)
)


#Wait

gold_query.awaitTermination()

