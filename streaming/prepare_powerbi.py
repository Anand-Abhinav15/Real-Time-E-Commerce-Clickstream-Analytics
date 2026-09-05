import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, when, to_date

#Config

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

if not STORAGE_ACCOUNT:
    raise RuntimeError("STORAGE_ACCOUNT is not set.")

if not STORAGE_KEY:
    raise RuntimeError("AZURE_STORAGE_KEY is not set.")


# ============================================================
# PATHS
# ============================================================

GOLD_PRODUCT_PATH = (
    f"abfss://gold@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"product_trends"
)

SILVER_PATH = (
    f"abfss://silver@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)

POWERBI_PRODUCT_PATH = (
    f"abfss://gold@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"powerbi/product_trends"
)

POWERBI_KPI_PATH = (
    f"abfss://gold@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"powerbi/daily_kpis"
)


#Spark

spark = (
    SparkSession.builder
    .appName("PreparePowerBIData")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    f"fs.azure.account.key."
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

#Product Trends (Gold Delta -> Power BI Parquet)

print("\nReading Gold Product Trends....")

product_df = (
    spark.read.format("delta").load(GOLD_PRODUCT_PATH)
)

product_df = product_df.select(
    "window_start",
    "window_end",
    "product_id",
    "product_name",
    "category",
    "product_views",
    "add_to_cart",
    "purchases",
    "revenue",
)

print("Writing Product Trends for Power BI...")

(
    product_df.write.mode("overwrite").format("parquet").save(POWERBI_PRODUCT_PATH)
)


#Daily KPIs (Silver -> Power BI Parquet)
# We derive this directly from Silver as the current streaming daily_kpis Gold table hasnt produced a dataset yet. 

print("\nReading Silver Clickstream...")

silver_df = (
    spark.read.format("delta").load(SILVER_PATH)
)

daily_kpis_df = (
    silver_df 
    .withColumn("event_date", to_date(col("event_time")))
    .groupBy("event_date")
    .agg(
        count("*").alias("total_events"),
        count(when(col("event_type") == "product_view", True)).alias("product_views"),
        count(when(col("event_type") == "add_to_cart", True)).alias("add_to_cart"),
        count(when(col("event_type") == "purchase", True)).alias("purchases"),
        sum(when(col("event_type") == col("price"), True).otherwise(0)).alias("revenue"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions")    
    )
    .orderBy("event_date")
)

print("Writing Daily KPIs for Power BI...")

(
    daily_kpis_df.write.mode("overwrite").format("parquet").save(POWERBI_KPI_PATH)
)


#Validation

print("\n===================================")
print("POWER BI DATA PREPARATION COMPLETE")
print("=====================================")

print("\nProduct Trends:")
product_df.printSchema()
print(f"Records: {product_df.count()}")

print("\nDaily KPIs:")
daily_kpis_df.printSchema()
print(f"Records: {daily_kpis_df.count()}")

daily_kpis_df.show(truncate=False)

spark.stop()