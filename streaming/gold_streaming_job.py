import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when, window

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





