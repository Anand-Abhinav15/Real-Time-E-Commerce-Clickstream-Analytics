import os 

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, col, when


#Environment

STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

if not STORAGE_ACCOUNT:
    raise RuntimeError("AZURE_STORAGE_ACCOUNT is not set.")

if not STORAGE_KEY:
    raise RuntimeError("AZURE_STORAGE_KEY is not set.")

#Paths

BRONZE_PATH = (
    f"abfss://bronze@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)

SILVER_PATH = (
    f"abfss://silver@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)

SILVER_CHECKPOINT_PATH = (
    f"abfss://checkpoints@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream_silver"
)

QUARANTINE_PATH = (
    f"abfss://quarantine@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)

QUARANTINE_CHECKPOINT_PATH = (
    f"abfss://checkpoints@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream_quarantine"
)

#Spark Session

spark = (
    SparkSession.builder
    .appName("ClickStreamSilverStreaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtention")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#ADLS Authentication

spark.conf.set(
    f"fs.azure.account.key."
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY 
)

#Read Bronze Stream






