import os

from pyspark.sql import SparkSession


STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")


if not STORAGE_ACCOUNT:
    raise RuntimeError("STORAGE_ACCOUNT is not set.")

if not STORAGE_KEY:
    raise RuntimeError("AZURE_STORAGE_KEY is not set.")


QUARANTINE_PATH = (
    f"abfss://quarantine@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)


spark = (
    SparkSession.builder
    .appName("InspectQuarantine")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


spark.conf.set(
    f"fs.azure.account.key."
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)


print("\n==============================")
print("QUARANTINE RECORD COUNT")
print("==============================")

PARQUET_PATH = (
    f"abfss://quarantine@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream/*.parquet"
)

quarantine_df = (
    spark.read
    .format("parquet")
    .load(PARQUET_PATH)
)

print(quarantine_df.count())


print("\n==============================")
print("QUARANTINE RECORDS")
print("==============================")

quarantine_df.show(
    50,
    truncate=False
)


print("\n==============================")
print("QUARANTINE REASONS")
print("==============================")

(
    quarantine_df
    .groupBy("quarantine_reason")
    .count()
    .show(truncate=False)
)


spark.stop()