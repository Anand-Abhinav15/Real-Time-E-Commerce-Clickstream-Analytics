import os

from pyspark.sql import SparkSession


# =========================================================
# ENVIRONMENT
# =========================================================

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

if not STORAGE_ACCOUNT:
    raise RuntimeError("STORAGE_ACCOUNT is not set.")

if not STORAGE_KEY:
    raise RuntimeError("AZURE_STORAGE_KEY is not set.")


# =========================================================
# PATH
# =========================================================

QUARANTINE_PATH = (
    f"abfss://quarantine@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)


# =========================================================
# SPARK
# =========================================================

spark = (
    SparkSession.builder
    .appName("InspectQuarantine")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# ADLS AUTHENTICATION
# =========================================================

spark.conf.set(
    f"fs.azure.account.key."
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)


# =========================================================
# READ QUARANTINE
# =========================================================

quarantine_df = (
    spark.read
    .format("delta")
    .load(QUARANTINE_PATH)
)


# =========================================================
# SHOW SCHEMA
# =========================================================

print("\n==============================")
print("QUARANTINE SCHEMA")
print("==============================")

quarantine_df.printSchema()


# =========================================================
# SHOW RECORDS
# =========================================================

print("\n==============================")
print("QUARANTINE RECORDS")
print("==============================")

(
    quarantine_df
    .select(
        "event_id",
        "user_id",
        "session_id",
        "event_time",
        "event_type",
        "quarantine_reason"
    )
    .show(
        50,
        truncate=False
    )
)


# =========================================================
# RECORD COUNT
# =========================================================

print("\n==============================")
print("TOTAL QUARANTINED RECORDS")
print("==============================")

print(
    quarantine_df.count()
)


spark.stop()