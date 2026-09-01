import os

from pyspark.sql import SparkSession


# ============================================================
# Environment
# ============================================================

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

if not STORAGE_ACCOUNT:
    raise RuntimeError("STORAGE_ACCOUNT is not set.")

if not STORAGE_KEY:
    raise RuntimeError("AZURE_STORAGE_KEY is not set.")


# ============================================================
# Gold Path
# ============================================================

GOLD_PATH = (
    f"abfss://gold@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"product_trends"
)


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("InspectGold")
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


# ============================================================
# ADLS Authentication
# ============================================================

spark.conf.set(
    f"fs.azure.account.key."
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)


# ============================================================
# Read Gold
# ============================================================

gold_df = (
    spark.read
    .format("delta")
    .load(GOLD_PATH)
)


# ============================================================
# Inspect
# ============================================================

print("\n===== GOLD SCHEMA =====")
gold_df.printSchema()

print("\n===== GOLD DATA =====")
gold_df.show(20, truncate=False)

print("\n===== GOLD COUNT =====")
print(f"Total records: {gold_df.count()}")


spark.stop()