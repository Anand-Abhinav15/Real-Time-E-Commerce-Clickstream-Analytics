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
# Path
# ============================================================

GOLD_KPI_PATH = (
    f"abfss://gold@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"daily_kpis"
)


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("InspectDailyKPIs")
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

kpi_df = (
    spark.read
    .format("delta")
    .load(GOLD_KPI_PATH)
)


# ============================================================
# Inspect
# ============================================================

print("\n===== DAILY KPI SCHEMA =====")
kpi_df.printSchema()

print("\n===== DAILY KPI DATA =====")
kpi_df.show(20, truncate=False)

print("\n===== DAILY KPI COUNT =====")
print(f"Total records: {kpi_df.count()}")


spark.stop()


