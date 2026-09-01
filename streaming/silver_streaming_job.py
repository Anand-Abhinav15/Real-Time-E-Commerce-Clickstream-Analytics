import os 

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, col, when, to_timestamp


#Environment

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

if not STORAGE_ACCOUNT:
    raise RuntimeError("STORAGE_ACCOUNT is not set.")

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

#Read Bronze Stream

bronze_df = (
    spark.readStream.format("delta").load(BRONZE_PATH)
)

#Validation

invalid_condition = (
    col("event_id").isNull()
    | col("user_id").isNull()
    | col("session_id").isNull()
    | col("event_time").isNull()
    | col("event_type").isNull()
)

#Quarantine Invalid Records

quarantine_df = (
    bronze_df
    .filter(invalid_condition)
    .withColumn("quarantine_reason", 
        when(col("event_id").isNull(), "missing_event_id")
        .when(col("user_id").isNull(), "missing_user_id")
        .when(col("session_id").isNull(), "missing_session_id")
        .when(col("event_time").isNull(), "missing_event_time")
        .when(col("event_type").isNull(), "missing_event_type")
        .otherwise("unknown")
    )
)

quarantine_query = (
    quarantine_df.writeStream 
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", QUARANTINE_CHECKPOINT_PATH)
    .start(QUARANTINE_PATH)
)

#Valid Records

valid_df = (bronze_df.filter(~invalid_condition))

#Watermark + Deduplication

silver_df = (
    valid_df
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["event_id"])
)

#Standardization

silver_df = (
    silver_df 
    .withColumn("device_type", lower(trim(col("device_type"))))
    .withColumn("traffic_source", lower(trim(col("traffic_source"))))
    .withColumn("country", trim(col("country")))
    .withColumn("browser", lower(trim(col("browser"))))
    .withColumn("page", lower(trim(col("page"))))
    .withColumn("event_type", lower(trim(col("event_type"))))
)

#Write Silver

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
    .start(SILVER_PATH) 
)

#Wait for Streams

spark.streams.awaitAnyTermination()





