import os
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# Environment Variables

EVENTHUB_CONNECTION_STRING = os.getenv(
    "EVENTHUB_SPARK_CONNECTION_STRING"
)

EVENTHUB_CONSUMER_GROUP = os.getenv(
    "EVENTHUB_CONSUMER_GROUP",
    "spark-streaming"
)

if not EVENTHUB_CONNECTION_STRING:
    raise RuntimeError(
        "EVENTHUB_SPARK_CONNECTION_STRING is not set."
    )

STORAGE_ACCOUNT = os.getenv(
    "AZURE_STORAGE_ACCOUNT"
)

STORAGE_KEY = os.getenv(
    "AZURE_STORAGE_KEY"
)

if not STORAGE_ACCOUNT:
    raise RuntimeError(
        "AZURE_STORAGE_ACCOUNT is not set."
    )

if not STORAGE_KEY:
    raise RuntimeError(
        "AZURE_STORAGE_KEY is not set."
    )


#Spark Session

spark = (
    SparkSession.builder
    .appName("ClickstreamBronzeStreaming")
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


#ADLS Authentication

spark.conf.set(
    f"fs.azure.account.key."
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)


#ADLS Paths

BRONZE_PATH = (
    f"abfss://bronze@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)

CHECKPOINT_PATH = (
    f"abfss://checkpoints@"
    f"{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    f"clickstream"
)


# Event Hubs connector configuration
eventhub_conf = {
    "eventhubs.connectionString": (
        EVENTHUB_CONNECTION_STRING
    ),
    "eventhubs.consumerGroup": (
        EVENTHUB_CONSUMER_GROUP
    ),
}

# Encrypt the connection string before passing
# it into the Event Hubs connector.

eventhub_conf[
    "eventhubs.connectionString"
] = (
    spark._jvm
    .org.apache.spark.eventhubs.EventHubsUtils
    .encrypt(EVENTHUB_CONNECTION_STRING)
)

# Connectivity Test - Stream start from the earliest available event so we can see the events already produced.
starting_position = {
    "offset": "-1",
    "seqNo": -1,
    "enqueuedTime": None,
    "isInclusive": True,
}

eventhub_conf[
    "eventhubs.startingPosition"
] = json.dumps(starting_position)


# Read stream from Azure Event Hubs
events = (
    spark.readStream
    .format("eventhubs")
    .options(**eventhub_conf)
    .load()
)

# Event Hub body is binary
events_text = events.selectExpr(
    "CAST(body AS STRING) AS body"
)


#Event Schema

event_schema = StructType ([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("persona", StringType(), True),
    StructField("page", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
])



#Write Bronze Delta

query = (
    parsed_events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(BRONZE_PATH)
)

query.awaitTermination()



