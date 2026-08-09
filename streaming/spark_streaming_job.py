import os
import json

from pyspark.sql import SparkSession

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

spark = (
    SparkSession.builder
    .appName("ClickstreamEventHubReader")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

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

