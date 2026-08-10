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

query = (
    events_text.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/clickstream-checkpoint")
    .start()
)

query.awaitTermination()



