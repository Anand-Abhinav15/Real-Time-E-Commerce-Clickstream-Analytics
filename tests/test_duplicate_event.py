import json
import os

from azure.eventhub import EventHubProducerClient, EventData


EVENT_HUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENTHUB_NAME")


if not EVENT_HUB_CONNECTION_STRING:
    raise RuntimeError("EVENT_HUB_CONNECTION_STRING is not set.")

if not EVENT_HUB_NAME:
    raise RuntimeError("EVENT_HUB_NAME is not set.")


event = {
    "event_id": "DEDUP-TEST-001",
    "event_time": "2026-08-23T14:30:00",
    "user_id": "U9999",
    "session_id": "SDEDUP001",
    "persona": "Serious Buyer",
    "page": "product",
    "event_type": "product_view",
    "traffic_source": "Direct",
    "country": "India",
    "device_type": "Desktop",
    "browser": "Chrome",
    "product_id": "P1001",
    "product_name": "Dedup Test Product",
    "category": "Test",
    "price": 99.99,
}


producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STRING,
    eventhub_name=EVENT_HUB_NAME,
)


with producer:
    batch = producer.create_batch()

    # SAME EVENT TWICE
    batch.add(EventData(json.dumps(event)))
    batch.add(EventData(json.dumps(event)))

    producer.send_batch(batch)


print("Sent the same event twice.")
print(f"event_id: {event['event_id']}")


