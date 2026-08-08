# Add the producer code which will send data to azure eventhub to simulate real-time data.
import json

from azure.eventhub import EventData, EventHubProducerClient

from configs.azure_configs import (
    EVENTHUB_CONNECTION_STRING,
    EVENTHUB_NAME,
)


class EventHubProducer:

    def __init__(self):
        self.client = EventHubProducerClient.from_connection_string(
            conn_str=EVENTHUB_CONNECTION_STRING,
            eventhub_name=EVENTHUB_NAME,
        )

    def send_events(self, events):
        batch = self.client.create_batch()

        for event in events:
            event_json = json.dumps(event)

            try:
                batch.add(EventData(event_json))

            except ValueError:
                #Batch full
                self.client.send_batch(batch)
                batch = self.client.create_batch()
                batch.add(EventData(event_json))

        if len(batch) > 0:
            self.client.send_batch(batch)

    def close(self):
        self.client.close()







