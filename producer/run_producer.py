import time

from producer.event_generator import EventGenerator
from producer.eventhub_producer import EventHubProducer
from configs.config import EVENTS_PER_SECOND


generator = EventGenerator()
producer = EventHubProducer()

print("Starting Clickstream Producer......\n")
print(f"Target event rate: {EVENTS_PER_SECOND} events/sec")

interval = 1 / EVENTS_PER_SECOND

try:
    while True:
        events = generator.generate_session()

        producer.send_events(events)

        print(f"Sent {len(events)} events")

        time.sleep(interval)

except KeyboardInterrupt:
    print("\nStopping producer.....")

finally:
    producer.close()