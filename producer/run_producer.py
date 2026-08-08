import time

from producer.event_generator import EventGenerator
from producer.eventhub_producer import EventHubProducer

generator = EventGenerator()
producer = EventHubProducer()

print("Starting Clickstream Producer......\n")

try:
    while True:
        events = generator.generate_session()
        producer.send_events(events)
        print(f"Sent {len(events)} events")
        time.sleep(2)

except KeyboardInterrupt:
    print("\nStopping producer.....")

finally:
    producer.close()
    
