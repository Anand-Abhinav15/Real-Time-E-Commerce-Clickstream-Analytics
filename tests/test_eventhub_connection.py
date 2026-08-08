from producer.event_generator import EventGenerator
from producer.eventhub_producer import EventHubProducer

generator = EventGenerator()
producer = EventHubProducer()

events = generator.generate_session()

producer.send_events(events)

producer.close()

print(f"Successfully sent {len(events)} events to Event Hub.")



