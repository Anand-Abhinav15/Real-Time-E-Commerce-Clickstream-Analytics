import json

from producer.event_generator import EventGenerator

generator = EventGenerator()

events = generator.generate_session()

for event in events:
    print(json.dumps(event, indent=4))



