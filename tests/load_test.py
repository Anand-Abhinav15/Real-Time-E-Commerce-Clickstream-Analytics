import time

from producer.event_generator import EventGenerator
from producer.eventhub_producer import EventHubProducer


TOTAL_SESSIONS = 100


generator = EventGenerator()
producer = EventHubProducer()

total_events = 0

print("\nStarting Clickstream Load Test...")
print(f"Target sessions: {TOTAL_SESSIONS}\n")

start_time = time.time()

try:
    for session_number in range(1, TOTAL_SESSIONS + 1):

        events = generator.generate_session()

        producer.send_events(events)

        total_events += len(events)

        print(
            f"Session {session_number}/{TOTAL_SESSIONS} "
            f"-> {len(events)} events "
            f"| Total: {total_events}"
        )

        # Small pause so Spark/Event Hubs can keep up
        time.sleep(0.1)

finally:
    producer.close()

elapsed = time.time() - start_time

print("\n====================================")
print("LOAD TEST COMPLETE")
print("====================================")
print(f"Sessions generated : {TOTAL_SESSIONS}")
print(f"Events generated   : {total_events}")
print(f"Elapsed time       : {elapsed:.2f} seconds")
print(f"Events/sec         : {total_events / elapsed:.2f}")