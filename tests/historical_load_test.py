import random
import sys
from pathlib import Path 
from datetime import date, datetime, timedelta

# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from producer.event_generator import EventGenerator 
from producer.eventhub_producer import EventHubProducer


#Config

# Existing data currently goes up to approximately Sep 3.
# We generate additional realistic synthetic activity for
# Sep 4 -> Sep 7 so Spark's streaming watermark will accept it.

DAILY_SESSIONS = {
    date(2026, 9, 4): 500,
    date(2026, 9, 5): 800,
    date(2026, 9, 6): 650,
    date(2026, 9, 7): 1000,
}


#Helpers

def parse_timestamp(value):
    """Convert an ISO timestamp into a datetime object."""

    if isinstance(value, datetime):
        return value 
    
    if not isinstance(value, str):
        return None 

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None 
    

def shift_events_to_date(events, target_date):
    """
    Move all valid event_time values in a generated session
    to the requested historical date.

    ingestion_timestamp is intentionally NOT modified.
    Invalid/missing event_time values are left untouched so
    the existing data-quality/quarantine logic can process them.
    """

    #Find the first valid timestamp to use as the session anchor. 
    base_timestamp = None 

    for event in events:
        event_time = event.get("event_time")

        parsed = parse_timestamp(event_time)

        if parsed is not None: 
            base_timestamp = parsed
            break
            
    if base_timestamp is None:
        return events 


    #Pick a realistic time during the day
    target_hour = random.randint(9, 19)
    target_minute = random.randint(0, 59)

    target_timestamp = base_timestamp.replace(
        year = target_date.year,
        month = target_date.month,
        day = target_date.day,
        hour = target_hour,
        minute = target_minute,
        second = random.randint(0, 59),
        microsecond = 0, 
    )

    shift = target_timestamp - base_timestamp

    for event in events:
        event_time = event.get("event_time")
        parsed = parse_timestamp(event_time)

        #Leave invalid/missing timestamps  untouched
        if parsed is None:
            continue 

        shifted_timestamp = parsed + shift

        event["event_time"] = shifted_timestamp.isoformat()

    return events 


#Main Load Test 

def main():

    generator = EventGenerator()
    producer = EventHubProducer()

    total_sessions = 0
    total_events = 0

    print("=" *70)
    print("HISTORICAL CLICKSTREAM LOAD TEST")
    print("=" *70)

    print("\nTotal dates:")

    for target_date, sessions in DAILY_SESSIONS.items():
        print(f" {target_date} -> {sessions:,} sessions")

    print("\nStarting load...\n")

    try:

        for target_date, session_count in DAILY_SESSIONS.items():

            daily_events = 0

            print("-"*70)
            print(f"Generating data for {target_date}")
            print(f"Sessions: {session_count:,}")
            print("-"*70)

            for session_number in range(1, session_count + 1):

                # Generate a normal session using the existing
                # clickstream generator.
                events = generator.generate_session()

                # Move its event timestamps to the target date. 
                events = shift_events_to_date(
                    events, target_date,
                )

                # Send through the EXISTING Event Hub pipeline. 
                producer.send_events(events)

                event_count = len(events)

                daily_events += event_count
                total_events += event_count
                total_sessions += 1

                if session_number % 100 == 0:

                    print(
                        f" Progress: "
                        f"{session_number:,}/{session_count:,} session | "
                        f"{daily_events:,} events"
                    )

            print(
                f":\nCompleted {target_date}: "
                f"{session_count:,} sessions | "
                f"{daily_events:,} events\n"
            )

    except KeyboardInterrupt:
        print("\nLoad test interrupted by user.")

    finally:
        producer.close()

    
    print("=" * 70)
    print("LOAD TEST COMPLETE")
    print("=" * 70)

    print(f"Total sessions : {total_sessions:,}")
    print(f"Total events   : {total_events:,}")


if __name__ == "__main__":
    main()
