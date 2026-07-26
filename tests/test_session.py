from producer.session_generator import generate_session_pattern

for _ in range(10):
    journey, events = generate_session_pattern()

    print(f"\nJourney: {journey}")

    for page, event in events:
        print(page, "->", event)

        