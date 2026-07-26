import random
import uuid
from datetime import datetime, timedelta

from producer.product_catalog import get_random_product
from producer.session_generator import generate_session_pattern


DEVICE_TYPES = [
    "Mobile",
    "Desktop",
    "Tablet"
]

TRAFFIC_SOURCES = [
    "Google",
    "Facebook",
    "Instagram",
    "Direct",
    "Organic"
]

COUNTRIES = [
    "India",
    "United States",
    "United Kingdom",
    "Germany",
    "Canada"
]

BROWSERS = [
    "Chrome",
    "Edge",
    "Firefox",
    "Safari"
]

class EventGenerator:

    def __init__(self):
        pass

    def generate_session(self):
        """
        Generates one complete user session.
        Returns a list of clickstream events.
        """

        user_id = f"U{random.randint(1000, 9999)}"
        session_id = f"S{uuid.uuid4().hex[:10]}"

        device = random.choice(DEVICE_TYPES)
        source = random.choice(TRAFFIC_SOURCES)
        country = random.choice(COUNTRIES)
        browser = random.choice(BROWSERS)

        journey_name, journey = generate_session_pattern()

        product = get_random_product()

        current_time = datetime.utcnow()

        events = []

        for index, (page, event_type) in enumerate(journey):

            event = {

                "event_id": str(uuid.uuid4()),
                "event_time" : (
                    current_time +
                    timedelta(seconds=index*random.randint(3, 12))
                ).isoformat(),
                "user_id": user_id,
                "session_id": session_id,
                "page": page,
                "event_type": event_type,
                "product_id": product.product_id,
                "product_name": product.product_name,
                "category": product.category,
                "price": product.price,
                "device_type": device,
                "traffic_source": source,
                "country": country,
                "browser": browser

            }

            event.append(event)
        
        return events





