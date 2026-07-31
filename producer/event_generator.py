import random
import uuid
from datetime import datetime, timedelta

from producer.constants import (
    EVENT_DELAYS,
    TRAFFIC_SOURCES,
)

from producer.personas import get_random_persona
from producer.product_catalog import get_random_product
from  producer.session_generator import SessionGenerator
from producer.user_simulator import UserSimulator
from producer.utils.random_utils import weighted_choice


class EventGenerator:

    def __init__(self):

        self.user_simulator = UserSimulator()
        self.session_generator = SessionGenerator()

    def generate_session(self):

        user = self.user_simulator.get_user()

        persona = get_random_persona()

        journey = self.session_generator.generate_journey(persona)

        session_id = f"S{uuid.uuid4().hex[:10]}"

        traffic_source = weighted_choice(TRAFFIC_SOURCES)

        current_time = datetime.utcnow()

        elapsed_seconds = 0

        events = []

        current_product = None

        for page, event_type in journey:

            delay_min, delay_max = EVENT_DELAYS[event_type]

            elapsed_seconds += random.randint(delay_min, delay_max)

            event_time = current_time + timedelta(seconds= elapsed_seconds)

            # Search doesn't need a product
            if event_type == "search":
                product = None
            elif event_type == "product_view":
                current_product = get_random_product()
                product = current_product
            elif event_type in ("add_to_cart", "checkout", "purchase"):
                if current_product is None:
                    current_product = get_random_product()

                product = current_product
            else:
                product = None

                
                # Cart / Checkout / Purchase should use
                # the last viewed product
                product = current_product


            event = {

                "event_id": str(uuid.uuid4()),
                "event_time": event_time.isoformat(),
                "user_id": user.user_id,
                "session_id": session_id,
                "persona": persona.name,
                "page": page,
                "event_type": event_type,
                "traffic_source": traffic_source,
                "country": user.country,
                "device_type": user.preferred_device,
                "browser": user.preferred_browser,
            }

            if product:
                event.update({
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "category": product.category,
                    "price": product.price,
                })


            if event_type == "purchase" and product is not None:

                quantity = random.randint(1,3)
                event["quantity"] = quantity
                event["revenue"] = round(
                    quantity*product.price, 2,
                )

            events.append(event)

        return events


