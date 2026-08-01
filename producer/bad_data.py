import random
from datetime import timedelta, datetime

from config.config import (
    ENABLE_BAD_DATA,
    MISSING_USER_PROBAIBLITY,
    DUPLICATE_USER_PROBAIBLITY,
    INVALID_TIMESTAMP_PROBABILITY,
    MISSING_PRODUCT_PROBAIBLITY,
    LATE_EVENT_PROBAIBLITY,
)

class BadDataInjector:
    
    @staticmethod
    def inject(event: dict):
        if not ENABLE_BAD_DATA:
            return event
    
        event = event.copy()

        # Missing User
        if random.random() < MISSING_USER_PROBABILITY:
            event["user_id"] = None

        # Missing Product
        if (
            "product_id" in event
            and random.random() < MISSING_PRODUCT_PROBAIBLITY
        ):
            event["product_id"] = None

        # Invalid Timestamp
        if random.random() < INVALID_TIMESTAMP_PROBABILITY:
            event["event_time"] = "INVALID_TIMESTAMP"

        # Late Event
        if random.random() < LATE_EVENT_PROBABILITY:
            ts = datetime.fromisoformat(event["event_time"])
            event["event_time"] = (ts - timedelta(minutes=15)).isoformat()

        return event 

    @staticmethod
    def should_duplicate():
        if not ENABLE_BAD_DATA:
            return False

        return random.random() < DUPLICATE_EVENT_PROBABILITY
