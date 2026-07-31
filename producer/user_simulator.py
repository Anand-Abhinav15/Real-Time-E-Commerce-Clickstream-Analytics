import random
from datetime import datetime

from producer.constants import (
    DEVICE_TYPES,
    COUNTRIES,
    BROWSERS,
)

from producer.utils.random_utils import (
    weighted_choice,
    probability_check,
)

from configs.config import (
    INITIAL_USER_POOL,
    RETURNING_USER_PROBABILITY,
)

from producer.models.user import User

class UserSimulator:

    def __init__(
        self,
        returning_user_probability= 0.80,
    ):

        self.returning_user_probability = returning_user_probability
        self.user_pool = {}
        self.next_user_number = 1000

        for _ in range(INITIAL_USER_POOL):
            self._create_new_user()


    def _create_new_user(self):

        user = User(
            user_id = f"U{self.next_user_number}",
            country=weighted_choice(COUNTRIES),
            preferred_device=weighted_choice(DEVICE_TYPES),
            preferred_browser=weighted_choice(BROWSERS),
            created_at= datetime.utcnow(),
            total_sessions = 0,
        )

        self.user_pool[user.user_id] = user 
        self.next_user_number +=1
        
        return user

    def get_user(self):

        if probability_check(1 - self.returning_user_probability):
            user = self._create_new_user()
        else:
            user = random.choice(list(self.user_pool.values()))

        user.total_sessions += 1

        return user
        

