import random
from datetime import datetime

from producer.constants import (
    DEVICE_TYPES,
    COUNTRIES,
    BROWSERS,
)

from producer.models.user import User

class UserSimulator:

    def __init__(
        self,
        initial_user_pool= 1000,
        returning_user_probability= 0.80,
    ):

        self.returning_user_probability = returning_user_probability
        self.user_pool = {}
        self.next_user_number = 1000

        for _ in range(initial_user_pool):
            self._create_new_user()

    def _weighted_choice(self, choices: dict):

        return random.choices(
            population= list(choices.keys()),
            weights = list(choices.values()),
            k= 1,
        )[0]

    def _create_new_user(self):

        user = User(
            user_id = f"U{self.next_user_number}",
            country = self._weighted_choice(COUNTRIES),
            preferred_device = self._weighted_choice(DEVICE_TYPES),
            preferred_browser= self._weighted_choice(BROWSERS),
            created_at= datetime.utcnow(),
            total_sessions = 0,
        )

        self.user_pool[user.user_id] = user 
        self.next_user_number +=1
        
        return user

    def get_user(self):

        create_new = (
            random.random()
            > self.returning_user_probability
        )

        if create_new:
            user = self._create_new_user()
        else:
            user = random.choice(
                list(self.user_pool.values())
            )

        user.total_sessions += 1

        return user
        

