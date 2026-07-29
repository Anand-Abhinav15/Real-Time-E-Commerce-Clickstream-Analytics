import random

def weighted_choice(weight_map: dict):
    """
    Select one item using weighted probability,

    i.e: {"Mobile": 60, "Desktop": 30, "Tablet": 10}
    """

    return random.choices(
        population=list(weight_map.keys()),
        weights=list(weight_map.values()),
        k=1
    )[0]


def probability_check(probability: float) -> bool:
    """
    Returns True with the given probability.

    i.e: probability_check(0.8) -> approximately 80% True
    """

    return random.random() < probability


    

