from producer.models.persona import Persona
import random

from producer.utils.random_utils import weighted_choice

PERSONAS = {

    "casual_browser": Persona(
        persona_id= "P001",
        name= "Casual Browser",
        search_probability= 0.20,
        purchase_probability= 0.05,
        add_to_cart_probability=0.10,
        min_product_views=1,
        max_product_views=3,
        min_delay=5,
        max_delay=20,
    ),

    "window_shopper": Persona(
        persona_id= "P002",
        name= "Window Shopper",
        search_probability=0.50,
        purchase_probability=0.10,
        add_to_cart_probability=0.20,
        min_product_views=3,
        max_product_views=8,
        min_delay= 20,
        max_delay=60,
    ),

    "serious_buyer": Persona(
        persona_id= "P003",
        name= "Serious Buyer",
        search_probability=0.60,
        purchase_probability= 0.90,
        add_to_cart_probability=0.95,
        min_product_views=2,
        max_product_views=5,
        min_delay=30,
        max_delay=90,
    ),

    "impulse_buyer": Persona(
        persona_id= "P004",
        name="Impulse Buyer",
        search_probability=0.10,
        purchase_probability=0.95,
        add_to_cart_probability=1.00,
        min_product_views=1,
        max_product_views=2,
        min_delay=5,
        max_delay=15,
    ),

    "returning_customer": Persona(
        persona_id= "P005",
        name="Returning Customer",
        search_probability=0.05,
        purchase_probability= 0.80,
        add_to_cart_probability=0.90,
        min_product_views=1,
        max_product_views=3,
        min_delay=10,
        max_delay=30,
    ),
}

PERSONA_WEIGHTS = {
    "casual_browser": 35,
    "window_shopper": 25,
    "serious_buyer": 20,
    "impulse_buyer": 10,
    "returning_customer": 10,
}


def get_random_persona():

    persona_name = weighted_choice(PERSONA_WEIGHTS)

    return PERSONAS[persona_name]
    

