from producer.models.persona import Persona
import random


PERSONAS = {

    "casual_browser": Persona(
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


def get_random_persona():

    persona_name= random.choices(
        population= list(PERSONA_WEIGHTS.keys()),
        weights= list(PERSONA_WEIGHTS.values()),
        k=1,
    )[0]

    return PERSONAS[persona_name]
    

