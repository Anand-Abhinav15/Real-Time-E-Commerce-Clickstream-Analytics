import random

from producer.models.persona import Persona


class SessionGenerator:
    """
    Builds a customer journey based on the selected persona.
    """

    def generate_journey(self, persona: Persona):

        journey = []

        # Homepage
        journey.append("homepage", "homepage_view")

        # Optional Search
        if random.random() < persona.search_probability:
            journey.append(("search", "search"))

        # Category Page
        journey.append(("category", "category_view"))

        # Product Views
        num_products = random.randint(
            persona.min_product_views,
            persona.max_product_views,
        )

        for _ in range(num_products):
            journey.append(("product", "product_view"))

        # Cart
        if random.random() < persona.add_to_cart_probability:
            journey.append(("cart", "add_to_cart"))

            # Checkout
            if random.random() < persona.purchase_probability:
                journey.append(("checkout", "checkout"))
                journey.append(("payment", "purchase"))
    








