from dataclasses import dataclass

@dataclass(frozen=True)
class Persona:
    """
    Represents a customer behaviour profile
    """
    persona_id: str
    name: str

    # Probability of performing a search before browsing
    search_probability: float

    # Probability of purchasing after browsing
    purchase_probability: float

    # Probability of adding a product to cart
    add_to_cart_probability: float

    # Number of products typically viewed
    min_product_views: int
    max_product_views: int

    # Typical delay between events (seconds)
    min_delay: int
    max_delay: int

