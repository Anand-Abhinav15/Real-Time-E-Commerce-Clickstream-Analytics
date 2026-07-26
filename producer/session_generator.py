import random

SESSION_PATTERNS = {
    "browser": [
        ("homepage", "homepage_view"),
        ("category", "category_view"),
        ("product", "product_view"),
    ],

    "buyer": [
        ("homepage", "homepage_view"),
        ("category", "category_view"),
        ("product", "product_view"),
        ("cart", "add_to_cart"),
        ("checkout", "checkout"),
        ("confirmation", "purchase"),
    ],

    "abondon_cart": [
        ("homepage", "homepage_view"),
        ("category", "category_view"),
        ("product", "product_view"),
        ("cart", "add_to_cart"),
    ]
}

def generate_session_pattern():
    """
    Returns one realistic customer journey.
    """
    journey = random.choices(
        population= ["browser", "buyer", "abondon_cart"],
        weights= [50, 30, 20],
        k= 1
    )[0]

    return journey, SESSION_PATTERNS[journey]

    








