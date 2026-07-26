from dataclasses import dataclass

@dataclass(frozen=True)
class Persona:

    name: str

    purchase_probability: float

    search_probability: float

    min_product_views: int

    max_product_views: int

    avg_page_delay: int
