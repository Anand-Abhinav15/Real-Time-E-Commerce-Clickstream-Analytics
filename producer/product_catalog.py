from dataclasses import dataclass
from typing import List
import random

from producer.models.product import Product


PRODUCTS: List[Product] = [

    # Electronics
    Product("P1001", "Apple iPhone 16", "Electronics", 999.99),
    Product("P1002", "Samsung Galaxy S26", "Electronics", 899.99),
    Product("P1003", "Sony WH-1000XM5", "Electronics", 299.99),
    Product("P1004", "Apple Watch Series 12", "Electronics", 499.99),
    Product("P1005", "Dell XPS 15", "Electronics", 1699.99),

    # Clothing
    Product("P2001", "Nike Running Shoes", "Clothing", 129.99),
    Product("P2002", "Adidas Hoddie", "Clothing", 79.99),
    Product("P2003", "Levi's Jeans", "Clothing", 69.99),
    Product("P2004", "Puma Sports T-Shirt", "Clothing", 39.99),
    Product("P2005", "Under Armour Jacket", "Clothing", 199.99),

    # Home
    Product("P3001", "Dyson Vacuum Cleaner", "Home", 549.99),
    Product("P3002", "Phillips Air Fryer", "Home", 149.99),
    Product("P3003", "Instant Pot Duo", "Home", 99.99),
    Product("P3004", "IKEA Office Chair", "Home", 189.99),
    Product("P3005", "KitchenAid Mixer", "Home", 399.99),

    # Books
    Product("P4001", "Clean Code", "Books", 34.99),
    Product("P4002", "Designing Data-Intensive Applications", "Books", 49.99),
    Product("P4003", "Atomic Habits", "Books", 21.99),
    Product("P4004", "Deep Learning", "Books", 59.99),
    Product("P4005", "The Pragmatic Programmer", "Books", 42.99),

    # Sports
    Product("P5001", "Wilson Tennis Racket", "Sports", 189.99),
    Product("P5002", "Yonex Badminton Kit", "Sports", 89.99),
    Product("P5003", "Adidas Football", "Sports", 39.99),
    Product("P5004", "Fitness Yoga Mat", "Sports", 24.99),
    Product("P5005", "Decathlon Dumbbells", "Sports", 79.99),
]

def get_random_product() -> Product:
    return random.choice(PRODUCTS)
