from datetime import timedelta

# DEVICE DISTRIBUTION

DEVICE_TYPES = {
    "Mobile": 62,
    "Desktop": 30,
    "Tablet": 8,
}

# TRAFFIC SOURCES

TRAFFIC_SOURCES = {
    "Google": 40,
    "Organic": 20,
    "Direct": 20,
    "Facebook": 10,
    "Instagram": 5,
    "Email": 5,
}

# COUNTRY DISTRIBUTION

COUNTRIES = {
    "India": 45,
    "United States": 20,
    "United Kingdom": 10,
    "Germany": 10,
    "Canada": 10,
    "Australia": 5,
}

# BROWSER DISTRIBUTION

BROWSERS = {
    "Chrome": 65,
    "Edge": 15,
    "Safari": 10,
    "Firefox": 10,
}

# EVENT DELAYS (SECONDS)

EVENT_DELAYS = {

    "homepage_view": (5, 20),

    "search": (5, 25),

    "category_view": (10, 30),

    "product_view": (20, 120),

    "add_to_cart": (10, 45),

    "checkout": (30, 180),

    "purchase": (30, 120),
}

# BAD DATA SETTINGS

BAD_DATA = {

    "missing_user_probability": 0.01,

    "duplicate_probability": 0.01,

    "invalid_timestamp_probability": 0.005,

    "missing_product_probability": 0.005,
}

# LATE EVENT SETTINGS

LATE_EVENTS = {

    "enabled": True,

    "late_probability": 0.05,

    "max_delay": timedelta(minutes=15),
}