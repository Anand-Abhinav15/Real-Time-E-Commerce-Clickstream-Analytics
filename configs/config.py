"""
    Runtime configuration for the simulator
"""

# ==========================================
# USER SIMULATION
# ==========================================

INITIAL_USER_POOL = 1000

RETURNING_USER_PROBABILITY = 0.80

# ==========================================
# DATA QUALITY SIMULATION
# ==========================================

ENABLE_BAD_DATA = True

MISSING_USER_PROBABILITY = 0.01

DUPLICATE_EVENT_PROBABILITY = 0.01

INVALID_TIMESTAMP_PROBABILITY = 0.005

MISSING_PRODUCT_PROBABILITY = 0.005

LATE_EVENT_PROBABILITY = 0.05

# ==========================================
# FEATURE FLAGS
# ==========================================

ENABLE_LATE_EVENTS = False


# ==========================================
# EVENT GENERATION
# ==========================================

EVENTS_PER_SECOND = 2