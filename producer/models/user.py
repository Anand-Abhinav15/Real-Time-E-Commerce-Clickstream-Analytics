from dataclasses import dataclass
from datetime import datetime

@dataclass
class User:
    """
    Represents a customer.

    User attributes persists across multiple sessions.
    """

    user_id: str
    country: str
    preferred_device: str
    preferred_browser: str
    created_at: datetime
    total_session: int = 0

