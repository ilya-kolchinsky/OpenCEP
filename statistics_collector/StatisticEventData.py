from datetime import datetime


class StatisticEventData:
    """
    Contains an event type along with event timestamp
    """
    def __init__(self, timestamp: datetime, event_type: str):
        self.timestamp = timestamp
        self.event_type = event_type
