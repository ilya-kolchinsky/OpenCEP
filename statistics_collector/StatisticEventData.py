from datetime import timedelta, datetime
from base.Event import Event


class StatisticEventData:
    """
    Contains an event type along with event timestamp
    """
    def __init__(self, time: datetime, event_type: str):
        self.timestamp = time
        self.event_type = event_type
