from datetime import timedelta, datetime
from base.Event import Event


# class StatisticEventData:
#
#     def __init__(self, timestamp: timedelta, event_type: str):
#         self.timestamp = timestamp
#         self.event_type = event_type


class StatisticEventData:
    """
    Contains an event type along with event timestamp
    """
    def __init__(self, time: datetime, event_type: str):
        self.timestamp = time
        self.event_type = event_type
