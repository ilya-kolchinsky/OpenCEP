from datetime import timedelta
from base.Event import Event

class EventTime:

    def __init__(self, timestamp: timedelta, event_type): # TODO type of event_type
        self.timestamp = timestamp
        self.event_type = event_type