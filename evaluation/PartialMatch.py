from typing import List

from base.Event import Event


class PartialMatch:
    """
    A partial match created at some intermediate stage during evaluation.
    """
    def __init__(self, events: List[Event]):
        self.events = events
        self.last_timestamp = max(events, key=lambda x: x.timestamp).timestamp
        self.first_timestamp = min(events, key=lambda x: x.timestamp).timestamp

    def __eq__(self, other):
        if len(self.events) != len(other.events):
            return False
        for (event_self, event_other) in zip(self.events, other.events):
            if event_self not in other.events or event_other not in self.events:
                return False
        return True
