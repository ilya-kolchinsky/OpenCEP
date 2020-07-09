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

    # TODO: implement a more accurate comparison operator for events list (prefix? duplications?)
    # TODO: implement a more optimized way for the inclusion check
    def __eq__(self, other):
        for event in self.events:
            if event not in other.events:
                return False
        return True
