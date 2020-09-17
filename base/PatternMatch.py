from base.Event import Event
from typing import List


class PatternMatch:
    """
    Represents a set of primitive events satisfying a pattern.
    An instance of this class could correspond either to a full pattern match, or to any intermediate result
    created during the evaluation process.
    """
    def __init__(self, events: List[Event]):
        self.events = events
        self.last_timestamp = max(events, key=lambda x: x.timestamp).timestamp
        self.first_timestamp = min(events, key=lambda x: x.timestamp).timestamp

    def __eq__(self, other):
        return isinstance(other, PatternMatch) and set(self.events) == set(other.events)

    def __str__(self):
        result = ""
        for event in self.events:
            result += "%s\n" % event
        result += "\n"
        return result
