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
        self.pattern_id = []

    def __eq__(self, other):
        return isinstance(other, PatternMatch) and set(self.events) == set(other.events) and self.pattern_id == other.pattern_id

    def __str__(self):
        result = ""
        match = ""
        for event in self.events:
            match += "%s\n" % event
        if len(self.pattern_id) == 0:
            result += match
            result += "\n"
        else:
            for idx in self.pattern_id:
                result += "%s: " % idx
                result += match
                result += "\n"
        return result

    def add_pattern_id(self, idx: int):
        self.pattern_id.append(idx)

