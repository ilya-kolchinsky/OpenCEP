from base.Event import Event
from typing import List


class PatternMatch:
    """
    This class's instances are the output results of an evaluation mechanism's eval function.
    It has one field which is the list of events in the pattern match.
    """

    def __init__(self, events: List[Event]):
        self.events = events

    def __repr__(self):
        return "Pattern Match has Events: {}".format(self.events)
