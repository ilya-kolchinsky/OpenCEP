from collections import Counter, deque

from base import Event


class Frequency:

    def __init__(self, event_types: Pattern.types):  ## Pattern.Types - represent the types in the current pattern
        self.event_types_counter = dict.fromkeys(event_types, 0)
        self.my_Events = []
        # self.statistics = Counter(event_types)


    def update(self, event: Event):
        for ev in event:
            self.event_types_counter[ev.type] += 1
            my_Events.append(MyEvent(ev.timestamp, ev.type))


class MyEvent:

    def __init__(self, timestamp: timestamp, type: type):
        self.dateTime = timestamp
        self.event_type = type
