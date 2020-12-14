from base.PatternStructure import PatternStructure, PrimitiveEventStructure
from base.Event import Event
from collections import Counter
from base.Pattern import Pattern
from datetime import timedelta
from statistics_collector.EventTime import EventTime


class ArrivalRates:
    def __init__(self, patterns: Pattern):
        self.event_types = patterns.get_all_event_types()
        self.arrival_rates = {}
        self.events_arrival_time = []
        self.window = patterns.window
        for event in self.event_types:
            self.arrival_rates[event] = 0

    def update(self, event: Event):
        event_type = event.type
        if event_type in self.event_types:
            self.arrival_rates[event_type] += 1
            self.events_arrival_time.append(EventTime(event.timestamp, event_type))
        self.__remove_expired_events(event.timestamp)

    def __remove_expired_events(self, timestamp: timedelta):
        for i, event_time in enumerate(self.events_arrival_time):
            if timestamp - event_time.timestamp > self.window:
                self.arrival_rates[event_time.event_type] -= 1
            else:
                self.events_arrival_time = self.events_arrival_time[i:]
                break
