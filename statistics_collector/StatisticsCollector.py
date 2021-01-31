from typing import List

from base.Event import Event
from base.Pattern import Pattern
from statistics_collector.Statistics import Statistics


class StatisticsCollector:
    """
    Collects, maintains and updates statistics from the stream
    """
    def __init__(self, pattern: Pattern, statistics: Statistics):
        self.pattern = pattern
        self.__statistics = statistics

    def event_handler(self, event: Event):
        """
        Updates the statistics with the new event
        """
        self.__statistics.update(event)

    def get_statistics(self):
        return self.__statistics.get_statistics()
