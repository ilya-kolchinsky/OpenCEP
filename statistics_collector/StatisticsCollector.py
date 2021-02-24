from base.Event import Event
from base.Pattern import Pattern
from misc.StatisticsTypes import StatisticsTypes


class StatisticsCollector:
    """
    Collects, maintains and updates statistics from the stream
    """

    def __init__(self, pattern: Pattern, statistics: dict):
        self.pattern = pattern
        self.__statistics = statistics

    def event_handler(self, event: Event):
        """
        Update all relevant statistics with the new event
        """
        for statistics in self.__statistics:
            statistics.update_by_event(event)

    def get_statistics(self):
        return self.__statistics

    def statistics_types(self):
        return self.__statistics.keys()

    def update_specific_statistics(self, statistics_type: StatisticsTypes, data):
        pass