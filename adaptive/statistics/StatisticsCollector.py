from base.Event import Event
from adaptive.statistics.StatisticsTypes import StatisticsTypes


class StatisticsCollector:
    """
    Collects, maintains and updates statistics from the stream
    """

    def __init__(self, statistics: dict):
        self.__statistics = statistics

    def handle_event(self, event: Event):
        """
        Handles events directly from the stream.
        Currently only arrival rates statistics handles the events
        """
        self.update_statistics_by_type(StatisticsTypes.ARRIVAL_RATES, event)

    def get_statistics(self):
        """
        Returns a dictionary containing the statistics types and the raw statistics accordingly.
        """
        return {statistics_type: statistics.get_statistics() for statistics_type, statistics in
                self.__statistics.items()}

    def update_statistics_by_type(self, statistics_type: StatisticsTypes, data):
        """
        This method exists because there are statistics(like selectivity)
        that are updated not based on events from the stream directly.
        """
        if statistics_type in self.__statistics:
            self.__statistics[statistics_type].update(data)
