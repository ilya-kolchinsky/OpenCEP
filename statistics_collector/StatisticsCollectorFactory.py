from typing import List
from datetime import timedelta
from base.Pattern import Pattern
from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes
from statistics_collector.StatisticsCollector import StatisticsCollector
from statistics_collector.StatisticsFactory import StatisticsFactory


class StatisticsCollectorParameters:

    def __init__(self, time_window: timedelta = timedelta(minutes=2), statistics_types: StatisticsTypes or List[StatisticsTypes] = DefaultConfig.DEFAULT_STATISTICS_TYPE):
        if isinstance(statistics_types, StatisticsTypes):
            statistics_types = [statistics_types]
        self.statistics_types = statistics_types
        self.time_window = time_window

class StatisticsCollectorFactory:
    """
    Creates an Statistic Collector given its specification.
    """

    @staticmethod
    def build_statistics_collector(statistics_collector_parameters: StatisticsCollectorParameters, patterns: List[Pattern]):
        if statistics_collector_parameters is None:
            statistics_collector_parameters = StatisticsCollectorFactory.__create_default_statistics_collector_parameters()
        return StatisticsCollectorFactory.__create_statistics_collector(statistics_collector_parameters, patterns)

    @staticmethod
    def __create_statistics_collector(statistics_collector_parameters: StatisticsCollectorParameters,
                                      patterns: List[Pattern]):
        """
        Currently, multi-pattern os not supported.
        TODO: To support multi-pattern it will need to go through a loop and create statistics for each pattern.
        """
        statistics_dict = {}
        pattern = patterns[0]
        time_window = statistics_collector_parameters.time_window
        for stat_type in statistics_collector_parameters.statistics_types:
            stat = StatisticsFactory.create_statistics(pattern, stat_type, time_window)
            statistics_dict[stat_type] = stat
        return StatisticsCollector(pattern, statistics_dict)

    @staticmethod
    def __create_default_statistics_collector_parameters():
        """
        Uses the default configuration to create statistics collector parameters.
        """
        return StatisticsCollectorParameters()
