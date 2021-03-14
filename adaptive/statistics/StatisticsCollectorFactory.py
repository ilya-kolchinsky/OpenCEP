from typing import List
from datetime import timedelta
from base.Pattern import Pattern
from misc import DefaultConfig
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from adaptive.statistics.StatisticsCollector import StatisticsCollector
from adaptive.statistics.StatisticsFactory import StatisticsFactory


class StatisticsCollectorParameters:
    """
    Parameters for the statistics collector
    """
    def __init__(self, statistics_time_window: timedelta = DefaultConfig.STATISTICS_TIME_WINDOW,
                 statistics_types: StatisticsTypes or List[StatisticsTypes] = DefaultConfig.DEFAULT_STATISTICS_TYPE):
        if isinstance(statistics_types, StatisticsTypes):
            statistics_types = [statistics_types]
        self.statistics_types = statistics_types
        self.statistics_time_window = statistics_time_window


class StatisticsCollectorFactory:
    """
    Creates a Statistics Collector given its specification.
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
        Currently, multi-pattern is not supported.
        TODO: To support multi-pattern mode it will need to go through a loop and create statistics for each pattern.
        """
        pattern = patterns[0]
        statistics_time_window = statistics_collector_parameters.statistics_time_window
        statistics_dict = {}
        for stat_type in statistics_collector_parameters.statistics_types:
            stat = StatisticsFactory.create_statistics(pattern, stat_type, statistics_time_window)
            statistics_dict[stat_type] = stat
        return StatisticsCollector(statistics_dict)

    @staticmethod
    def __create_default_statistics_collector_parameters():
        """
        Uses the default configuration to create statistics collector parameters.
        """
        return StatisticsCollectorParameters()
