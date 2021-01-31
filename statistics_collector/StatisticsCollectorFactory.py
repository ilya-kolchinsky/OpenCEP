from typing import List
from base.Pattern import Pattern
from statistics_collector.StatisticsCollector import StatisticsCollector
from statistics_collector.StatisticsFactory import StatisticsParameters, StatisticsFactory


class StatisticsCollectorParameters:

    def __init__(self, statistics_params: StatisticsParameters = StatisticsParameters()):
        self.statistics_params = statistics_params


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
        pattern = patterns[0]
        statistics = StatisticsFactory.create_statistics(pattern, statistics_collector_parameters.statistics_params)
        return StatisticsCollector(pattern, statistics)

    @staticmethod
    def __create_default_statistics_collector_parameters():
        """
        Uses the default configuration to create statistics collector parameters.
        """
        return StatisticsCollectorParameters()
