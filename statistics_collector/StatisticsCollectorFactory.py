from typing import List

from base.Pattern import Pattern
from misc import DefaultConfig
from statistics_collector.StatisticsCollector import StatisticsCollector
from statistics_collector.StatisticsFactory import StatisticsParameters, StatisticsFactory


class StatCollectorParameters:

    def __init__(self, statistics_params: StatisticsParameters = StatisticsParameters()):
        self.statistics_params = statistics_params


class StatCollectorFactory:
    """
    Creates an Statistic Collector given its specification.
    """

    @staticmethod
    def build_statistics_collector(statistics_collector_parameters: StatCollectorParameters, patterns: List[Pattern]):
        if statistics_collector_parameters is None:
            statistics_collector_parameters = StatCollectorFactory.__create_default_statistics_collector_parameters()
        return StatCollectorFactory.__create_statistics_collector(statistics_collector_parameters, patterns)

    @staticmethod
    def __create_statistics_collector(statistics_collector_parameters: StatCollectorParameters,
                                      patterns: List[Pattern]):
        """
        Currently, we only maintain one pattern.
        Next you will need to go through a loop and for each pattern create statistics.
        after that, send dictionary[key: pattern, value: statistics] to statistic collector
        Note: nested patterns are not yet supported.
        """
        pattern = patterns[0]
        statistics = StatisticsFactory.create_statistics(pattern, statistics_collector_parameters.statistics_params)
        return StatisticsCollector(pattern, statistics)

    @staticmethod
    def __create_default_statistics_collector_parameters():
        """
        Uses the default configuration to create statistics collector parameters.
        """
        return StatCollectorParameters()
