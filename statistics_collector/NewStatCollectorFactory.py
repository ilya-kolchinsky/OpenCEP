from misc import DefaultConfig
from statistics_collector.StatisticsCollector import StatisticsCollector
from statistics_collector.NewStatisticsFactory import StatisticsParameters, StatisticsFactory


class StatCollectorParameters:

    def __init__(self, statistics_params: StatisticsParameters = StatisticsParameters()):
        self.statistics_params = statistics_params


class StatCollectorFactory:
    """
    Creates an Statistic Collector given its specification.
    """

    @staticmethod
    def build_statistics_collector(statistics_collector_parameters: StatCollectorParameters):
        if statistics_collector_parameters is None:
            statistics_collector_parameters = StatCollectorFactory.__create_default_statistics_collector_parameters()
        return StatCollectorFactory.__create_statistics_collector(statistics_collector_parameters)

    @staticmethod
    def __create_statistics_collector(statistics_collector_parameters: StatCollectorParameters):

        statistics = StatisticsFactory.create_statistics(statistics_collector_parameters.statistics_params)
        return StatisticsCollector(statistics)

    @staticmethod
    def __create_default_statistics_collector_parameters():
        """
        Uses the default configuration to create statistics collector parameters.
        """
        return StatCollectorParameters()


