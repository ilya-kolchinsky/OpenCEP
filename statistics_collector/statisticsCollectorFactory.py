from misc.StatisticsTypes import StatisticsTypes
from statistics_collector.StatisticsCollector import StatisticsCollector
from statistics_collector.StatisticsCollectorParameters import StatisticsCollectorParameters
from statistics_collector.StatisticsObjects import ArrivalRates
from misc import DefaultConfig
from base.Pattern import Pattern


class StatisticsFactory:
    """
    Creates a statistics collector given its specification.
    """

    @staticmethod
    def build_statistics_collector(statistics_collector_params: StatisticsCollectorParameters,
                                   patterns: Pattern):

        if statistics_collector_params is None:
            statistics_collector_params = StatisticsFactory.__create_default_statistics_collector_parameters()

        statistics = []
        for param in statistics_collector_params.types:
            if param == StatisticsTypes.ARRIVAL_RATES:
                statistics.append(ArrivalRates(patterns))
            elif param == StatisticsTypes.SELECTIVITY_MATRIX:
                statistics.append(SelectivityMatrix(patterns))
            else:
                raise Exception("Unknown statistics collector type: %s" % (param,))
        return StatisticsCollector(statistics)

    @staticmethod
    def __create_default_statistics_collector_parameters():
        """
        Uses the default configuration to create statistics collector parameters.
        """
        if DefaultConfig.DEFAULT_STATISTICS_COLLECTOR_TYPE == StatisticsTypes.NO_STATISTICS:
            return None
        if DefaultConfig.DEFAULT_STATISTICS_COLLECTOR_TYPE == StatisticsTypes.ARRIVAL_RATES:
            return StatisticsCollectorParameters(StatisticsTypes.ARRIVAL_RATES)
        elif DefaultConfig.DEFAULT_STATISTICS_COLLECTOR_TYPE == StatisticsTypes.SELECTIVITY_MATRIX:
            return StatisticsCollectorParameters(StatisticsTypes.SELECTIVITY_MATRIX)
        raise Exception(
            "Unknown statistics collector type: %s" % (DefaultConfig.DEFAULT_STATISTICS_COLLECTOR_TYPE,))
