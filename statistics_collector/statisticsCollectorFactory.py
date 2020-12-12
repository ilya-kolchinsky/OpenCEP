from misc.StatisticsTypes import StatisticsTypes
from statistics_collector.StatisticsCollector import StatisticsCollector
from misc import DefaultConfig


class StatisticsCollectorParameters:
    """
    Parameters required for statistics collector creation.
    """

    def __init__(self, stats_collector_types: StatisticsTypes or List[
        StatisticsTypes] = DefaultConfig.DEFAULT_STATISTICS_COLLECTOR_TYPE):
        if isinstance(stats_collector_types, StatisticsTypes):
            stats_collector_types = [stats_collector_types]
        self.types = stats_collector_types


class StatisticsFactory:
    """
    Creates a statistics collector given its specification.
    """

    @staticmethod
    def build_statistics_collector(statistics_collector_params: StatisticsCollectorParameters = None):

        if statistics_collector_params is None:
            statistics_collector_params = StatisticsFactory.__create_default_statistics_collector_parameters()

        statistics = []
        for param in statistics_collector_params.types:
            if param == StatisticsTypes.ARRIVAL_RATES:
                statistics.append(ArrivalRates)
            elif param == StatisticsTypes.SELECTIVITY_MATRIX:
                statistics.append(SelectivityMatrix)
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
            pass  # TODO
        elif DefaultConfig.DEFAULT_STATISTICS_COLLECTOR_TYPE == StatisticsTypes.SELECTIVITY_MATRIX:
            pass  # TODO
        raise Exception(
            "Unknown statistics collector type: %s" % (DefaultConfig.DEFAULT_STATISTICS_COLLECTOR_TYPE,))
