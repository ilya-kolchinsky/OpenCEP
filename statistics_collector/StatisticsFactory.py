from datetime import timedelta
from base.Pattern import Pattern
from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes
from statistics_collector.Statistics import SelectivityAndArrivalRatesStatistics, SelectivityStatistics, \
    ArrivalRatesStatistics


class StatisticsParameters:
    """
    Parameters required for statistics creation.
    """

    def __init__(self, time_window: timedelta = timedelta(seconds=30), stat_type: StatisticsTypes = DefaultConfig.DEFAULT_STATISTICS_TYPE):
        self.stat_type = stat_type
        self.time_window = time_window


class ArrivalRateStatisticsParameters(StatisticsParameters):
    """
    implement this class if need
    """
    pass


class SelectivityMatrixStatisticsParameters(StatisticsParameters):
    """
    implement this class if need
    """
    pass


class StatisticsFactory:
    """
    Creates a statistics collector given its specification.
    """
    @staticmethod
    def create_statistics(pattern: Pattern, statistics_params: StatisticsParameters):
        if statistics_params.stat_type == StatisticsTypes.ARRIVAL_RATES:
            return ArrivalRatesStatistics(statistics_params.time_window, pattern)
        if statistics_params.stat_type == StatisticsTypes.SELECTIVITY_MATRIX:
            return SelectivityStatistics(pattern)
        if statistics_params.stat_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            return SelectivityAndArrivalRatesStatistics(ArrivalRatesStatistics(statistics_params.time_window, pattern),
                                                        SelectivityStatistics(pattern))
        raise Exception("Unknown statistics type: %s" % (statistics_params.stat_type,))
