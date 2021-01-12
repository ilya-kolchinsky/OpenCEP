from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes
from statistics_collector.NewStatistics import SelectivityAndArrivalRatesStatistics, SelectivityStatistics, \
    ArrivalRatesStatistics


class StatisticsParameters:
    """
    Parameters required for statistics creation.
    """

    def __init__(self, stat_type: StatisticsTypes = DefaultConfig.DEFAULT_STATISTICS_TYPE):
        self.stat_type = stat_type
        # self.window_time = window_time
        # need to add the constructor some window time


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
    def create_statistics(statistics_params: StatisticsParameters):
        if statistics_params.stat_type == StatisticsTypes.ARRIVAL_RATES:
            return ArrivalRatesStatistics()
        if statistics_params.stat_type == StatisticsTypes.SELECTIVITY_MATRI:
            return SelectivityStatistics()
        if statistics_params.stat_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            return SelectivityAndArrivalRatesStatistics()
        raise Exception("Unknown statistics type: %s" % (statistics_params.stat_type,))
