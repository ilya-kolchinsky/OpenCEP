from datetime import timedelta
from base.Pattern import Pattern
from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes
from statistics_collector.Statistics import SelectivityAndArrivalRatesStatistics, SelectivityStatistics, \
    ArrivalRatesStatistics


class StatisticsFactory:
    """
    Creates a statistics collector given its specification.
    """
    @staticmethod
    def create_statistics(pattern: Pattern, stat_type: StatisticsTypes, time_window: timedelta):
        if stat_type == StatisticsTypes.ARRIVAL_RATES:
            return ArrivalRatesStatistics(time_window, pattern)
        if stat_type == StatisticsTypes.SELECTIVITY_MATRIX:
            return SelectivityStatistics(pattern)
        raise Exception("Unknown statistics type: %s" % (StatisticsTypes.stat_type,))
