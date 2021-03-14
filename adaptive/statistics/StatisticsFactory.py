from datetime import timedelta
from base.Pattern import Pattern
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from adaptive.statistics.Statistics import SelectivityStatistics, ArrivalRatesStatistics


class StatisticsFactory:
    """
    Creates a statistics collector given its specification.
    """

    @staticmethod
    def create_statistics(pattern: Pattern, stat_type: StatisticsTypes, statistics_time_window: timedelta):
        predefined_statistics = None
        if pattern.statistics and stat_type in pattern.statistics:
            predefined_statistics = pattern.statistics[stat_type]

        if stat_type == StatisticsTypes.ARRIVAL_RATES:
            return ArrivalRatesStatistics(statistics_time_window, pattern, predefined_statistics)
        if stat_type == StatisticsTypes.SELECTIVITY_MATRIX:
            return SelectivityStatistics(pattern, predefined_statistics)
        raise Exception("Unknown statistics type: %s" % (StatisticsTypes.stat_type,))
