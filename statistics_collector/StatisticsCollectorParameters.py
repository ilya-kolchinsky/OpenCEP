from misc import DefaultConfig
from misc.StatisticsTypes import StatisticsTypes

class StatisticsCollectorParameters:
    """
    Parameters required for statistics collector creation.
    """

    def __init__(self, stats_collector_types: StatisticsTypes or List[
        StatisticsTypes] = DefaultConfig.DEFAULT_STATISTICS_COLLECTOR_TYPE):
        if isinstance(stats_collector_types, StatisticsTypes):
            stats_collector_types = [stats_collector_types]
        self.types = stats_collector_types

