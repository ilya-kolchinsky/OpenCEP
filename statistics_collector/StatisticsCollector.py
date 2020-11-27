
from base.Event import Event
from misc.StatisticsTypes import StatisticsTypes
from optimizer import Optimizer
from statistics_collector.ArrivalRates import ArrivalRates
from statistics_collector.Frequency import Frequency
from statistics_collector.SelectivityMatrixAndArrivalRates import SelectivityMatrixAndArrivedRates

class StatisticsCollector:

    def __init__(self, optimizer: Optimizer, statistic_types: StatisticsTypes, pattern: Pattern):
        self.__optimizer = optimizer
        # self.__statistic_types = statistic_types
        # self.currStatistics = StatisticsFactory(statistic_types)  # functions or objects????
        event_types = {primitive_event.eventType for primitive_event in pattern.positive_structure.args}
        self.__statistics = StatisticsFactory.register_statistics_type(statistic_type, event_types)  # refractor this name!

    def event_handler(self, event: Event):
        for stat in statistics:
            stat.update(event)


class StatisticsFactory:

    @staticmethod
    def register_statistics_type(statistic_types, event_types):
        for stat_type in statistic_types:
            if stat_type == StatisticsTypes.ARRIVAL_RATES:
                self.stats.append(ArrivalRates())

            elif stat_type == StatisticsTypes.FREQUENCY_DICT:
                self.stats.append(Frequency(event_types))

            elif stat_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
                self.stats.append(SelectivityMatrixAndArrivedRates())

        return self.stats


