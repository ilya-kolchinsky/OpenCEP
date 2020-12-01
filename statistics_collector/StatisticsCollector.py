

from base.Event import Event
from misc.StatisticsTypes import StatisticsTypes
from optimizer import Optimizer
from statistics_collector.ArrivalRates import ArrivalRates
from statistics_collector.FrequencyDict import FrequencyDict
from statistics_collector.SelectivityMatrixAndArrivalRates import SelectivityMatrixAndArrivedRates

class StatisticsCollector:

    def __init__(self, optimizer: Optimizer, statistic_types: StatisticsTypes):
        # self.statistics_factory = StatisticsFactory()
        self.__optimizer = optimizer
        self.__statistic_types = statistic_types

        self.currStatistics = StatisticsFactory(statistic_types)  # functions or objects????

        #self.__calculator = self.statistics_factory.register_statistics_type(statistic_type)

    def handle_event(self, event: Event):
        type = event.type

        pass

    def register_statistics_type(self):
        if self.__statistic_type == StatisticsTypes.ARRIVAL_RATES:
            return self.__arrival_rates
        elif self.__statistic_type == StatisticsTypes.FREQUENCY_DICT:
            return self.__frequency_dict
        elif self.__statistic_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            return self.__selectivity_matrix_and_arrival_rates


class StatisticsFactory:

    def __init__(self, statistic_type):
        self.register_statistics_type(statistic_type)
        self.stats = []

    # def __init__(self):
    #     self.stats = []

    def register_statistics_type(self, statistic_type):
        for stat_type in statistic_type:
            if stat_type == StatisticsTypes.ARRIVAL_RATES:
                self.stats.append(ArrivalRates())
                # self.statistics.append(self.__arrival_rates)

            elif stat_type == StatisticsTypes.FREQUENCY_DICT:
                self.stats.append(FrequencyDict())
                # self.statistics.append(self.__frequency_dict)


            elif stat_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
                self.stats.append(SelectivityMatrixAndArrivedRates())
                # self.statistics.append(self.__selectivity_matrix_and_arrival_rates)

        return self.stats


# class ArrivalRates():
#     def __init__(self):
#         pass
#
# class FrequencyDict():
#     def __init__(self):
#         pass

# class SelectivityMatrixAndArrivalRates():
#     def __init__(self):
#         pass


    # def __arrival_rates(self):
    #     pass
    #
    # def __frequency_dict(self):
    #     pass
    #
    # def __selectivity_matrix_and_arrival_rates(self):
    #     pass