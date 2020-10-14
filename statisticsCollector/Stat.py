from abc import ABC
from copy import deepcopy
from base.Pattern import Pattern
from statisticsCollector.StatisticsTypes import StatisticsTypes
from statisticsCollector.DataManagerAlgorithm import DataManagerAlgorithm


class Stat(ABC):
    """
    A generic class for creating stat according to statistics type.
    Stat holds the statistics and is sent to the Optimizer
    """
    def __init__(self, statistics_type: StatisticsTypes):
        self.statistics_type = statistics_type

    def get_stat(self):
        """
        Returns a copy Stat
        """
        pass

    def get_generic_data(self):
        """
        Returns numeric statistics in an array
        """
        pass

    def update_data(self, data_manager: DataManagerAlgorithm):
        """
        Updates the statistics at Stat according data manager
        """
        pass


class ArrivalRateStat(Stat):
    def __init__(self, arrival_rates: dict, pattern: Pattern, statistics_type: StatisticsTypes):
        super().__init__(statistics_type)
        self.pattern = pattern
        self.arrival_rates = arrival_rates

    def get_stat(self):
        copy_stat = deepcopy(self)
        arrival_rates_list = []
        for arg in self.pattern.structure.args:
            arrival_rates_list.append(copy_stat.arrival_rates[arg.event_type])
        copy_stat.arrival_rates = arrival_rates_list
        return copy_stat

    def get_generic_data(self):
        generic_data = []
        for rate in self.arrival_rates:
            generic_data.insert(0, rate)
        return generic_data

    def update_data(self, data_manager: DataManagerAlgorithm):
        self.arrival_rates = data_manager.get_data()


class SelectivityMatrixAndArrivalRateStat(Stat):
    def __init__(self, selectivity_matrix, arrival_rates, pattern: Pattern, statistics_type: StatisticsTypes):
        super().__init__(statistics_type)
        self.selectivity_matrix = selectivity_matrix
        self.arrival_rates = arrival_rates
        raise NotImplementedError

    def get_stat(self):
        raise NotImplementedError

    def get_generic_data(self):
        raise NotImplementedError

    def update_data(self, data_manager: DataManagerAlgorithm):
        raise NotImplementedError