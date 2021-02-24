from abc import ABC


class StatisticsWrapper(ABC):
    """
    An abstract object that contains the pure statistics, intended to be sent to other classes
    """
    pass


class ArrivalRatesWrapper(StatisticsWrapper):
    """
    Represents an arrival rates statistics wrapper.
    """
    def __init__(self, arrival_rates):
        self.statistics = arrival_rates


class SelectivityWrapper(StatisticsWrapper):
    """
    Represents a selectivity matrix statistics wrapper.
    """
    def __init__(self, selectivity_matrix):
        self.statistics = selectivity_matrix
