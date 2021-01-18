from abc import ABC

from misc.StatisticsTypes import StatisticsTypes


class StatisticsObject(ABC):
    pass


class ArrivalRates(StatisticsObject):

    def __init__(self, arrival_rates):
        self.statistics = arrival_rates


class SelectivityMatrix(StatisticsObject):

    def __init__(self, selectivity_matrix):
        self.statistics = selectivity_matrix


class SelectivityMatrixAndArrivalRates(StatisticsObject):

    def __init__(self, arrival_rates, selectivity_matrix):
        self.statistics = selectivity_matrix, arrival_rates


class Frequency(StatisticsObject):

    def __init__(self, frequency):
        self.statistics = frequency
