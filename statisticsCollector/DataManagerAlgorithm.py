from enum import Enum
from abc import ABC
from base.Event import Event
from base.Pattern import Pattern
from statisticsCollector.StatisticsTypes import StatisticsTypes


class DataManagerAlgorithmTypes(Enum):
    """
    Type of event statistics provided
    NO_STATISTICS if nothing is available
    """
    TRIVIAL_ALGORITHM = 0
    EXPONENTIAL_HISTOGRAM = 1


class DataManagerAlgorithm(ABC):
    """
    A generic class for creating an data manager algorithm according to DataManagerAlgorithmTypes
    """
    def insert_event(self, event: Event):
        """
        Inserts an event to the data manager algorithm
        """
        pass

    def remove_event(self, event: Event):
        """
        Removes an event from the data manager algorithm
        """
        pass

    def get_data(self):
        """
        Returns the relevant data according to the statistics type
        """
        pass


class TrivialAlgorithm(DataManagerAlgorithm):
    """
    A trivial algorithm for managing the Statistics Collector data
    """
    def __init__(self, pattern):
        self.pattern = pattern

    def insert_event(self, event: Event):
        pass

    def remove_event(self, event: Event):
        pass

    def get_data(self):
        pass


class ArrivalRateTrivialAlgorithm(TrivialAlgorithm):
    """
    Calculates the arrival rates of the events by basic counting
    """
    def __init__(self, pattern: Pattern):
        super().__init__(pattern)
        args = pattern.structure.args
        self.arrival_rates = {}   # List of counters. One for each even
        for arg in args:
            self.arrival_rates[arg.event_type] = 0

    def insert_event(self, event: Event):
        self.arrival_rates[event.event_type] += 1

    def remove_event(self, event: Event):
        self.arrival_rates[event.event_type] -= 1

    def get_data(self):
        return self.arrival_rates


def create_trivial_algorithm(statistics_type: StatisticsTypes, pattern: Pattern):
    """
    Creates a trivial algorithm according to statistics type
    """
    if statistics_type == StatisticsTypes.ARRIVAL_RATES:
        return ArrivalRateTrivialAlgorithm(pattern)
    else:
        raise NotImplementedError


def create_data_manager_algorithm_by_statistics_type(data_manager_type: DataManagerAlgorithmTypes, pattern: Pattern,
                                                     statistics_type: StatisticsTypes):
    """
    Creates a data manager algorithm according to data manager type and statistics type
    """
    if data_manager_type == DataManagerAlgorithmTypes.TRIVIAL_ALGORITHM:
        return create_trivial_algorithm(statistics_type, pattern)
    else:
        raise NotImplementedError
