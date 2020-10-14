from statisticsCollector.DataManagerAlgorithm import create_data_manager_algorithm_by_statistics_type,\
    DataManagerAlgorithmTypes
from statisticsCollector.StatisticsTypes import StatisticsTypes
from base.Pattern import Pattern
from base.Event import Event
from statisticsCollector.Stat import ArrivalRateStat


class StatisticsCollector:
    def __init__(self, pattern: Pattern, statistics_type: StatisticsTypes, data_manager_type: DataManagerAlgorithmTypes):
        if statistics_type == StatisticsTypes.NO_STATISTICS:
            return
        self.data_manager_algorithm = create_data_manager_algorithm_by_statistics_type(data_manager_type,
                                                                                       pattern, statistics_type)
        self.stat = self.create_stat_by_statistics_type(statistics_type, pattern)

    def create_stat_by_statistics_type(self, statistics_type: StatisticsTypes, pattern: Pattern):
        """
        Creates Stat according to statistics type
        """
        if statistics_type == StatisticsTypes.ARRIVAL_RATES:
            return ArrivalRateStat(arrival_rates=self.data_manager_algorithm.get_data(), pattern=pattern,
                                   statistics_type=statistics_type)
        elif statistics_type == StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            return NotImplementedError
        else:
            raise NotImplementedError

    def get_stat(self):
        """
        Returns Stat which contains the relevant statistics according to the statistics type
        """
        return self.stat.get_stat()

    def insert_event(self, event: Event):
        """
        Inserts an event to the data manager algorithm and updating Stat accordingly
        """
        self.data_manager_algorithm.insert_event(event)
        self.stat.update_data(self.data_manager_algorithm)

    def remove_event(self, event: Event):
        """
        Removes an event from the data manager algorithm and updating Stat accordingly
        """
        self.data_manager_algorithm.remove_event(event)
        self.stat.update_data(self.data_manager_algorithm)

