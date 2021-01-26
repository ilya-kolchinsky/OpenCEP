from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from base.Event import Event
from base.Pattern import Pattern
from statistics_collector.StatisticsObjects import ArrivalRates, SelectivityMatrix, SelectivityMatrixAndArrivalRates
from statistics_collector.EventTime import EventTime


class Statistics(ABC):

    @abstractmethod
    def update(self, event: Event):
        pass

    @abstractmethod
    def get_statistics(self):
        pass


class ArrivalRatesStatistics(Statistics):

    def __init__(self, time_window: timedelta, pattern: Pattern):
        self.arrival_rates = [0.0] * len(pattern.positive_structure.args)
        self.event_type_to_index_map = {e.type: i
                                        for i, e in enumerate(pattern.positive_structure.args)}
        self.events_arrival_time = []
        self.time_window = time_window
        self.count = 0

    def update(self, event: Event):
        event_type = event.type
        time = datetime.now()
        if event_type in self.event_type_to_index_map:
            self.arrival_rates[self.event_type_to_index_map[event_type]] += 1
            # self.events_arrival_time.append(EventTime(event.timestamp, event_type))
            self.events_arrival_time.append(EventTime(time, event_type))

        self.__remove_expired_events(time)

    def __remove_expired_events(self, last_timestamp: datetime):
        """
        This method is efficient if we call this function every time we update statistics and
        our assumption that is more efficient then binary search because we know that ther is
        a little mount of expired event in the beginning.
        In addition, if we use this function not every time we update statistics but rather when
        the evaluation want get statistics the efficient method to implement this function is
        probably by binary search.
        """
        is_removed_elements = False
        for i, event_time in enumerate(self.events_arrival_time):
            if last_timestamp - event_time.timestamp > self.time_window:
                # print(last_timestamp - event_time.timestamp)
                self.arrival_rates[self.event_type_to_index_map[event_time.event_type]] -= 1
                self.count += 1
                # print(self.count)
            else:
                is_removed_elements = True
                self.events_arrival_time = self.events_arrival_time[i:]
                break

        if not is_removed_elements:
            self.events_arrival_time = []

    def get_statistics(self):
        return ArrivalRates(self.arrival_rates)


class SelectivityStatistics(Statistics):
    """
    Note: currently, this statistics  ignore time window
    """
    # todo Implement selectivity that also takes into account a time window

    def __init__(self, pattern: Pattern):
        self.pattern = pattern
        self.args = pattern.positive_structure.args
        self.args_num = len(self.args)
        """
        Need to change from 1.0 to 0.0
        """
        self.selectivity_matrix = [[1.0 for _ in range(self.args_num)] for _ in range(self.args_num)]
        self.success_counter = {}
        self.total_counter = {}
        self.event_type_to_index_map = {e.type: i
                                        for i, e in enumerate(pattern.positive_structure.args)}
        self.event_type_to_name_and_condition_map = {}
        self.event_type_to_selectivity_indexes_matrix = {}

    def update(self, event: Event):
        pass
        """
        event_type = event.type
        index = self.event_type_to_index_map[event_type]
        structure_name = self.args[index]
        for e_type,d in self.event_type_to_name_and_condition_map[event_type]:
            for name,condition in d:
                self.total_counter[event_type] += 1
                if condition.eval({structure_name: event, name: event2.event}):
                    self.success_counter[event_type] += 1
                 self.selectivity_matrix[index][] = self.success_counter[event_type] / self.total_counter[event_type]
        """

    def get_statistics(self):
        return SelectivityMatrix(self.selectivity_matrix)

    def init_maps(self):
        for event in self.args:
            name_to_condition_map = {}
            for i in range(self.args_num):
                condition = self.pattern.condition.get_condition_of({self.args[self.event_type_to_index_map[event]].name,
                                                                     self.args[i].name})
                if condition is not None:
                    name_to_condition_map[self.args[i].name] = condition
            self.event_type_to_name_and_condition_map[event.type] = name_to_condition_map


class SelectivityAndArrivalRatesStatistics(Statistics):

    def __init__(self, arrival_rates: ArrivalRatesStatistics, selectivity_matrix: SelectivityStatistics):
        self.arrival_rates = arrival_rates
        self.selectivity_matrix = selectivity_matrix

    def update(self, event: Event):
        self.arrival_rates.update(event)
        self.selectivity_matrix.update(event)

    def get_statistics(self):
        arrival_rates = self.arrival_rates.arrival_rates
        selectivity_matrix = self.selectivity_matrix.selectivity_matrix
        return SelectivityMatrixAndArrivalRates(arrival_rates, selectivity_matrix)


class FrequencyDict(Statistics):

    def __init__(self):
        self.frequency_dict = {}

    def update(self, event):
        pass

    def get_statistics(self):
        return self.frequency_dict

