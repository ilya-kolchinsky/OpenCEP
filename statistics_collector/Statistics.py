import copy
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from base.Event import Event
from base.Pattern import Pattern
from misc.Statistics import calculate_selectivity_matrix
from statistics_collector.StatisticsWrapper import ArrivalRatesWrapper, SelectivityWrapper, SelectivityAndArrivalRatesWrapper
from statistics_collector.StatisticEventData import StatisticEventData


class Statistics(ABC):
    """
    An abstract class for statistics.
    """
    @abstractmethod
    def update(self, event: Event):
        """
        Given the newly arrived event, update the statistics.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_statistics(self):
        """
        Return the current statistics.
        """
        raise NotImplementedError()


class ArrivalRatesStatistics(Statistics):
    """
    Represents the arrival rates statistics.
    """
    def __init__(self, time_window: timedelta, pattern: Pattern):
        args = pattern.get_primitive_events()
        self.arrival_rates = [0.0] * len(args)

        self.__event_type_to_indexes_map = {}
        for i, arg in enumerate(args):
            if arg.get_type() in self.__event_type_to_indexes_map:
                self.__event_type_to_indexes_map[arg.type].append(i)
            else:
                self.__event_type_to_indexes_map[arg.type] = [i]

        self.__events_arrival_time = []
        self.__time_window = time_window

    def update(self, event: Event):
        event_type = event.type
        event_timestamp = event.timestamp

        if event_type in self.__event_type_to_indexes_map:
            indexes = self.__event_type_to_indexes_map[event_type]
            for index in indexes:
                self.arrival_rates[index] += 1
                self.__events_arrival_time.append(StatisticEventData(event_timestamp, event_type))

        self.__remove_expired_events(event_timestamp)

    def __remove_expired_events(self, last_timestamp: datetime):
        # TODO: refactor this
        """
        This method is efficient if we call this function every time we update statistics and
        our assumption that is more efficient then binary search because we know that there is
        a little mount of expired event in the beginning.
        In addition, if we use this function not every time we update statistics but rather when
        the evaluation want get statistics the efficient method to implement this function is
        probably by binary search.
        """
        is_removed_elements = False
        for i, event_time in enumerate(self.__events_arrival_time):
            if last_timestamp - event_time.timestamp > self.__time_window:
                indexes = self.__event_type_to_indexes_map[event_time.event_type]
                for index in indexes:
                    self.arrival_rates[index] -= 1

            else:
                is_removed_elements = True
                self.__events_arrival_time = self.__events_arrival_time[i:]
                break

        if not is_removed_elements:
            self.__events_arrival_time = []

    def get_statistics(self):
        return copy.deepcopy(ArrivalRatesWrapper(self.arrival_rates))


class SelectivityStatistics(Statistics):
    """
    Represents the arrival rates statistics.
    NOTE: Currently, this statistic ignores time window
    """
    # TODO: Implement selectivity that also takes into account a time window

    def __init__(self, pattern: Pattern):
        self.__pattern = pattern
        self.__args = pattern.get_primitive_events()
        self.__args_len = len(self.__args)
        self.__args_index_to_events_common_conditions_indexes_map = {}
        self.__indexes_to_condition_map = {}
        self.__total_map = {}
        self.__success_map = {}
        self.__event_type_to_arg_indexes_map = {}
        self.__event_type_to_events_map = {primitive_event.type: [] for primitive_event in self.__args}
        self.selectivity_matrix = [[1.0 for _ in range(self.__args_len)] for _ in range(self.__args_len)]

        self.init_maps()

    def update(self, event1: Event):
        event_type = event1.type
        event_arg_1_indexes = self.__event_type_to_arg_indexes_map[event_type]
        for arg_1_index in event_arg_1_indexes:
            for arg_2_index in self.__args_index_to_events_common_conditions_indexes_map[arg_1_index]:
                condition = self.__indexes_to_condition_map[(arg_1_index, arg_2_index)]
                arg_2_type = self.__args[arg_2_index].type
                if arg_1_index == arg_2_index:
                    self.__total_map[(arg_1_index, arg_2_index)] += 1
                    if condition.eval({self.__args[arg_1_index].name: event1.payload}):
                        self.__success_map[(arg_1_index, arg_2_index)] += 1
                else:
                    for event2 in self.__event_type_to_events_map[arg_2_type]:
                        self.__total_map[(arg_1_index, arg_2_index)] += 1
                        self.__total_map[(arg_2_index, arg_1_index)] += 1
                        if condition.eval({self.__args[arg_1_index].name: event1.payload,
                                           self.__args[arg_2_index].name: event2.payload}):
                            self.__success_map[(arg_1_index, arg_2_index)] += 1
                            self.__success_map[(arg_2_index, arg_1_index)] += 1
                if self.__total_map[(arg_1_index, arg_2_index)] == 0:
                    continue
                sel = self.__success_map[(arg_1_index, arg_2_index)] / self.__total_map[(arg_1_index, arg_2_index)]
                self.selectivity_matrix[arg_1_index][arg_2_index] = sel
                self.selectivity_matrix[arg_2_index][arg_1_index] = sel
        self.__event_type_to_events_map[event_type].append(event1)

    def get_statistics(self):
        return copy.deepcopy(SelectivityWrapper(self.selectivity_matrix))

    def init_maps(self):
        self.init_event_type_to_arg_indexes_map()
        self.init_condition_maps()

    def init_event_type_to_arg_indexes_map(self):
        for i, primitive_event in enumerate(self.__args):
            if primitive_event.type in self.__event_type_to_arg_indexes_map:
                self.__event_type_to_arg_indexes_map[primitive_event.type].append(i)
            else:
                self.__event_type_to_arg_indexes_map[primitive_event.type] = [i]

    def init_condition_maps(self):
        for i in range(self.__args_len):
            for j in range(i + 1):
                condition = self.__pattern.condition.get_condition_of({self.__args[i].name, self.__args[j].name})
                if condition is not None:
                    if i in self.__args_index_to_events_common_conditions_indexes_map:
                        self.__args_index_to_events_common_conditions_indexes_map[i].append(j)
                    else:
                        self.__args_index_to_events_common_conditions_indexes_map[i] = [j]
                    if i != j:
                        if j in self.__args_index_to_events_common_conditions_indexes_map:
                            self.__args_index_to_events_common_conditions_indexes_map[j].append(i)
                        else:
                            self.__args_index_to_events_common_conditions_indexes_map[j] = [i]

                    self.__indexes_to_condition_map[(i, j)] = self.__indexes_to_condition_map[(j, i)] = condition
                    self.__total_map[(i, j)] = 0
                    self.__total_map[(j, i)] = 0
                    self.__success_map[(i, j)] = 0
                    self.__success_map[(j, i)] = 0


class SelectivityAndArrivalRatesStatistics(Statistics):
    """
    Represents both the arrival rates and selectivity statistics.
    """
    def __init__(self, arrival_rates: ArrivalRatesStatistics, selectivity_matrix: SelectivityStatistics):
        self.arrival_rates = arrival_rates
        self.selectivity_matrix = selectivity_matrix

    def update(self, event: Event):
        self.arrival_rates.update(event)
        self.selectivity_matrix.update(event)

    def get_statistics(self):
        arrival_rates = self.arrival_rates.arrival_rates
        selectivity_matrix = self.selectivity_matrix.selectivity_matrix
        return copy.deepcopy(SelectivityAndArrivalRatesWrapper(arrival_rates, selectivity_matrix))


class FrequencyDict(Statistics):
    """
    Not implemented
    """
    def __init__(self):
        self.frequency_dict = {}

    def update(self, event):
        raise NotImplementedError()

    def get_statistics(self):
        return self.frequency_dict

