import copy
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from base.Event import Event
from base.Pattern import Pattern
from misc.Statistics import calculate_selectivity_matrix
from statistics_collector.StatisticsWrapper import ArrivalRatesWrapper, SelectivityWrapper, \
    SelectivityAndArrivalRatesWrapper
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
    Represents the the arrival rates statistics.
    """
    def __init__(self, time_window: timedelta, pattern: Pattern):
        args = pattern.get_primitive_events()
        self.arrival_rates = [0.0] * len(args)

        self.event_type_to_indexes_map = {}
        for i, arg in enumerate(args):
            if arg.get_type() in self.event_type_to_indexes_map:
                self.event_type_to_indexes_map[arg.get_type()].append(i)
            else:
                self.event_type_to_indexes_map[arg.get_type()] = [i]

        self.events_arrival_time = []
        self.time_window = time_window

    def update(self, event: Event):
        event_type = event.type
        time = datetime.now()

        if event_type in self.event_type_to_indexes_map:
            indexes = self.event_type_to_indexes_map[event_type]
            for index in indexes:
                self.arrival_rates[index] += 1
                self.events_arrival_time.append(StatisticEventData(time, event_type))

        self.__remove_expired_events(time)

    def __remove_expired_events(self, last_timestamp: datetime):
        is_removed_elements = False
        for i, event_time in enumerate(self.events_arrival_time):
            if last_timestamp - event_time.timestamp > self.time_window:
                indexes = self.event_type_to_indexes_map[event_time.event_type]
                for index in indexes:
                    self.arrival_rates[index] -= 1

            else:
                is_removed_elements = True
                self.events_arrival_time = self.events_arrival_time[i:]
                break

        if not is_removed_elements:
            self.events_arrival_time = []

    def get_statistics(self):
        return copy.deepcopy(ArrivalRatesWrapper(self.arrival_rates))


class SelectivityStatistics(Statistics):
    """
    Represents the arrival rates statistics.
    NOTE: Currently, this statistic ignores time window
    """
    # TODO: Implement selectivity that also takes into account a time window

    def __init__(self, pattern: Pattern):
        self.pattern = pattern
        self.args = pattern.get_primitive_events()
        self.args_len = len(self.args)
        self.args_index_to_events_common_conditions_indexes_map = {}
        self.i_j_to_condition_map = {}
        self.condition_to_total_map = {}
        self.condition_to_success_count_map = {}
        self.event_type_to_arg_indexes_map = {}
        self.event_type_to_events_map = {primitive_event.type: [] for primitive_event in self.args}
        self.selectivity_matrix = [[1.0 for _ in range(self.args_len)] for _ in range(self.args_len)]

        self.init()

    def update(self, event1: Event):
        event1_type = event1.type
        self.event_type_to_events_map[event1_type].append(event1)
        event1_arg_indexes = self.event_type_to_arg_indexes_map[event1_type]
        used_conditions = {}
        for event1_arg_index in event1_arg_indexes:
            for events_common_conditions_index in self.args_index_to_events_common_conditions_indexes_map[event1_arg_index]:
                condition = self.i_j_to_condition_map[(event1_arg_index, events_common_conditions_index)]
                if str(condition) in used_conditions:
                    i, j = used_conditions[str(condition)]
                    self.selectivity_matrix[event1_arg_index][events_common_conditions_index] = \
                        self.selectivity_matrix[i][j]
                    continue
                used_conditions[str(condition)] = [event1_arg_index, events_common_conditions_index]

                e_type = self.args[events_common_conditions_index].type

                if event1_arg_index == events_common_conditions_index:
                    self.condition_to_total_map[str(condition)] += 1
                    if condition.eval({self.args[event1_arg_index].name: event1.payload}):
                        self.condition_to_success_count_map[str(condition)] += 1

                else:
                    for event2 in self.event_type_to_events_map[e_type]:
                        self.condition_to_total_map[str(condition)] += 1
                        if condition.eval({self.args[event1_arg_index].name: event1.payload, self.args[events_common_conditions_index].name: event2.payload}):
                            self.condition_to_success_count_map[str(condition)] += 1
                if self.condition_to_total_map[str(condition)] == 0:
                    continue
                sel = self.condition_to_success_count_map[str(condition)] / self.condition_to_total_map[str(condition)]
                self.selectivity_matrix[event1_arg_index][events_common_conditions_index] = sel

    def get_statistics(self):
        return copy.deepcopy(SelectivityWrapper(self.selectivity_matrix))

    def init(self):
        self.init_event_type_to_arg_indexes_map()
        self.init_condition_maps()

    def init_event_type_to_arg_indexes_map(self):
        for i, primitive_event in enumerate(self.args):
            if primitive_event.type in self.event_type_to_arg_indexes_map:
                self.event_type_to_arg_indexes_map[primitive_event.type].append(i)
            else:
                self.event_type_to_arg_indexes_map[primitive_event.type] = [i]

    def init_condition_maps(self):
        for i in range(self.args_len):
            for j in range(i + 1):
                condition = self.pattern.condition.get_condition_of({self.args[i].name, self.args[j].name})
                if condition is not None:
                    if i in self.args_index_to_events_common_conditions_indexes_map:
                        self.args_index_to_events_common_conditions_indexes_map[i].append(j)
                    else:
                        self.args_index_to_events_common_conditions_indexes_map[i] = [j]
                    if i != j:
                        if j in self.args_index_to_events_common_conditions_indexes_map:
                            self.args_index_to_events_common_conditions_indexes_map[j].append(i)
                        else:
                            self.args_index_to_events_common_conditions_indexes_map[j] = [i]

                    self.i_j_to_condition_map[(i, j)] = self.i_j_to_condition_map[(j, i)] = condition
                    self.condition_to_total_map[str(condition)] = 0
                    self.condition_to_success_count_map[str(condition)] = 0


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

