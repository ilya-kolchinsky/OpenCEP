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
        pass

    @abstractmethod
    def get_statistics(self):
        """
        Return the current statistics.
        """
        pass


class ArrivalRatesStatistics(Statistics):
    """
    Represents the the arrival rates statistics.
    """
    def __init__(self, time_window: timedelta, pattern: Pattern):
        self.arrival_rates = [0.0] * len(pattern.positive_structure.args)

        self.event_type_to_indexes_map = {}
        for i, event in enumerate(pattern.positive_structure.args):
            if event.type in self.event_type_to_indexes_map:
                self.event_type_to_indexes_map[event.type].append(i)
            else:
                self.event_type_to_indexes_map[event.type] = [i]

        self.events_arrival_time = []
        self.time_window = time_window
        self.count = 0

    def update(self, event: Event):
        event_type = event.type
        time = datetime.now()

        if event_type in self.event_type_to_indexes_map:
            indexes = self.event_type_to_indexes_map[event_type]
            for index in indexes:
                self.arrival_rates[index] += 1
                # self.events_arrival_time.append(EventTime(event.timestamp, event_type))
                self.events_arrival_time.append(StatisticEventData(time, event_type))

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
        return ArrivalRatesWrapper(self.arrival_rates)


class SelectivityStatistics(Statistics):
    """
    Represents the the arrival rates statistics.
    NOTE: Currently, this statistic ignores time window
    """
    # TODO: Implement selectivity that also takes into account a time window

    def __init__(self, pattern: Pattern):
        self.pattern = pattern
        self.args = pattern.positive_structure.args
        self.args_num = len(self.args)

        self.selectivity_matrix = [[1.0 for _ in range(self.args_num)] for _ in range(self.args_num)]
        self.success_counter = {}
        self.total_counter = {}

        self.event_type_to_arg_indexes_map = {}
        for i, e in enumerate(pattern.positive_structure.args):
            if e.type in self.event_type_to_arg_indexes_map:
                self.event_type_to_arg_indexes_map[e.type].append(i)
            else:
                self.event_type_to_arg_indexes_map[e.type] = [i]

        self.args_index_to_events_common_conditions_indexes_map = {}

        self.i_j_to_condition_map = {}
        self.condition_to_total_map = {}
        self.condition_to_success_count_map = {}
        self.events = {event.type: [] for event in pattern.positive_structure.args}
        self.is_sequence = True

        self.init()

    def update(self, event1: Event):
        pass
        # event1_type = event1.type
        # self.events[event1_type].append(event1)
        # arg_indexes = self.event_type_to_arg_indexes_map[event1_type]
        # for arg_index in arg_indexes:
        #     for events_common_conditions_index in self.args_index_to_events_common_conditions_indexes_map[arg_index]:
        #         condition = self.i_j_to_condition_map[(arg_index, events_common_conditions_index)]
        #         e_type = self.args[events_common_conditions_index].type
        #         self.condition_to_total_map[condition] += 1
        #         for event2 in self.events[e_type]:
        #             if arg_index == events_common_conditions_index:
        #                 if condition.eval({self.args[arg_index].name: event2}):
        #                     self.condition_to_success_count_map[condition] += 1
        #
        #             else:
        #                 if condition.eval({self.args[arg_index].name: event1.payload, self.args[events_common_conditions_index].name: event2.payload}):
        #                     self.condition_to_success_count_map[condition] += 1
        #
        #         sel = self.condition_to_success_count_map[condition] / self.condition_to_total_map[condition]
        #         self.selectivity_matrix[arg_index][events_common_conditions_index] = sel

    def get_statistics(self):
        return SelectivityWrapper(self.selectivity_matrix)

    def init(self):
        for i in range(self.args_num):
            for j in range(i + 1):
                condition = self.pattern.condition.get_condition_of({self.args[i].name, self.args[j].name})
                if condition is not None:
                    if i in self.args_index_to_events_common_conditions_indexes_map:
                        self.args_index_to_events_common_conditions_indexes_map[i].append(j)
                    else:
                        self.args_index_to_events_common_conditions_indexes_map[i] = [j]

                    if j in self.args_index_to_events_common_conditions_indexes_map:
                        self.args_index_to_events_common_conditions_indexes_map[j].append(i)
                    else:
                        self.args_index_to_events_common_conditions_indexes_map[j] = [i]

                    self.i_j_to_condition_map[(i, j)] = self.i_j_to_condition_map[(j, i)] = condition
                    # self.condition_to_total_map[condition] = 0
                    # self.condition_to_success_count_map[condition] = 0


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
        return SelectivityAndArrivalRatesWrapper(arrival_rates, selectivity_matrix)


class FrequencyDict(Statistics):
    """
    Not implemented
    """
    def __init__(self):
        self.frequency_dict = {}

    def update(self, event):
        pass

    def get_statistics(self):
        return self.frequency_dict

