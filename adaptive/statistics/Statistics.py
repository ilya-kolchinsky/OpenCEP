import copy
from abc import ABC
from datetime import timedelta, datetime
from typing import List
from base.Event import Event
from base.Pattern import Pattern


class StatisticEventData:
    """
    A container class that contains the event type along with event timestamp
    """
    def __init__(self, timestamp: datetime, event_type: str):
        self.timestamp = timestamp
        self.event_type = event_type


class Statistics(ABC):
    """
    An abstract class for the statistics.
    """

    def update(self, data):
        """
        Given the newly arrived event, update the statistics.
        """
        raise NotImplementedError()

    def get_statistics(self):
        """
        Return the current statistics.
        """
        raise NotImplementedError()


class ArrivalRatesStatistics(Statistics):
    """
    Represents the arrival rates statistics.
    """
    def __init__(self, arrival_rates_time_window: timedelta, pattern: Pattern, predefined_statistics: List = None):
        primitive_events = pattern.get_primitive_events()
        self.__arrival_rates = [0.0] * len(primitive_events) if not predefined_statistics else predefined_statistics
        self.__event_type_to_indices_map = {}
        for i, arg in enumerate(primitive_events):
            if arg.type in self.__event_type_to_indices_map:
                self.__event_type_to_indices_map[arg.type].append(i)
            else:
                self.__event_type_to_indices_map[arg.type] = [i]
        self.__events_arrival_time = []
        self.__arrival_rates_time_window = arrival_rates_time_window
        self.__last_timestamp = None

    def update(self, event: Event):
        """
        Increases the arrival rate of the current event type by 1 and decreases the arrival rates of the expired events.
        """
        event_type = event.type
        event_timestamp = event.timestamp

        if event_type in self.__event_type_to_indices_map:
            self.__events_arrival_time.append(StatisticEventData(event_timestamp, event_type))

            indices = self.__event_type_to_indices_map[event_type]
            for index in indices:
                self.__arrival_rates[index] += 1

        self.__remove_expired_events(event_timestamp)

    def __remove_expired_events(self, last_timestamp: datetime):
        """
        Lowers the arrival rates of the events that left the time window.
        """
        is_removed_elements = False
        for i, event_time in enumerate(self.__events_arrival_time):
            if last_timestamp - event_time.timestamp > self.__arrival_rates_time_window:
                indices = self.__event_type_to_indices_map[event_time.event_type]
                for index in indices:
                    self.__arrival_rates[index] -= 1

            else:
                is_removed_elements = True
                self.__events_arrival_time = self.__events_arrival_time[i:]
                break

        if not is_removed_elements:
            self.__events_arrival_time = []

    def get_statistics(self):
        return copy.deepcopy(self.__arrival_rates)


class SelectivityStatistics(Statistics):
    """
    Represents the selectivity statistics.
    NOTE: Currently it ignores the time window
    """
    # TODO: support time window for selectivity

    def __init__(self, pattern: Pattern, predefined_statistics: List[List[float]] = None):
        self.__args = pattern.get_primitive_events()
        self.__args_len = len(self.__args)
        self.__atomic_condition_to_total_map = {}
        self.__atomic_condition_to_success_map = {}
        self.__indices_to_atomic_condition_map = {}
        self.__relevant_indices = set()

        if not predefined_statistics:
            self.__selectivity_matrix = [[1.0 for _ in range(self.__args_len)] for _ in range(self.__args_len)]
        else:
            self.__selectivity_matrix = predefined_statistics

        self.__init_maps(pattern)

    def update(self, data):
        """
        Updates the selectivity of an atomic condition.
        """
        (atomic_condition, is_condition_success) = data

        if atomic_condition:
            atomic_condition_id = str(atomic_condition)
            if atomic_condition_id in self.__atomic_condition_to_total_map:
                self.__atomic_condition_to_total_map[atomic_condition_id] += 1
                if is_condition_success:
                    self.__atomic_condition_to_success_map[atomic_condition_id] += 1

    def get_statistics(self):
        """
        Calculates the value of cell (i, j) in the selectivity matrix by multiplying the atomic selectivities of
        all the conditions between event i and event j.
        If there is no condition between any pair of events then the selectivity is always 1.0
        """
        for i, j in self.__relevant_indices:
            atomic_conditions_id = self.__indices_to_atomic_condition_map[(i, j)]

            # computation of the (i, j), (j, i) entries in the selectivity matrix
            selectivity = 1.0
            for atomic_condition_id in atomic_conditions_id:
                numerator = self.__atomic_condition_to_success_map[atomic_condition_id]
                denominator = self.__atomic_condition_to_total_map[atomic_condition_id]
                if denominator != 0.0:
                    selectivity *= (numerator / denominator)

            self.__selectivity_matrix[j][i] = self.__selectivity_matrix[i][j] = copy.deepcopy(selectivity)

        return copy.deepcopy(self.__selectivity_matrix)

    def __init_maps(self, pattern: Pattern):
        """
        Initiates the success counters and total evaluation counters for each pair of event types.
        """
        for i in range(self.__args_len):
            for j in range(i + 1):
                conditions = pattern.condition.get_condition_of({self.__args[i].name, self.__args[j].name})
                atomic_conditions = conditions.extract_atomic_conditions()
                for atomic_condition in atomic_conditions:
                    if atomic_condition:
                        atomic_condition_id = str(atomic_condition)
                        self.__relevant_indices.add((i, j))
                        self.__atomic_condition_to_total_map[atomic_condition_id] = 0.0
                        self.__atomic_condition_to_success_map[atomic_condition_id] = 0.0
                        if (i, j) in self.__indices_to_atomic_condition_map:
                            self.__indices_to_atomic_condition_map[(i, j)].append(atomic_condition_id)
                        else:
                            self.__indices_to_atomic_condition_map[(i, j)] = [atomic_condition_id]
