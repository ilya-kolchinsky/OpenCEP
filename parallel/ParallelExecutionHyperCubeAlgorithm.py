"""
 Data parallel HyperCube algorithms
"""
from abc import ABC
from parallel.ParallelExecutionAlgorithms import DataParallelAlgorithm
import math
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *


class HyperCubeAlgorithm(DataParallelAlgorithm, ABC):
    """
           A class for data parallel evaluation HyperCube algorithm
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform, attributes_dict: dict):
        super().__init__(units_number - 1, patterns, eval_mechanism_params, platform)
        self.__attributes_dict = attributes_dict
        self.__types = []
        for pattern in patterns:
            self.__types.extend(list(pattern.get_all_event_types_with_duplicates()))
        self.groups_num = math.ceil(self._units_number ** (1 / len(self.__types)))
        self.__matches_handler = Stream()
        self.__matches_unit = self._platform.create_parallel_execution_unit(
            unit_id=self._units_number,
            callback_function=self.__make_output_matches_stream)

    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
            Activates the algorithm evaluation mechanism
        """
        super().eval_algorithm(events, matches, data_formatter)
        self.__matches_unit.start()
        self._stream_divide()
        count = 0
        for t in self._units:
            count += 1
            t.wait()

        self.__matches_handler.close()
        self.__matches_unit.wait()
        self._matches.close()

    def _stream_divide(self):
        """
            Divides the input stream by attributes types
        """
        for event_raw in self._events:
            event = Event(event_raw, self._data_formatter)
            event_type = event.type
            if event_type not in self.__types:
                continue
            if event_type not in self.__attributes_dict.keys():
                raise Exception("%s has no matching attribute" % event_type)
            for index in range(len(self.__attributes_dict[event_type])):
                event_attribute = self.__attributes_dict[event_type][index]
                attribute_val = event.payload[event_attribute]
                if not isinstance(attribute_val, (int, float)):
                    raise Exception(
                        "Attribute %s has no numeric value" % event_attribute)
                group_index = int(attribute_val % self.groups_num)
                type_index = self._finding_type_index_considering_duplications(
                    index, event_type)
                leg_size = self.groups_num ** type_index
                new_start = group_index * leg_size
                jump = leg_size * (self.groups_num - 1) + 1
                j = new_start
                while j < self._units_number:
                    self._events_list[j].add_item(event_raw)
                    leg_size -= 1
                    if leg_size == 0:
                        j += jump
                        leg_size = self.groups_num ** type_index
                    else:
                        j += 1
        for stream in self._events_list:
            stream.close()

    def _eval_unit(self, id_unit: int, data_formatter: DataFormatter):
        """
            Activates the unit evaluation mechanism
        """
        self._eval_trees[id_unit].eval(self._events_list[id_unit], self.__matches_handler, data_formatter, False)

    def _finding_type_index_considering_duplications(self, index_among_type, event_type):
        """
            finding the exact index for a given type considering multiple appearances of types
        """
        count = 0
        i = 0
        while list(self.__attributes_dict.keys())[i] != event_type:
            key = list(self.__attributes_dict.keys())[i]
            count += len(self.__attributes_dict[key])
            i += 1
        count += index_among_type
        return count

    @staticmethod
    def __check_duplicates_in_match(match):
        """
            checks if the match as a duplicate in the output stream
        """
        events_in_match = [event.__str__() for event in match.events]
        events_set = set()
        for event in events_in_match:
            events_set.add(event)
        if len(events_in_match) == len(events_set):
            return False
        return True

    def __make_output_matches_stream(self):
        """
            remove duplicated matches and send the matches to the output stream
        """
        duplicates = list()
        count = 0
        for match in self.__matches_handler:
            count += 1
            if not self.__check_duplicates_in_match(match) and match.__str__() not in duplicates:
                self._matches.add_item(match)
                duplicates.append(match.__str__())
