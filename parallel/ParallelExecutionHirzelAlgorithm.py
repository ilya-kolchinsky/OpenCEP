"""
 Data parallel Hirzel algorithms
"""
from abc import ABC
from parallel.ParallelExecutionAlgorithms import DataParallelAlgorithm

from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *


class HirzelAlgorithm(DataParallelAlgorithm, ABC):
    """
    A class for data parallel evaluation Hirzel algorithm
    """
    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, key: str):
        super().__init__(units_number, patterns, eval_mechanism_params,platform)
        self.__key = key

    def eval_algorithm(self, events: InputStream, matches: OutputStream,
                       data_formatter: DataFormatter):
        event = Event(events.first(), data_formatter)
        key_val = event.payload[self.__key]
        if not isinstance(key_val, (int, float)):
            raise Exception("key %s has no numeric value" % (self.__key,))
        super().eval_algorithm(events, matches, data_formatter)
        self._stream_divide()
        for t in self._units:
            t.wait()
        self._matches.close()

    def _stream_divide(self):
        """
        Divide the input stream into calculation units according to the values of the key
        """
        for event_raw in self._events:
            event = Event(event_raw, self._data_formatter)
            index = int(event.payload[self.__key] % (self._units_number - 1))
            self._events_list[index].add_item(event_raw)

        for stream in self._events_list:
            stream.close()

    def _eval_unit(self, thread_id: int, data_formatter: DataFormatter):
        self._eval_trees[thread_id].eval(self._events_list[thread_id],
                                         self._matches, data_formatter, False)
