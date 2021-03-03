"""
 Data parallel algorithms
"""
from abc import ABC
from stream.DataParallelStream import *
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters, EvaluationMechanismFactory
from base.DataFormatter import DataFormatter
from base.PatternMatch import *


class DataParallelAlgorithm(ABC):
    """
        An abstract base class for all  data parallel evaluation algorithms.
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform):
        self._platform = platform
        self._data_formatter = None
        self._units_number = units_number
        self._units = []
        self._eval_trees = []
        self._events = None
        self._events_list = []
        self._matches = None
        self._patterns = [patterns] if isinstance(patterns, Pattern) else \
            patterns

        for _ in range(0, self._units_number):
            self._eval_trees.append(EvaluationMechanismFactory.build_eval_mechanism(eval_mechanism_params, patterns))
            self._events_list.append(Stream())

    def eval_algorithm(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
            Activates the algorithm evaluation mechanism
        """
        self._events = events
        self._data_formatter = data_formatter
        self._matches = matches
        for i in range(self._units_number):
            unit = self._platform.create_parallel_execution_unit(
                unit_id=i,
                callback_function=self._eval_unit,
                thread_id=i,
                data_formatter=data_formatter)
            self._units.append(unit)
            unit.start()

    def _eval_unit(self, thread_id: int, data_formatter: DataFormatter):
        """
            Activates the unit evaluation mechanism
        """
        raise NotImplementedError()

    def _stream_divide(self):
        """
            Divide the input stream into the appropriate units
        """
        raise NotImplementedError()

    def get_structure_summary(self):
        return self._eval_trees[0].get_structure_summary()
