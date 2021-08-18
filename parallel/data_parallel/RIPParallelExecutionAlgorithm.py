from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.PatternMatch import *
from typing import Set
from base.DataFormatter import DataFormatter
from stream.Stream import *


class RIPParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the RIP algorithm.
    multiple - Ratio between 'time window' and the 'interval'
    units_number - Indicate the number of units/threads to run, doesn't include the "main execution unit".
    """
    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, multiple: float):
        super().__init__(units_number, patterns, eval_mechanism_params, platform)

        # in case of multi pattern
        if isinstance(patterns, list):
            self._time_delta = max(pattern.window for pattern in patterns)
        else:
            self._time_delta = patterns.window

        self._interval = self._time_delta * multiple

        if self._time_delta > self._interval:  # multiple must be > 1
            raise Exception("time delta > interval")

        self._start_time = None

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Evaluates the input stream events based on the given dataFormatter
        Sets the algorithm's start time as the time of the first event, this start time will be referenced by
        the calling methods as a base point.
        """
        first_event = Event(events.first(), data_formatter)
        self._start_time = first_event.timestamp
        super(RIPParallelExecutionAlgorithm, self).eval(events, matches, data_formatter)

    def _create_skip_item(self, unit_id: int):
        """
        Only allows a match to pass if it was returned by the first of the two overlapping execution units.
        """
        def skip_item(item: PatternMatch):
            first_matching_unit = self.__get_unit_number(item.first_timestamp)
            return first_matching_unit != unit_id
        return skip_item

    def _classifier(self, event: Event) -> Set[int]:
        """
        Returns possible unit ids for the given event
        """
        event_time = event.timestamp
        unit_id1 = self.__get_unit_number(event_time - self._time_delta)
        unit_id2 = self.__get_unit_number(event_time)
        return {unit_id1, unit_id2}

    def __get_unit_number(self, event_time) -> int:
        """
        Returns the ID of the execution unit in charge of the time interval to which the given timestamp belongs.
        In case of an overlapping window between two threads, the first ID is returned.
        """
        diff_time = event_time - self._start_time
        unit_id = int((diff_time / self._interval) % self.units_number)
        return unit_id  # result is zero based
