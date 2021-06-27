from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.PatternMatch import *
from datetime import timedelta
from typing import Set
from base.DataFormatter import DataFormatter
from stream.Stream import *


class RIPParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the RIP algorithm.
    """

    def __init__(self, units_number,
                 patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform,
                 interval: timedelta,
                 debug: bool = False):
        super().__init__(units_number, patterns, eval_mechanism_params, platform, debug)

        self.interval = interval

        # in case of multi pattern
        if isinstance(patterns, list):
            self.__time_delta = max(pattern.window for pattern in patterns)  # TODO: check willingness
        else:
            self.__time_delta = patterns.window

        if self.__time_delta > self.interval:
            raise Exception("time delta > interval")

        self.__start_time = None

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        first_event = Event(events.first(), data_formatter)
        self.__start_time = first_event.timestamp
        super(RIPParallelExecutionAlgorithm, self).eval(events, matches, data_formatter)

    def _check_first_event(self, first_event: Event):
        """
        Init events start time
        """
        self.__start_time = first_event.timestamp

    def _create_skip_item(self, unit_id: int):
        """
        Creates and returns FilterStream object.
        """

        def skip_item(item: PatternMatch):
            """
            last_matching_unit > unit_id: the unit will skip matches that ends after the unit time
            first_matching_unit < unit_id: the unit will skip matches that starts before the unit time
            """
            first_matching_unit = self._get_unit_number(item.first_timestamp)
            last_matching_unit = self._get_unit_number(item.last_timestamp)
            return last_matching_unit > unit_id or first_matching_unit < unit_id
        return skip_item

    def _get_unit_number(self, event_time) -> int:
        """
        returns the corresponding unit to the event time
        """
        diff_time = event_time - self.__start_time
        unit_id = int((diff_time / self.interval) % self.units_number)
        return unit_id  # result is zero based

    def _classifier(self, event: Event) -> Set[int]:
        """
        Returns possible unit ids for the given event
        """
        event_time = event.timestamp
        unit_id1 = self._get_unit_number(event_time)
        unit_id2 = self._get_unit_number(event_time + self.__time_delta)
        return {unit_id1, unit_id2}
