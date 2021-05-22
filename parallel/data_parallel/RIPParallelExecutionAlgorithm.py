from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.PatternMatch import *
from stream.Stream import *
from datetime import timedelta
from typing import Callable


class RIPParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the RIP algorithm.
    """
    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, interval: timedelta):
        super().__init__(units_number, patterns, eval_mechanism_params, platform)

        self.interval = interval
        if isinstance(patterns, list):
            self.__time_delta = max(pattern.window for pattern in patterns)  # check willingness
        else:
            self.__time_delta = patterns.window

        if self.__time_delta > self.interval:
            raise Exception("time delta > interval")

        self.__start_time = None

    def _check_first_event(self, first_event: Event):
        self.__start_time = first_event.timestamp

    def _get_matches(self, matches: OutputStream, unit_id: int):
        def skip_item(item: PatternMatch):
            return self._get_unit_number(item.last_timestamp) == unit_id

        return FilterStream(skip_item=skip_item, matches=matches)

    def _classifier(self, event: Event):
        event_time = event.timestamp
        unit_id1 = self._get_unit_number(event_time)
        unit_id2 = self._get_unit_number(event_time, self.__time_delta)
        return {unit_id1, unit_id2}

    def _get_unit_number(self, cur_time, time_delta=timedelta(seconds=0)):
        event_time = cur_time + time_delta
        diff_time = event_time - self.__start_time
        unit_id = int((diff_time/self.interval) % self.units_number)
        return unit_id  # result is zero based


class FilterStream(Stream):
    def __init__(self, skip_item: Callable[[PatternMatch], bool], matches: OutputStream):
        super().__init__()
        self.matches = matches
        self.unique_match = skip_item

    def add_item(self, item: PatternMatch):
        if self.unique_match(item):
            self.matches.add_item(item)
