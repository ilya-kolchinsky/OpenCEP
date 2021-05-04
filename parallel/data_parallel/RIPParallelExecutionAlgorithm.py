from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm, \
    DataParallelExecutionUnit
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
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
            self.time_delta = max(pattern.window for pattern in patterns)  # check willingness
        else:
            self.time_delta = patterns.window

        if self.interval <= self.time_delta:
            raise Exception("time delta > interval")

        self.filters = []
        self.start_time = None

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Activates the actual parallel algorithm.
        """
        execution_units = list()
        first_event = Event(events.first(), data_formatter)
        self.start_time = first_event.timestamp

        def get_skip_item(unit_id):
            def skip_item(item: PatternMatch):
                return self._get_unit_number(item.last_timestamp) == unit_id
            return skip_item

        self.filters = [FilterStream(skip_item=get_skip_item(unit_id), matches=matches)
                        for unit_id in range(self.units_number)]

        for unit_id, evaluation_manager in enumerate(self.evaluation_managers):
            execution_unit = DataParallelExecutionUnit(self.platform,
                                                       unit_id,
                                                       evaluation_manager,
                                                       self.filters[unit_id],
                                                       data_formatter)
            execution_unit.start()
            execution_units.append(execution_unit)

        for raw_event in events:
            for unit_id in self._classifier(raw_event, data_formatter):
                execution_units[unit_id].add_event(raw_event)

        for execution_unit in execution_units:
            execution_unit.wait()

    def _classifier(self, raw_event: str, data_formatter: DataFormatter):
        event = Event(raw_event, data_formatter)
        event_time = event.timestamp
        unit_id1 = self._get_unit_number(event_time)
        unit_id2 = self._get_unit_number(event_time, self.time_delta)
        return {unit_id1, unit_id2}

    def _get_unit_number(self, cur_time, time_delta=timedelta(seconds=0)):
        event_time = cur_time + time_delta
        diff_time = event_time - self.start_time
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
