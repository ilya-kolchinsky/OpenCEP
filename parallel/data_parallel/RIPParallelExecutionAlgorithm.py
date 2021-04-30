from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm, DataParallelExecutionUnit
import math
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters, EvaluationMechanismFactory
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *
from datetime import datetime, timedelta
from misc.Utils import is_int, is_float


class RIPParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the RIP algorithm.
    """
    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, multiple: timedelta):
        super().__init__(units_number - 1, patterns, eval_mechanism_params, platform)
        self.interval = multiple
        if isinstance(patterns, list):
            self.time_delta = max(pattern.window for pattern in patterns)  # check willingness
        else:
            self.time_delta = patterns.window
        self.filters = []

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Activates the actual parallel algorithm.
        """
        self._check_legal_input(events, data_formatter)
        execution_units = list()
        self.filters = [RIPFilterStream(interval=self.interval,
                                        time_delta=self.time_delta,
                                        matches=matches,
                                        data_formatter=data_formatter) for _ in range(self.units_number)]

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
        raise NotImplementedError


class RIPFilterStream(Stream):
    def __init__(self, interval: timedelta, time_delta: timedelta,
                 matches: OutputStream, data_formatter: DataFormatter):
        super().__init__()
        self.interval = interval
        self.time_delta = time_delta
        self.data_formatter = data_formatter
        self.matches = matches
        self.start_time = None

    def update_start_time(self, start_time: datetime):
        self.start_time = start_time

    def add_item(self, item: object):
        if self.skip_item(item):
            return
        else:
            self.matches.add_item(item)

    def skip_item(self, item):
        parsed_item = self.data_formatter.parse_event(item)
        item_time = parsed_item['Date']
        window_start = self.start_time + self.time_delta
        window_end = window_start + self.interval
        return not (window_start < item_time < window_end)
