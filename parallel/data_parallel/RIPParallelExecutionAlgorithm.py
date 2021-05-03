from abc import ABC

from bson import timestamp

from parallel.data_parallel.DataParallelExecutionAlgorithm import (
    DataParallelExecutionAlgorithm,
    DataParallelExecutionUnit)
import math
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters, EvaluationMechanismFactory
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *
from datetime import datetime, timedelta

class RIPParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the RIP algorithm.
    """
    #----------------- ORIGINAL ---------------------
    # def __init__(self, units_number, patterns: Pattern or List[Pattern],
    #              eval_mechanism_params: EvaluationMechanismParameters,
    #              platform, multiple):
    #     super().__init__(units_number - 1, patterns, eval_mechanism_params, platform)
    #     self.__eval_mechanism_params = eval_mechanism_params


    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform, interval):
        super().__init__(units_number - 1, patterns, eval_mechanism_params, platform)
        self.interval = interval
        if isinstance(patterns, list):
            self.time_delta = patterns[0].window  # assuming there is only one time delta for all patterns maybe take maximal
        else:
            self.time_delta = patterns.window

        self.start_time = None




    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
            Activates the actual parallel algorithm.
        """
        self._check_legal_input(events, data_formatter)

        execution_units = list()

        for unit_id, evaluation_manager in enumerate(self.evaluation_managers):
            execution_unit = DataParallelExecutionUnit(self.platform,
                                                       unit_id,
                                                       evaluation_manager,
                                                       self.filters[unit_id],
                                                       data_formatter)
            execution_unit.start()
            execution_units.append(execution_unit)

        for raw_event in events:
            if not self.start_time: #todo ask if the first event in the stream is the earliest. Is the stream synchrony
                first_event = Event(raw_event, data_formatter)
                self.start_time = first_event.timestamp
            for unit_id in self._classifier(raw_event, data_formatter):
                execution_units[unit_id].add_event(raw_event)


        for execution_unit in execution_units:
            execution_unit.wait()


    # todo check about time_deltas the span over a few intervals - 3/4/5...
    def _classifier(self, raw_event: str, data_formatter: DataFormatter):
        event = Event(raw_event, data_formatter)
        event_time = event.timestamp
        unit_id1 = self._calcUnitNumber(event_time)
        unit_id2 = self._calcUnitNumber(event_time, self.time_delta)

        if unit_id1 != unit_id2:
            if event_time-self.start_time > self.interval: # updates start_time when needed
                self._updateStartTime(event_time)
            return [unit_id1, unit_id2]
        return unit_id1


    def _calcUnitNumber(self, cur_time, time_delta=timedelta(seconds=0)):
        event_time = cur_time + time_delta
        if self.start_time is None:
            raise Exception("start_time in RIP is not initialized")
        diff_time = event_time - self.start_time
        unit_id = int((diff_time/self.interval)%self.units_number)
        return unit_id+1 # result is zero based

    def _updateStartTime(self, timestamp):
        raise NotImplementedError  # Marcus has implemented that















