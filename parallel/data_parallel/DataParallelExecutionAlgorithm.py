from abc import ABC
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import \
    EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform
from stream.Stream import *
from parallel.manager.EvaluationManager import EvaluationManager
from parallel.manager.SequentialEvaluationManager import SequentialEvaluationManager


class DataParallelExecutionAlgorithm(ABC):
    """
    An abstract base class for all data parallel evaluation algorithms.
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform: ParallelExecutionPlatform):
        self.units_number = units_number
        self.platform = platform
        self.evaluation_managers = [SequentialEvaluationManager(patterns, eval_mechanism_params) for _ in
                                    range(self.units_number)]

    def _check_first_event(self, first_event: Event):
        raise NotImplementedError()

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Activates the actual parallel algorithm.
        """
        first_event = Event(events.first(), data_formatter)
        self._check_first_event(first_event)
        execution_units = list()
        for unit_id, evaluation_manager in enumerate(self.evaluation_managers):
            execution_unit = DataParallelExecutionUnit(self.platform,
                                                       unit_id,
                                                       evaluation_manager,
                                                       self._get_matches(matches, unit_id),
                                                       data_formatter)
            execution_unit.start()
            execution_units.append(execution_unit)

        # iterate over all events
        for raw_event in events:
            event = Event(raw_event, data_formatter)
            for unit_id in self._classifier(event):
                execution_units[unit_id].add_event(raw_event)

        for execution_unit in execution_units:
            execution_unit.wait()

    def _get_matches(self, matches: OutputStream, unit_id: int):
        return matches

    def _classifier(self, event: Event):
        raise NotImplementedError()

    def get_structure_summary(self):
        return tuple(map(lambda em: em.get_structure_summary(), self.evaluation_managers))



class DataParallelExecutionUnit:
    def __init__(self, platform, unit_id, evaluation_manager, matches, data_formatter):
        self.events = Stream()
        self.execution_unit = platform.create_parallel_execution_unit(unit_id,
                                                                      self._run,
                                                                      evaluation_manager,
                                                                      self.events,
                                                                      matches,
                                                                      data_formatter)

    def start(self):
        self.execution_unit.start()

    def add_event(self, raw_event):
        self.events.add_item(raw_event)
        self.events.task_done()

    def wait(self):
        self.events.join()
        self.events.close()
        self.execution_unit.wait()

    @staticmethod
    def _run(evaluation_manager: EvaluationManager,
             events: InputStream,
             matches: OutputStream,
             data_formatter: DataFormatter):
        evaluation_manager.eval(events, matches, data_formatter)
