from abc import ABC
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform, Lock
from stream.Stream import *
from parallel.manager.EvaluationManager import EvaluationManager
from parallel.manager.SequentialEvaluationManager import SequentialEvaluationManager
from typing import Set, Callable


class DataParallelExecutionAlgorithm(ABC):
    """
    An abstract base class for all data parallel evaluation algorithms.
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform: ParallelExecutionPlatform):
        self.units_number = units_number
        self.platform = platform
        # create SequentialEvaluationManager for every unit
        self.evaluation_managers = [SequentialEvaluationManager(patterns, eval_mechanism_params)
                                    for _ in range(self.units_number)]
        self.match_lock = platform.create_lock()

    def eval(self, events: InputStream, matches: OutputStream, data_formatter: DataFormatter):
        """
        Activates the parallel algorithm of the instance.
        """
        execution_units = list()
        # create and run execution unit for each unit
        for unit_id, evaluation_manager in enumerate(self.evaluation_managers):
            execution_unit = self.ExecutionUnit(self.platform,
                                                unit_id,
                                                evaluation_manager,
                                                self.FilterStream(skip_item=self._create_skip_item(unit_id),
                                                                  matches=matches,
                                                                  unit_id=unit_id,
                                                                  lock=self.match_lock),
                                                data_formatter)
            execution_unit.start()
            execution_units.append(execution_unit)

        # iterate over all events
        for raw_event in events:
            event = Event(raw_event, data_formatter)
            for unit_id in self._classifier(event):
                execution_units[unit_id].add_event(raw_event)

        # waits for all execution_units to terminate
        for execution_unit in execution_units:
            execution_unit.wait()

        # close global OutputStream (only here) after all execution units finished
        matches.close()

    def _create_skip_item(self, unit_id: int) -> Callable[[PatternMatch], bool]:
        """
        Returns a function for filtering out the matches arriving from the different execution units.
        """
        raise NotImplementedError()

    def _classifier(self, event: Event) -> Set[int]:
        """
        returns list of unit ids that will evaluate the event
        """
        raise NotImplementedError()

    def get_structure_summary(self):
        return tuple(map(lambda em: em.get_structure_summary(), self.evaluation_managers))

    class ExecutionUnit:
        """
        A wrap for single unit that has input stream and an execution unit.
        """
        def __init__(self, platform, unit_id, evaluation_manager, matches, data_formatter):
            self.events = Stream()
            self.execution_unit = platform.create_parallel_execution_unit(unit_id,
                                                                          self._run,
                                                                          evaluation_manager,
                                                                          self.events,
                                                                          matches,
                                                                          data_formatter)
            self.unit_id = unit_id

        def start(self):
            self.execution_unit.start()

        def add_event(self, raw_event):
            """
            :param raw_event: from the input stream
            :returns: adds a specific event to the execution unit's event stream.
            """
            self.events.add_item(raw_event)

        def wait(self):
            self.events.close()
            self.execution_unit.wait()

        @staticmethod
        def _run(evaluation_manager: EvaluationManager,
                 events: InputStream,
                 matches: OutputStream,
                 data_formatter: DataFormatter):
            evaluation_manager.eval(events, matches, data_formatter)

    class FilterStream(Stream):
        """
        Used to filter matches coming from the execution manager into the output stream to prevent duplicates
        (same data from different units).
        """
        def __init__(self, skip_item: Callable[[PatternMatch], bool], matches: OutputStream, unit_id: int, lock: Lock):
            super().__init__()
            self.matches = matches
            # set the unique_match function
            self.skip_item = skip_item
            self.unit_id = unit_id
            self.lock = lock

        def add_item(self, item: PatternMatch):
            """
            adds to the stream only the first occurrence of the item (to prevent duplicates)
            """
            if not self.skip_item(item):
                self.lock.acquire()
                self.matches.add_item(item)
                self.lock.release()

        def close(self):
            pass
