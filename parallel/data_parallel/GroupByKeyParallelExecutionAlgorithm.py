from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform
from parallel.manager.SequentialEvaluationManager import SequentialEvaluationManager
from misc.Utils import is_int, is_float
from parallel.manager.EvaluationManager import EvaluationManager


class GroupByKeyParallelExecutionAlgorithm(DataParallelExecutionAlgorithm):
    """
    Implements the key-based partitioning algorithm.
    """

    def __init__(self,
                 units_number,
                 patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform: ParallelExecutionPlatform,
                 key: str):
        super().__init__(units_number, patterns, eval_mechanism_params, platform)
        self.__key = key
        self.evaluation_managers = [SequentialEvaluationManager(self.patterns, self.eval_mechanism_params) for _ in range(self.units_number)]

    def eval(self,
             events: InputStream, matches: OutputStream,
             data_formatter: DataFormatter):

        self._check_legal_input(events, data_formatter)

        def run_t(t_evaluation_manager: EvaluationManager,
                  t_events: InputStream,
                  t_matches: OutputStream,
                  t_data_formatter: DataFormatter):
            t_evaluation_manager.eval(t_events, t_matches, t_data_formatter)

        execution_units = list()
        events_streams = list()
        for unit_id, evaluation_manager in enumerate(self.evaluation_managers):
            input_events = Stream()
            execution_unit = self.platform.create_parallel_execution_unit(unit_id,
                                                                          run_t,
                                                                          evaluation_manager,
                                                                          input_events,
                                                                          matches,
                                                                          data_formatter)
            events_streams.append(input_events)
            execution_units.append(execution_unit)
            execution_unit.start()

        for raw_event in events:
            payload = data_formatter.parse_event(raw_event)
            value = payload[self.__key]
            input_events = events_streams[int(value) % self.units_number]
            input_events.add_item(raw_event)
            input_events.task_done()

        for input_events, execution_unit in zip(events_streams, execution_units):
            input_events.join()
            input_events.close()
            execution_unit.wait()


    def _check_legal_input(self, events: InputStream, data_formatter: DataFormatter):
        first_raw_event = events.first()
        first_event = Event(first_raw_event, data_formatter)
        value = first_event.payload[self.__key]
        if not is_int(value) and not is_float(value):
            raise Exception('Non numeric key')

    def get_structure_summary(self):
        return tuple(map(lambda em: em.get_structure_summary(), self.evaluation_managers))
        # return {unit_id: evaluation_manager.get_structure_summary() for unit_id, evaluation_manager in enumerate(self.evaluation_managers)}

