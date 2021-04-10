from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.DataFormatter import DataFormatter
from base.PatternMatch import *
from stream.Stream import *
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform
from parallel.manager.SequentialEvaluationManager import SequentialEvaluationManager
from misc.Utils import is_int, is_float


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

    def eval(self,
             events: InputStream, matches: OutputStream,
             data_formatter: DataFormatter):

        self._check_legal_input(events, data_formatter)

        def run_t(t_patterns: Pattern or List[Pattern],
                  t_eval_mechanism_params: EvaluationMechanismParameters,
                  t_events: InputStream,
                  t_matches: OutputStream,
                  t_data_formatter: DataFormatter):
            evaluation_manager = SequentialEvaluationManager(t_patterns, t_eval_mechanism_params)
            evaluation_manager.eval(t_events, t_matches, t_data_formatter)

        execution_units = list()
        events_streams = list()
        matches_list = list()
        for unit_id in range(self.units_number):
            t_events = Stream()
            t_matches = OutputStream()
            execution_unit = self.platform.create_parallel_execution_unit(unit_id,
                                                                          run_t,
                                                                          self.patterns,
                                                                          self.eval_mechanism_params,
                                                                          t_events,
                                                                          t_matches,
                                                                          data_formatter)
            events_streams.append(t_events)
            matches_list.append(t_matches)
            execution_units.append(execution_unit)
            execution_unit.start()

        for raw_event in events:
            event = Event(raw_event, data_formatter)
            value = event.payload[self.__key]
            events_streams[int(value) % self.units_number].add_item(raw_event)

        for t_events, t_matches, execution_unit in zip(events_streams, matches_list, execution_units):
            t_events.close()
            execution_unit.wait()
            for t_match in t_matches:
                matches.add_item(t_match)
        matches.close()


    def _check_legal_input(self, events: InputStream, data_formatter: DataFormatter):
        first_raw_event = events.first()
        first_event = Event(first_raw_event, data_formatter)
        value = first_event.payload[self.__key]
        if not is_int(value) and not is_float(value):
            raise Exception('Non numeric key')
