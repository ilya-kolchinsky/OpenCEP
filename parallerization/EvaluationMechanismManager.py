
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismTypes,
    EvaluationMechanismFactory,
)
from misc.IOUtils import Stream
from base.Pattern import Pattern

from parallerization import ParallelWorkLoadFramework, ParallelExecutionFramework
from typing import List


class EvaluationMechanismManager:

    def __init__(self, work_load_fr: ParallelWorkLoadFramework, execution_fr: ParallelExecutionFramework,
                 eval_mechanism_type: EvaluationMechanismTypes, eval_params: EvaluationMechanismParameters, patterns: List[Pattern]):

        self.work_load_fr = work_load_fr
        self.execution_fr = execution_fr
        self.patterns = patterns
        self.master = None
        self.eval_mechanism_list = None
        self.event_stream_splitted = None
        self.source_event_stream = None
        self.pattern_matches = None
        self.pattern_matches_list = None

        if len(patterns) == 1:          # single pattern
            self.source_eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism\
                (eval_mechanism_type, eval_params, self.patterns[0])
        else:        # multi pattern
            raise NotImplementedError()

        if self.work_load_fr.get_execution_units() > 1:
            self.master = self.eval_mechanism_list = self.work_load_fr.split_structure(self.source_eval_mechanism)
        else:
            self.eval_mechanism_list = []

    def eval(self, event_stream: Stream, pattern_matches, is_async: bool = False, file_path = None, time_limit = None):
        if event_stream is None:
            raise Exception("Missing event_stream")

        self.source_event_stream = event_stream.duplicate()

        if self.work_load_fr.get_is_data_splitted():
            self.event_stream_splitted = self.work_load_fr.split_data(event_stream)
            self.pattern_matches_list = [Stream()] * len(self.event_stream_splitted)
        else:
            self.event_stream_splitted = []
            self.pattern_matches = pattern_matches

        self.eval_util(is_async, file_path, time_limit)

    def eval_util(self, is_async, file_path, time_limit):
        if len(self.eval_mechanism_list) == 1 and len(self.event_stream_splitted) == 0:
            self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit, self.source_eval_mechanism,
                                                      self.source_event_stream, self.pattern_matches)
        elif len(self.eval_mechanism_list) == 1 and len(self.event_stream_splitted) > 0:
            self.eval_by_single_mechanizm_multiple_data(is_async, file_path, time_limit)
        elif len(self.eval_mechanism_list) > 1 and len(self.event_stream_splitted) == 0:
            self.eval_by_multiple_mechanizm_single_data(is_async, file_path, time_limit)
        elif len(self.eval_mechanism_list) > 1 and len(self.event_stream_splitted) > 0:
            self.eval_by_multiple_mechanizm_multiple_data(is_async, file_path, time_limit)

    def eval_by_single_mechanizm_single_data(self, is_async, file_path, time_limit, eval_mechanism, event_stream, pattern_matches):
        if is_async:
            eval_mechanism.eval(event_stream, pattern_matches, is_async=True, file_path=file_path, time_limit=time_limit)
        else:
            eval_mechanism.eval(self.source_event_stream, pattern_matches)

    def eval_by_single_mechanizm_multiple_data(self, is_async, file_path, time_limit):
        for i in range(len(self.event_stream_splitted)):
            event_stream = self.event_stream_splitted[i]
            pattern_match = self.pattern_matches_list[i]
            self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit, self.source_eval_mechanism,
                                                      event_stream, pattern_match)

    def eval_by_multiple_mechanizm_single_data(self, is_async, file_path, time_limit):
        for i in range(len(self.eval_mechanism_list)):
            event_stream = self.source_event_stream
            pattern_match = self.pattern_matches_list[i]
            eval_mechanism = self.eval_mechanism_list[i]
            self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit, eval_mechanism,
                                                      event_stream, pattern_match)

    def eval_by_multiple_mechanizm_multiple_data(self, is_async, file_path, time_limit):
        data_for_each = len(self.event_stream_splitted) / len(self.eval_mechanism_list)
        remainder = len(self.event_stream_splitted) - data_for_each * len(self.eval_mechanism_list)

        data_index = 0
        for i in range (len(self.eval_mechanism_list)):
            eval_mechanism = self.eval_mechanism_list[i]
            for j in range(round(data_for_each)):#for round, need to check if upper or lower...
                assert len(self.event_stream_splitted) == len(self.pattern_matches_list)
                event_stream = self.event_stream_splitted[data_index]
                pattern_match = self.pattern_matches_list[data_index]
                self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit,eval_mechanism, event_stream, pattern_match)
                data_index += 1

        if remainder > 0:
           eval_mechanism = self.eval_mechanism_list[0]
           for j in range(remainder):
               event_stream = self.event_stream_splitted[data_index]
               pattern_match = self.pattern_matches_list[data_index]
               self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit, eval_mechanism, event_stream, pattern_match)
               data_index += 1

    def get_matches(self):
        if len(self.pattern_matches_list) > 0:
            return self.pattern_matches_list
        else:
            return self.pattern_matches
