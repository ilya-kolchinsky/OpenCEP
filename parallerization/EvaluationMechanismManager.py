
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

        #1
        if len(patterns) == 1:               #if there is a single pattern
            self.source_eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism\
                (eval_mechanism_type, eval_params, self.patterns[0])
        else:                               #multi pattern
            raise NotImplementedError()

        #3
        if (self.work_load_fr.get_execution_units() > 1):
            self.master, self.eval_mechanism_list = self.work_load_fr.split_structure(self.source_eval_mechanism)
        else:
            self.eval_mechanism_list = []

    def eval(self, event_stream: Stream, pattern_matches, is_async, file_path, time_limit):
        if event_stream is None:
            raise Exception("Missing event_stream")

        self.source_event_stream = event_stream
        self.pattern_matches = pattern_matches

        if self.work_load_fr.get_is_data_splitted() == True:
            self.event_stream_splitted = self.work_load_fr.split_data(event_stream)
        else:
            self.event_stream_splitted = []

        self.eval_util(is_async, file_path, time_limit)

    def eval_util(self, is_async, file_path, time_limit):
        # TODO: continue from here:

        if len(self.eval_mechanism_list) == 0 and len(self.event_stream_splitted) == 0:
            if is_async:
                self.s.eval(self.event_stream, self.__pattern_matches, is_async=True, file_path=file_path,
                            time_limit=time_limit)
            else:
                self.__eval_mechanism_manager.eval(event_stream, self.__pattern_matches)
        elif:
            pass
        elif:
            pass
        elif:
            pass
        for evaluation_mechanism in self.eval_mechanism_list:
            self.execution_fr.eval(evaluation_mechanism)

    def get_matches(self):#maybe we don't need that
        raise NotImplementedError()



