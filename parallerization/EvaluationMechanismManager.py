
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
                 eval_mechanism_type: EvaluationMechanismTypes, eval_params: EvaluationMechanismParameters,
                 patterns: List[Pattern]):
        self.work_load_fr = work_load_fr
        self.execution_fr = execution_fr
        self.patterns = patterns

        self._master = None
        self.eval_mechanism_list = []

        self._event_stream_splitted = []

        #1
        if len(patterns) == 1:#if there is a single pattern
            self.source_eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism\
                (eval_mechanism_type, eval_params, self.patterns[0])
        else:#multi pattern
            raise NotImplementedError()



        #3
        self._master, self.eval_mechanism_list = self.work_load_fr.split_structure(self.source_eval_mechanism)

    def eval(self, event_stream: Stream, pattern_matches):

        #2
        self._event_stream_splitted = self.work_load_fr.split_data(event_stream)
        #for events in self._event_stream_splitted: double for?
        for evaluation_mechanism in self.eval_mechanism_list:
            self.execution_fr.eval(evaluation_mechanism)

    def get_matches(self):#maybe we don't need that
        raise NotImplementedError()



