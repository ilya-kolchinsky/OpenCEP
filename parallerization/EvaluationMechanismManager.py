
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

    def __init__(self, work_load_fr: ParallelWorkLoadFramework,
                 eval_mechanism_type: EvaluationMechanismTypes, eval_params: EvaluationMechanismParameters, patterns: List[Pattern]):

        self.work_load_fr = work_load_fr
        self.patterns = patterns
        self.masters_list = None
        self.eval_mechanism_list = None
        self.event_stream_splitted = None
        self.source_event_stream = None
        self.pattern_matches = None
        self.pattern_matches_list = None
        self.execution_map = None
        self.results = None

        if len(patterns) == 1:          # single pattern
            self.source_eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism\
                (eval_mechanism_type, eval_params, self.patterns[0])
        else:        # multi pattern
            raise NotImplementedError()

    def initialize_single_mechanizm_single_data(self):
        self.eval_mechanism_list = [self.source_eval_mechanism]
        self.event_stream_splitted = [self.source_event_stream]
        self.pattern_matches_list = [Stream()]

    def initialize_multiple_mechanizm_single_data(self):
        self.eval_mechanism_list = self.work_load_fr.split_structure(self.source_eval_mechanism)
        self.event_stream_splitted = [self.source_event_stream]
        rows, cols = (len(self.eval_mechanism_list), 1)
        self.pattern_matches_list = [[Stream()] * cols] * rows

    def initialize_single_mechanizm_multiple_data(self):
        self.eval_mechanism_list = [self.source_eval_mechanism]
        self.event_stream_splitted = self.work_load_fr.split_data(self.source_event_stream)
        rows, cols = (1, len(self.event_stream_splitted))
        self.pattern_matches_list = [[Stream()] * cols] * rows

    def initialize_multiple_mechanizm_multiple_data(self):
        self.eval_mechanism_list = self.work_load_fr.split_structure(self.source_eval_mechanism)
        self.event_stream_splitted = self.work_load_fr.split_data(self.source_event_stream)
        self.execution_map = self.work_load_fr.get_multiple_data_to_multiple_execution_units_index()
        rows, cols = (len(self.eval_mechanism_list), len(self.event_stream_splitted))
        self.pattern_matches_list = [[Stream()] * cols] * rows

    def eval(self, event_stream: Stream, pattern_matches, is_async: bool = False, file_path = None, time_limit = None):
        if event_stream is None:
            raise Exception("Missing event_stream")

        self.source_event_stream = event_stream.duplicate()
        self.masters_list = self.work_load_fr.get_masters()
        self.results = pattern_matches

        if not self.work_load_fr.get_is_data_splitted() and self.work_load_fr.get_execution_units() == 1:
            self.initialize_single_mechanizm_single_data()
        elif not self.work_load_fr.get_is_data_splitted() and self.work_load_fr.get_execution_units() > 1:
            self.initialize_multiple_mechanizm_single_data()
        elif self.work_load_fr.get_is_data_splitted() and self.work_load_fr.get_execution_units() == 1:
            self.initialize_single_mechanizm_multiple_data()
        elif  self.work_load_fr.get_is_data_splitted() and self.work_load_fr.get_execution_units() > 1:
            self.initialize_multiple_mechanizm_multiple_data()

        self.eval_util(is_async, file_path, time_limit)
        self.results = getDataFromAllMasters()


    def eval_util(self, is_async, file_path, time_limit):
        if len(self.eval_mechanism_list) == 1 and len(self.event_stream_splitted) == 1:
            self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit, self.source_eval_mechanism, self.source_event_stream, self.pattern_matches)
            self.master_list[0].wait_till_finish()
        elif len(self.eval_mechanism_list) == 1 and len(self.event_stream_splitted) > 1:
            self.eval_by_single_mechanizm_multiple_data(is_async, file_path, time_limit)
        elif len(self.eval_mechanism_list) > 1 and len(self.event_stream_splitted) == 1:
            self.eval_by_multiple_mechanizm_single_data(is_async, file_path, time_limit)
            self.master_list[0].wait_till_finish()
        elif len(self.eval_mechanism_list) > 1 and len(self.event_stream_splitted) > 1:
            self.eval_by_multiple_mechanizm_multiple_data(is_async, file_path, time_limit)

    def eval_by_single_mechanizm_single_data(self, is_async, file_path, time_limit, eval_mechanism, event_stream, pattern_matches):
        if is_async:
            eval_mechanism.eval(event_stream, pattern_matches, is_async=True, file_path=file_path, time_limit=time_limit)
        else:
            eval_mechanism.eval(event_stream, pattern_matches)

    def eval_by_single_mechanizm_multiple_data(self, is_async, file_path, time_limit):
        for i in range(len(self.event_stream_splitted)):
            event_stream = self.event_stream_splitted[i]
            pattern_match = self.pattern_matches_list[i]
            self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit, self.eval_mechanism_list[0],
                                                      event_stream, pattern_match)

            self.master_list[0].wait_till_finish()
            self.eval_mechanism_list[0].restart_state_for_next_run()

    def eval_by_multiple_mechanizm_single_data(self, is_async, file_path, time_limit):
        for i in range(len(self.eval_mechanism_list)):
            event_stream = self.source_event_stream.duplicate()
            pattern_match = self.pattern_matches_list[i]
            eval_mechanism = self.eval_mechanism_list[i]
            self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit, eval_mechanism, event_stream, pattern_match)
            self.master_list[0].wait_till_finish()
            eval_mechanism.restart_state_for_next_run()

    def eval_by_multiple_mechanizm_multiple_data(self, is_async, file_path, time_limit):
        start_index_of_ems = 0
        for i in range (len(self.execution_map)):
            event_stream = self.event_stream_splitted[i]
            for j in range(len(self.execution_map[i])):
                eval_mechanism = self.eval_mechanism_list[start_index_of_ems + j] # UnaryParallelTree
                start_index_of_ems += 1
                self.eval_by_single_mechanizm_single_data(is_async, file_path, time_limit,eval_mechanism, event_stream, self.pattern_matches_list[i,j])

        for k in range(len(self.masters_list)):
            self.master_list[k].wait_till_finish()

    def get_matches(self):
        if len(self.pattern_matches_list) > 0:
            return self.pattern_matches_list
        else:
            return self.pattern_matches



# single em->single data
# ems = [em]
# datas = [data]
#
# single em->mult data
# ems = [em]
# datas = [data1, data2, data3]
# em.eval(data1)
# em.eval(data2)
# em.eval(data3)
#
# mult em->single data
# ems=[em1,em2,em3]
# datas=[data]
# em1.eval(data)
# em2.eval(data)
# em3.eval(data)
#
# mult em->mult data
# general: map = {4-1, 2-2, 5-3}
# us: map = {}
# ems = [em1,em2,em3,em4 ,em5,em6,em7]
# datas = [data1, dat2]
#
# ems= [em1,em2,em3,                em4,em5,em6,em7,em7]
# datas = [data1,data1,data1        data2,data2,data2,data2]
#
# split_data
# map(ems->single_data)
#
# em1.eval(data)
# em2.eval(data)
# em3.eval(data)
