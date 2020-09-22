
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismTypes,
    EvaluationMechanismFactory,
)
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
from base.DataFormatter import DataFormatter

from parallerization import ParallelWorkLoadFramework, ParallelExecutionFramework
from typing import List

import threading


class EvaluationMechanismManager:

    def __init__(self, work_load_fr: ParallelWorkLoadFramework, eval_params: EvaluationMechanismParameters,
                 patterns: List[Pattern]):

        self.work_load_fr = work_load_fr
        self.patterns = patterns
        self.masters_list = None
        self.eval_mechanism_list = None
        self.event_stream_splitted = None
        self.source_event_stream = None
        self.pattern_matches_list = [] #list of stream
        self.execution_map = None
        self.results = OutputStream()
        self.eval_params = eval_params

        if len(patterns) == 1:          # single pattern
            self.source_eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism\
                (eval_params, self.patterns[0])
        else:        # multi pattern
            raise NotImplementedError()

    def initialize_single_tree_single_data(self, pattern_matches: OutputStream):
        self.eval_mechanism_list = self.work_load_fr.split_structure(self.source_eval_mechanism,
                                                                      self.eval_params)
        #self.eval_mechanism_list = [self.source_eval_mechanism]
        self.event_stream_splitted = [self.source_event_stream.duplicate()]
        self.pattern_matches_list = [pattern_matches]

    def initialize_multiple_tree_single_data(self):
        self.eval_mechanism_list = self.work_load_fr.split_structure(self.source_eval_mechanism,
                                                                     self.eval_params)
        self.event_stream_splitted = [self.source_event_stream.duplicate()]
        rows, cols = (len(self.eval_mechanism_list), 1)
        for i in range(rows*cols):
            self.pattern_matches_list.append(OutputStream())

    def initialize_single_tree_multiple_data(self):
        self.event_stream_splitted = self.work_load_fr.split_data(self.source_event_stream, self.source_eval_mechanism,
                                                                   self.eval_params)
        self.eval_mechanism_list = self.work_load_fr.get_masters()
        rows, cols = (1, len(self.event_stream_splitted))
        for i in range(cols):
            self.pattern_matches_list.append(OutputStream())

    def initialize_multiple_tree_multiple_data(self):
        self.event_stream_splitted = self.work_load_fr.split_data(self.source_event_stream, self.source_eval_mechanism,
                                                                   self.eval_params)
        self.eval_mechanism_list = self.work_load_fr.split_structure(self.source_eval_mechanism,
                                                                      self.eval_params)
        self.execution_map = self.work_load_fr.get_multiple_data_to_multiple_execution_units_index()
        rows, cols = (len(self.eval_mechanism_list), len(self.event_stream_splitted))
        self.pattern_matches_list = [[OutputStream()] * cols] * rows#TODO: change to for in range...

    def get_data_from_all_masters(self):
        for pmlist in self.pattern_matches_list:
            for match in pmlist:
                if match is not None:
                    self.results.add_item(match)
        self.results.close()
        #return self.results


    def eval(self, event_stream: InputStream, pattern_matches: OutputStream, data_formatter: DataFormatter):
        if event_stream is None:
            raise Exception("Missing event_stream")

        self.source_event_stream = event_stream.duplicate()
        self.results = pattern_matches

        if not self.work_load_fr.get_is_data_splitted() and not self.work_load_fr.get_is_tree_splitted():
            self.initialize_single_tree_single_data(pattern_matches) # TODO : apply fix to all cases
        elif not self.work_load_fr.get_is_data_splitted() and self.work_load_fr.get_is_tree_splitted():
            self.initialize_multiple_tree_single_data()
        elif self.work_load_fr.get_is_data_splitted() and not self.work_load_fr.get_is_tree_splitted():
            self.initialize_single_tree_multiple_data()
        elif self.work_load_fr.get_is_data_splitted() and self.work_load_fr.get_is_tree_splitted():
            self.initialize_multiple_tree_multiple_data()

        self.masters_list = self.work_load_fr.get_masters()
        self.eval_util(data_formatter)
        if self.work_load_fr.get_is_data_splitted() or self.work_load_fr.get_is_tree_splitted():
            self.get_data_from_all_masters()
        # self.pattern_matches_list[0].close()    #TODO: change for all cases

    def eval_util(self, data_formatter: DataFormatter):
        if not self.work_load_fr.get_is_tree_splitted() and len(self.event_stream_splitted) == 1:
            self.eval_by_single_tree_single_data( self.source_eval_mechanism,
                                                 self.source_event_stream, self.pattern_matches_list[0], data_formatter)
            #self.masters_list[0].wait_till_finish() => if we are in this case then there is only one thread so no need to wait?
        elif not self.work_load_fr.get_is_tree_splitted() and len(self.event_stream_splitted) > 1:
            self.eval_by_single_tree_multiple_data(data_formatter)
        elif self.work_load_fr.get_is_tree_splitted() and len(self.event_stream_splitted) == 1:
            self.eval_by_multiple_tree_single_data(data_formatter)
        elif self.work_load_fr.get_is_tree_splitted() and len(self.event_stream_splitted) > 1:
            self.eval_by_multiple_tree_multiple_data(data_formatter)

    def eval_by_single_tree_single_data(self,
                                        eval_mechanism: ParallelExecutionFramework, event_stream: InputStream,
                                        pattern_matches:OutputStream, data_formatter):
            eval_mechanism.eval(event_stream, pattern_matches, data_formatter)


    def eval_by_single_tree_multiple_data(self, data_formatter):
        for i in range(len(self.event_stream_splitted)):
            event_stream = self.event_stream_splitted[i]
            pattern_match = self.pattern_matches_list[i]

            self.eval_by_single_tree_single_data( self.eval_mechanism_list[i],
                                                 event_stream, pattern_match, data_formatter)

        for i in range(len(self.event_stream_splitted)):
            self.masters_list[i].thread.join()

        #print("Thread end count:" , threading.active_count())

    def eval_by_multiple_tree_single_data(self, data_formatter):
        for i in range(len(self.eval_mechanism_list)):
            event_stream = self.source_event_stream.duplicate()
            pattern_match = self.pattern_matches_list[i]
            eval_mechanism = self.eval_mechanism_list[i]
            self.eval_by_single_tree_single_data( eval_mechanism, event_stream,
                                                      pattern_match, data_formatter)
            #self.masters_list[0].wait_till_finish()
        for i in range(len(self.event_stream_splitted)):
            self.masters_list[i].thread.join()

    def eval_by_multiple_tree_multiple_data(self, data_formatter):
        start_index_of_ems = 0
        for i in range(len(self.execution_map)):
            event_stream = self.event_stream_splitted[i]
            for j in range(len(self.execution_map[i])):
                eval_mechanism = self.eval_mechanism_list[start_index_of_ems + j] # UnaryParallelTree
                start_index_of_ems += 1
                self.eval_by_single_tree_single_data(eval_mechanism, event_stream, self.pattern_matches_list[i,j], data_formatter)

        for k in range(len(self.masters_list)):
            self.masters_list[k].wait_till_finish()

    def get_matches(self):#EVA maybe we dont need it
        if len(self.pattern_matches_list) > 0:
            return self.pattern_matches_list
        else:
            return self.pattern_matches

    def get_structure_summary(self):#To allow other tests
        return self.source_eval_mechanism.get_structure_summary()



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
