from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismFactory,
)
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
from base.DataFormatter import DataFormatter

from parallerization.ParallelWorkLoadFramework import ParallelWorkLoadFramework
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from typing import List
from base.PatternMatch import PatternMatch


class EvaluationMechanismManager:

    def __init__(self, work_load_fr: ParallelWorkLoadFramework, eval_params: EvaluationMechanismParameters, patterns: List[Pattern]):
        self.masters_list = None
        self.eval_mechanism_list = None
        self.eval_mechanism_families = None
        self.eval_params = eval_params
        self.patterns = patterns
        self.pattern_matches_stream = None
        self.data_formatter = None

        self.streams = None # TODO:

        if work_load_fr is None:
            self.work_load_fr = ParallelWorkLoadFramework(1, False, False, 0)  # here no parallelism is needed
        else:
            self.work_load_fr = work_load_fr

        if len(patterns) == 1:          # single pattern
            self.source_eval_mechanism = EvaluationMechanismFactory.\
                build_single_pattern_eval_mechanism(eval_params, self.patterns[0])
        else:                            # multi pattern
            raise NotImplementedError()

    def initialize(self, pattern_matches):
        self.pattern_matches_stream = pattern_matches

        if not self.work_load_fr.get_is_data_parallelized() and not self.work_load_fr.get_is_structure_parallelized():
            self.initialize_single_tree_single_data(self.pattern_matches_stream)
        elif self.work_load_fr.get_is_data_parallelized() and not self.work_load_fr.get_is_structure_parallelized():
            self.initialize_single_tree_multiple_data()
        elif not self.work_load_fr.get_is_data_parallelized() and self.work_load_fr.get_is_structure_parallelized():
            self.initialize_multiple_tree_single_data()
        elif self.work_load_fr.get_is_data_parallelized() and self.work_load_fr.get_is_structure_parallelized():
            self.initialize_multiple_tree_multiple_data()

    def initialize_single_tree_single_data(self, pattern_matches: OutputStream):
        self.eval_mechanism_list = [self.source_eval_mechanism]
        self.masters_list = None
        self.pattern_matches_list = [pattern_matches]

    def initialize_single_tree_multiple_data(self):
        self.eval_mechanism_list, self.masters_list = self.work_load_fr.duplicate_structure(self.source_eval_mechanism,
                                                                                            self.eval_params)

    def initialize_multiple_tree_single_data(self):
        self.eval_mechanism_list, self.masters_list = self.work_load_fr.split_structure(self.eval_params)

    def initialize_multiple_tree_multiple_data(self):
        self.eval_mechanism_list = None

        # eval_mechanism_families is list of lists
        self.eval_mechanism_families, self.masters_list = self.work_load_fr.\
            split_structure_to_families(self.source_eval_mechanism, self.eval_params)

    def eval(self, event_stream: InputStream, pattern_matches: OutputStream, data_formatter: DataFormatter):
        self.work_load_fr.set_events(event_stream)

        self.streams = [self.work_load_fr.event_stream.duplicate(), self.work_load_fr.event_stream.duplicate()] # TODO: REMOVE

        self.work_load_fr.set_data_formatter(data_formatter)

        self.initialize(pattern_matches)
        try:
            self.run_eval()
        except:
            raise Exception("7")

        print(" manger Finished pushing events")

        if self.work_load_fr.get_is_data_parallelized() or self.work_load_fr.get_is_structure_parallelized():
            try:
                self.notify_all_to_finish()
            except:
                raise Exception("8")
            try:
                # self.wait_masters_to_finish()
                 self.work_load_fr.join_all()
            except:
                raise Exception("9")
            print("get_results_from_masters")
            self.get_results_from_masters()

    def run_eval(self):
        multiple_data = self.work_load_fr.get_is_data_parallelized()
        multiple_structures = self.work_load_fr.get_is_structure_parallelized()

        if not multiple_data and not multiple_structures:
            self.eval_single_tree_single_data()
        elif multiple_data and not multiple_structures:
            self.eval_single_tree_multiple_data()
        elif not multiple_data and multiple_structures:
            self.eval_multiple_tree_single_data()
        elif multiple_data and multiple_structures:
            self.eval_multiple_em_multiple_data()

    def get_results_from_masters(self):
        for i in range(len(self.masters_list)):
            pattern_matches = self.masters_list[i].get_pattern_matches()
            pattern_matches_size = pattern_matches.count()
            for i in range(pattern_matches_size):
                try:
                    match = pattern_matches.get_item()
                    self.pattern_matches_stream.add_item(match)
                except:
                    pass

        self.pattern_matches_stream.close()

    def wait_masters_to_finish(self):
        for i in range(len(self.masters_list)):
            print("Waiting for master " + str(self.eval_mechanism_list[i].get_thread().ident) + " to Finish")
            self.masters_list[i].wait_till_finish()

    def notify_all_to_finish(self):
        for i in range(len(self.eval_mechanism_list)):
            print("Notifying thread " + str(self.eval_mechanism_list[i].get_thread().ident) + " to stop")
            self.eval_mechanism_list[i].stop()

    def eval_single_tree_single_data(self):
        self.source_eval_mechanism.eval(self.work_load_fr.event_stream, self.pattern_matches_list[0],
                                        self.work_load_fr.data_formatter)

    def eval_single_tree_multiple_data(self):
        self.eval_util()

    def eval_multiple_tree_single_data(self):
        self.eval_util()

    # def eval_util(self):
    #     self.activate_all_ems(self.eval_mechanism_list)
    #
    #     event, em_indexes = self.work_load_fr.get_next_event_and_destinations_em()
    #     while event is not None:
    #         for index in em_indexes:
    #             em = self.eval_mechanism_list[index]
    #             em.process_event(event)
    #         event, em_indexes = self.work_load_fr.get_next_event_and_destinations_em()
    #     print("finished pushing events to threads")

    def eval_util(self): # TODO: REMOVE
        self.activate_all_ems(self.eval_mechanism_list)
        master_counter = 0
        x = self.streams[0].count()
        for i in range(x - 2):
            try:
                next_event1 = self.streams[0].get_item()
            except:
                raise Exception("2")
            try:
                next_event2 = self.streams[1].get_item()
            except:
                raise Exception("3")

            # print("pushing events to threads" + str(i))
            try:
                input_stream1 = self.work_load_fr.create_input_stream(next_event1)
                input_stream2 = self.work_load_fr.create_input_stream(next_event2)
            except:
                raise Exception("4")
            try:
                em1 = self.eval_mechanism_list[0]
                em2 = self.eval_mechanism_list[1]
            except:
                raise Exception("5")
            try:
                master_counter += 1
                if master_counter % 10000 == 0:
                    print("master_counter  =  " + str(master_counter))
                em1.process_event(input_stream1)
                em2.process_event(input_stream2)
            except:
                raise Exception("6")

    def eval_multiple_em_multiple_data(self):
        for family in self.eval_mechanism_families:
            self.activate_all_ems(family)

        event, families_indexes, em_indexes_list = self.work_load_fr.get_next_event_families_indexes_and_destinations_ems()

        while event is not None:
            for i in range(0, len(families_indexes)):
                family_index = families_indexes[i]
                family = self.eval_mechanism_families[family_index]
                em_indexes = em_indexes_list[i]
                for em_index in em_indexes:
                    em = family[em_index]
                    em.proccess_event(event)
            event, families_indexes, em_indexes_list = self.work_load_fr.get_next_event_families_indexes_and_destinations_ems()

    def activate_all_ems(self, eval_mechanism_list):
        for em in eval_mechanism_list:
            em.activate()
