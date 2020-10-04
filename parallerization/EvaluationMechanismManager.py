from TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
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

class EvaluationMechanismManager:

    def __init__(self, work_load_fr: ParallelWorkLoadFramework, eval_params: EvaluationMechanismParameters, patterns: List[Pattern]):
        self.masters_list = None
        self.eval_mechanism_list = None
        self.eval_mechanism_families = None
        self.eval_params = eval_params
        self.patterns = patterns
        self.pattern_matches_list = []
        self.data_formatter = None


        if work_load_fr is None:
            self.work_load_fr = ParallelWorkLoadFramework(1, False, False, patterns, 0)  # here no parallelism is needed
        else:
            self.work_load_fr = work_load_fr

        if len(patterns) == 1:          # single pattern
            self.source_eval_mechanism = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_params, self.patterns[0])
        else:                            # multi pattern
            raise NotImplementedError()

    def initialize(self, pattern_matches):
        if (not self.work_load_fr.get_is_data_splitted()) and (not self.work_load_fr.get_is_structure_parallelized()):
            self.initialize_single_tree_single_data(pattern_matches)
        elif self.work_load_fr.get_is_data_splitted() and (not self.work_load_fr.get_is_structure_parallelized()):
            self.initialize_single_tree_multiple_data()
        elif (not self.work_load_fr.get_is_data_splitted()) and self.work_load_fr.get_is_structure_parallelized():
            self.initialize_multiple_tree_single_data()
        elif self.work_load_fr.get_is_data_splitted() and self.work_load_fr.get_is_structure_parallelized():
            self.initialize_multiple_tree_multiple_data()

    def initialize_single_tree_single_data(self, pattern_matches: OutputStream):
        self.eval_mechanism_list = [self.source_eval_mechanism]
        self.masters_list = None
        self.pattern_matches_list = [pattern_matches]

    def initialize_single_tree_multiple_data(self):
        self.eval_mechanism_list, self.masters_list = self.work_load_fr.duplicate_structure(self.source_eval_mechanism, self.eval_params)

    def initialize_multiple_tree_single_data(self):
        self.eval_mechanism_list, self.masters_list = self.work_load_fr.split_structure(self.source_eval_mechanism, self.eval_params, self.patterns[0])

    def initialize_multiple_tree_multiple_data(self):
        self.eval_mechanism_list = None

        # eval_mechanism_families is list of lists
        self.eval_mechanism_families, self.masters_list = self.work_load_fr.split_structure_to_families(self.source_eval_mechanism, self.eval_params)

    def eval(self, event_stream: InputStream, pattern_matches: OutputStream, data_formatter: DataFormatter):
        self.work_load_fr.set_events(event_stream)
        self.work_load_fr.set_data_formatter(data_formatter)

        self.initialize(pattern_matches)

        self.run_eval()

        if self.work_load_fr.get_is_data_parallelised() or self.work_load_fr.get_is_structure_parallelized():
            self.notify_all_to_finish()
            self.wait_masters_to_finish()
            self.get_results_from_masters()

        pattern_matches = self.convert_to_one_stream(self.pattern_matches_list)

    def run_eval(self):
        multiple_data = self.work_load_fr.get_is_data_parallelised()
        multiple_structures = self.work_load_fr.get_is_structure_parallelized()

        if (not multiple_data) and (not multiple_structures):
            self.eval_single_tree_single_data()
        elif (multiple_data) and (not multiple_structures):
            self.eval_single_tree_multiple_data()
        elif (not multiple_data) and (multiple_structures):
            self.eval_multiple_tree_single_data()
        elif (multiple_data) and (multiple_structures):
            self.eval_multiple_tree_multiple_data()

    def get_results_from_masters(self):
        for i in len(self.masters_list):
            pattern_matches = self.masters_list[i].get_pattern_matches()
            self.pattern_matches_list += pattern_matches

    def wait_masters_to_finish(self):
        for i in len(self.masters_list):
            self.masters_list[i].wait_till_finish()

    def notify_all_to_finish(self):
        for i in len(self.eval_mechanism_list):
            self.eval_mechanism_list[i].stop()

    def eval_single_tree_single_data(self):
        self.source_eval_mechanism.eval(self.event_stream, self.pattern_matches_list[0], self.data_formatter)

    def eval_single_tree_multiple_data(self):
        self.eval_util()

    def eval_multiple_tree_single_data(self):
        self.eval_util()

    def eval_util(self):
        self.activate_all_ems(self.eval_mechanism_list)

        event, em_indexes = self.work_load_fr.get_next_event_and_destinations_em()

        while (event is not None):
            for index in em_indexes:
                em = self.eval_mechanism_list[index]
                em.proccess_event(event)
            event, em_indexes = self.work_load_fr.get_next_event_and_destinations_em()

    def eval_multiple_tree_multiple_data(self, data_formatter: DataFormatter):
        for family in self.eval_mechanism_families:
            self.activate_all_ems(family)

        event, families_indexes, em_indexes_list = self.work_load_fr.get_next_event_family_and_destinations_em()

        while (event is not None):
            for family_index in families_indexes:
                family = self.eval_mechanism_families[family_index]
                em_indexes = # TODO:
                for em_index in em_indexes:
                    em = family[em_index]
                    em.proccess_event(event)
            event, family_index, em_index = self.work_load_fr.get_next_event_family_and_destinations_em()

    def activate_all_ems(self, eval_mechanism_list):
        for em in eval_mechanism_list:
            em.activate()
