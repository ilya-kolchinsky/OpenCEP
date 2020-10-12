from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismFactory,
)
from stream.Stream import InputStream, OutputStream
from base.Pattern import Pattern
from base.DataFormatter import DataFormatter

from parallerization.ParallelWorkLoadFramework import ParallelWorkLoadFramework
from typing import List


class EvaluationMechanismManager:

    def __init__(self, work_load_fr: ParallelWorkLoadFramework, eval_params: EvaluationMechanismParameters, patterns: List[Pattern]):
        self.masters_list = None
        self.eval_mechanism_list = None
        self.eval_mechanism_families = None
        self.eval_params = eval_params
        self.patterns = patterns
        self.pattern_matches_stream = None
        self.pattern_matches_list = None

        if work_load_fr is None:
            self.work_load_fr = ParallelWorkLoadFramework(1, False, False, 0)  # no parallelism is needed
        else:
            self.work_load_fr = work_load_fr

        if len(patterns) == 1:          # single pattern
            self.source_eval_mechanism = EvaluationMechanismFactory.\
                build_single_pattern_eval_mechanism(eval_params, self.patterns[0])
        else:                            # multi pattern
            raise NotImplementedError()

    def initialize(self, pattern_matches):
        self.pattern_matches_stream = pattern_matches

        if not self.work_load_fr.get_is_data_parallel() and not self.work_load_fr.get_is_structure_parallel():
            self.initialize_single_tree_single_data(self.pattern_matches_stream)
        elif self.work_load_fr.get_is_data_parallel() and not self.work_load_fr.get_is_structure_parallel():
            self.initialize_single_tree_multiple_data()
        elif not self.work_load_fr.get_is_data_parallel() and self.work_load_fr.get_is_structure_parallel():
            self.initialize_multiple_tree_single_data()
        elif self.work_load_fr.get_is_data_parallel() and self.work_load_fr.get_is_structure_parallel():
            self.initialize_multiple_tree_multiple_data()

    def initialize_single_tree_single_data(self, pattern_matches: OutputStream):
        self.eval_mechanism_list = [self.source_eval_mechanism]
        self.masters_list = None
        self.pattern_matches_list = [pattern_matches]

    def initialize_single_tree_multiple_data(self):
        self.eval_mechanism_list, self.masters_list = self.work_load_fr.duplicate_structure(self.eval_params)

    def initialize_multiple_tree_single_data(self):
        self.eval_mechanism_list, self.masters_list = self.work_load_fr.split_structure(self.eval_params)

    def initialize_multiple_tree_multiple_data(self):
        self.eval_mechanism_families, self.masters_list = self.work_load_fr.split_structure_to_families(self.eval_params)
        self.eval_mechanism_list = []

        for family in self.eval_mechanism_families:
            for em in family:
                self.eval_mechanism_list.append(em)

    def eval(self, event_stream: InputStream, pattern_matches: OutputStream, data_formatter: DataFormatter):
        self.work_load_fr.set_events(event_stream)
        self.work_load_fr.set_data_formatter(data_formatter)

        self.initialize(pattern_matches)

        self.run_eval()

        if self.work_load_fr.get_is_data_parallel() or self.work_load_fr.get_is_structure_parallel():
            self.notify_all_to_finish()

            self.work_load_fr.wait_masters_to_finish()

            self.get_results_from_masters()

    def run_eval(self):
        multiple_data = self.work_load_fr.get_is_data_parallel()
        multiple_structures = self.work_load_fr.get_is_structure_parallel()

        if not multiple_data and not multiple_structures:
            self.eval_single_tree_single_data()
        elif multiple_data and not multiple_structures:
            self.eval_single_tree_multiple_data()
        elif not multiple_data and multiple_structures:
            self.eval_multiple_tree_single_data()
        elif multiple_data and multiple_structures:
            self.eval_multiple_em_multiple_data()

    def eval_single_tree_single_data(self):
        self.source_eval_mechanism.eval(self.work_load_fr.event_stream, self.pattern_matches_list[0],
                                        self.work_load_fr.data_formatter)

    def eval_single_tree_multiple_data(self):
        self.activate_all_ems()

        events, em_indexes = self.work_load_fr.get_data_stream_and_destinations()
        event_index = 0
        while events is not None:
            for index in em_indexes:
                em = self.eval_mechanism_list[index]
                event = events[event_index]
                em.process_event(event)
                event_index += 1
            events, em_indexes = self.work_load_fr.get_data_stream_and_destinations()
            event_index = 0

    def eval_multiple_tree_single_data(self):
        self.activate_all_ems()

        events, em_indexes = self.work_load_fr.get_data_stream_and_destinations()
        event_index = 0
        while events is not None:
            for index in em_indexes:
                em = self.eval_mechanism_list[index]
                event = events[event_index]
                em.process_event(event)
                event_index += 1
            events, em_indexes = self.work_load_fr.get_data_stream_and_destinations()
            event_index = 0

    def eval_multiple_em_multiple_data(self):
        self.activate_all_ems()

        families_events, families_indexes, em_indexes_list = self.work_load_fr.get_data_stream_and_destinations()

        while families_events is not None:
            for i in range(len(families_indexes)):
                family_index = families_indexes[i]
                family = self.eval_mechanism_families[family_index]
                em_indexes = em_indexes_list[i]
                family_events = families_events[family_index]

                for em_index in em_indexes:
                    em = family[em_index]
                    event = family_events[em_index]
                    em.process_event(event)

            families_events, families_indexes, em_indexes_list = self.work_load_fr.get_data_stream_and_destinations()

    def activate_all_ems(self):
        for em in self.eval_mechanism_list:
            em.activate()

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

    def notify_all_to_finish(self):
        for i in range(len(self.eval_mechanism_list)):
            self.eval_mechanism_list[i].stop()

    def get_structure_summary(self):
        return self.source_eval_mechanism.get_structure_summary()

# TODO:
# multi + multi - tests

# remove None from original eval
# decide on sleep time
# refactor