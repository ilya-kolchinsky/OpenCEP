from abc import ABC

from base.DataFormatter import DataFormatter
from evaluation.EvaluationMechanism import EvaluationMechanism
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters


class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units: int = 1, is_data_parallelized: bool = False,
                 is_structure_parallelized: bool = False, num_of_families: int = 0):
        self._execution_units = execution_units
        self.num_of_families = num_of_families
        self._is_data_parallelized = is_data_parallelized
        self._is_structure_parallelized = is_structure_parallelized
        self.data_formatter = None
        self.event_stream = None
        self._source_eval_mechanism = None

    def get_execution_units(self):
        return self._execution_units

    def get_is_data_parallelized(self):
        return self._is_data_parallelized

    def get_is_structure_parallelized(self):
        return self._is_structure_parallelized

    def set_source_eval_mechanism(self, eval_mechanism: EvaluationMechanism):
        self._source_eval_mechanism = eval_mechanism

    def get_source_eval_mechanism(self):
        return self._source_eval_mechanism

    def set_events(self, events):
        self.event_stream = events

    def set_data_formatter(self, data_formatter):
        self.data_formatter = data_formatter

    def split_structure(self, eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()

    def get_data_stream_and_destinations(self):
        NotImplementedError()

    def get_next_event_families_indexes_and_destinations_ems(self):
        NotImplementedError()

    def duplicate_structure(self, evaluation_mechanism: EvaluationMechanism,
                            eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()

    def split_structure_to_families(self, evaluation_mechanism: EvaluationMechanism,
                                    eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()

    def join_all(self):
        raise NotImplementedError()



