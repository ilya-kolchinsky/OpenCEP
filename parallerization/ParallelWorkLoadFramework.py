from abc import ABC

import DataFormatter
from evaluation.EvaluationMechanism import EvaluationMechanism
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters


class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units: int = 1, is_data_parallelised: bool = False, is_em_splitted: bool = False, famalies = 0):
        self._execution_units = execution_units
        self.famalies = famalies
        self._is_data_parallelised = is_data_parallelised
        self._is_em_splitted = is_em_splitted
        self.data_formatter = None
        self.event_stream = None

    def get_execution_units(self):
        return self._execution_units

    def get_is_data_parallelised(self):
        return self._is_data_parallelised

    def get_is_structure_parallelized(self):
        return self._is_em_splitted

    def set_source_eval_mechanism(self, eval_mechanism: EvaluationMechanism):
        self._source_eval_mechanism = eval_mechanism

    def get_source_eval_mechanism(self):
        return self._source_eval_mechanism

    def set_events(self, events):
        self.event_stream = events

    def set_data_formatter(self, data_formatter):
        self.data_formatter = data_formatter

    def split_structure(self, evaluation_mechanism: EvaluationMechanism, eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()

    def get_next_event_and_destinations_em(self):
        NotImplementedError()

    def get_next_event_family_and_destinations_em(self):
        NotImplementedError()

    def duplicate_structure(self, evaluation_mechanism: EvaluationMechanism, eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()

    def split_structure_to_families(self, evaluation_mechanism: EvaluationMechanism, eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()



