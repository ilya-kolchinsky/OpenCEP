"""
This class contains requirements from the plugin which is responsible of parallelism level, in particular:
defines parallelism level, mechanism creation and duplication, supplies data stream for processing and destination mechanisms.
The manger will call this methods in order to achieve parallelism.
"""

from abc import ABC
from evaluation.EvaluationMechanism import EvaluationMechanism
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.DataFormatter import DataFormatter


class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units: int = 1, is_data_parallel: bool = False, is_structure_parallel: bool = False,
                 num_of_families: int = 0):
        # these parameters define parallelism level: data + structure parallelism
        self._is_data_parallel = is_data_parallel
        self._is_structure_parallel = is_structure_parallel

        # define relevant parameters to chosen parallelism level:
        self._execution_units = execution_units
        self.num_of_families = num_of_families
        self.data_formatter = None
        self.event_stream = None
        self._source_eval_mechanism = None

    def get_execution_units(self):
        return self._execution_units

    def get_is_data_parallel(self):
        return self._is_data_parallel

    def get_is_structure_parallel(self):
        return self._is_structure_parallel

    def set_source_eval_mechanism(self, eval_mechanism: EvaluationMechanism):
        self._source_eval_mechanism = eval_mechanism

    def get_source_eval_mechanism(self):
        return self._source_eval_mechanism

    def set_events(self, events):
        self.event_stream = events

    def set_data_formatter(self, data_formatter: DataFormatter):
        self.data_formatter = data_formatter

    def get_data_stream_and_destinations(self):
        """
        This function should supply the stream events and destination mechanisms for processing
        """
        raise NotImplementedError()

    def split_structure(self, eval_params: EvaluationMechanismParameters):
        """
        Parallelism level: multiple structure + single data
        """
        raise NotImplementedError()

    def duplicate_structure(self, eval_params: EvaluationMechanismParameters):
        """
        Parallelism level: single structure + multiple data
        """
        raise NotImplementedError()

    def split_structure_to_families(self, eval_params: EvaluationMechanismParameters):
        """
        Parallelism level: multiple structure + multiple data
        """
        raise NotImplementedError()

    def wait_masters_to_finish(self):
        """
        The assumption here is that when masters finish, final results are available.
        """
        raise NotImplementedError()




