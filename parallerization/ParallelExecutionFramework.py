"""
This class contains requirements from the plugin and represents a single evaluation mechanism.
The manager will call this methods in order to achieve parallelism.
"""

from abc import ABC
from stream.Stream import OutputStream, Stream
from evaluation.EvaluationMechanism import EvaluationMechanism
from base.DataFormatter import DataFormatter


class ParallelExecutionFramework(ABC):
    def __init__(self, evaluation_mechanism: EvaluationMechanism, data_formatter: DataFormatter):
        self.evaluation_mechanism = evaluation_mechanism
        self.data_formatter = data_formatter
        self.pattern_matches = OutputStream()

    def activate(self):
        """
        Activate server, thread or any kind of evaluation mechanism.
        """
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def process_event(self, event_stream: Stream):
        """
        This method will be called on any processed event
        """
        raise NotImplementedError()

    def wait_until_finish(self):
        raise NotImplementedError()

    def get_pattern_matches(self):
        """
        Get results
        """
        return self.pattern_matches

    def get_evaluation_mechanism(self):
        return self.evaluation_mechanism


