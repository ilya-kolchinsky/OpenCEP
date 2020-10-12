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
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def process_event(self, event_stream: Stream):
        raise NotImplementedError()

    def wait_until_finish(self):
        raise NotImplementedError()

    def get_pattern_matches(self):
        return self.pattern_matches

    def get_evaluation_mechanism(self):
        return self.evaluation_mechanism


