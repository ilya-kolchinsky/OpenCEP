from abc import ABC

from stream.Stream import OutputStream


class ParallelExecutionFramework(ABC):
    def __init__(self, evaluation_mechanism, data_formatter):
        self.evaluation_mechanism = evaluation_mechanism
        self.data_formatter = data_formatter
        self.pattern_matches = OutputStream()

    def activate(self):
        raise NotImplementedError

    def process_event(self, event):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def wait_till_finish(self):
        raise NotImplementedError()

    def get_pattern_matches(self):
        return self.pattern_matches


