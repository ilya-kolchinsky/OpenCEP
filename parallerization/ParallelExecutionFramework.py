from abc import ABC

from stream.Stream import OutputStream


class ParallelExecutionFramework(ABC):
    def __init__(self):
        self.pattern_matches = OutputStream()

    def activate(self):
        raise NotImplementedError

    def proccess_event(self, event):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def get_results(self):
        return self.pattern_matches

    def wait_till_finish(self):
        raise NotImplementedError()