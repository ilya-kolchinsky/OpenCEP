from abc import ABC


class ParallelExecutionFramework(ABC):

    def get_data(self):
        raise NotImplementedError()

    def eval(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()