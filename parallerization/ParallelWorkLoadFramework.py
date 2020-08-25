from abc import ABC
from misc.IOUtils import Stream


class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units : int = 1, is_data_splitted : bool = False):
        self.execution_units = execution_units
        self.is_data_splitted = is_data_splitted

    def split_data(self, input_stream: Stream):
        raise NotImplementedError()

    def split_structure(self, evaluation_mechanism):
        raise NotImplementedError()