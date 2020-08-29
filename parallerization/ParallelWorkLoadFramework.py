from abc import ABC
from misc.IOUtils import Stream
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, Node



class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units : int = 1, is_data_splitted : bool = False):
        self.execution_units = execution_units
        self.is_data_splitted = is_data_splitted

    def split_data(self, input_stream: Stream):#the output needs to be a list of streams of size <= execution_units
        raise NotImplementedError()

    def split_structure(self, evaluation_mechanism):#the output needs to be a list of nodes/a list with the whole tree when execution_units = 1
        raise NotImplementedError()

    def get_execution_units(self):
        return self.execution_units

    def get_is_data_splitted(self):
        return self.is_data_splitted





