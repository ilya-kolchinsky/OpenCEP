from abc import ABC
from misc.IOUtils import Stream


class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units: int = 1, is_data_splitted: bool = False, is_tree_splitted: bool = False):
        self._execution_units = execution_units
        self._is_data_splitted = is_data_splitted
        self._is_tree_splitted = is_tree_splitted

    def get_execution_units(self):
        return self._execution_units

    def get_is_data_splitted(self):
        return self._is_data_splitted

    # example:
    # map ={1,2}
    # evaluation_mechanism_list[em1,em2,em3]
    # spllited_data = [d1,d2]
    # result would be: em1.eval(d1), em2.eval(d2), em3.eval(d2)
    def get_multiple_data_to_multiple_execution_units_index(self):
        raise NotImplementedError()

    def get_masters(self):
        raise NotImplementedError()

    def split_data(self, input_stream: Stream):#the output needs to be a list of streams of size <= execution_units
        raise NotImplementedError()

    # the output needs to be a list of evalution mechanizms that implements ParallelExecutionFramework
    def split_structure(self, evaluation_mechanism):
        raise NotImplementedError()




