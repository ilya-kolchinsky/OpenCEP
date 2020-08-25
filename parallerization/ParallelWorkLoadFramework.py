from abc import ABC


class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units, is_data_splitted = False):
        self.execution_units = execution_units
        self.is_data_splitted = is_data_splitted

    def split_data(self, inputStream):
        raise NotImplementedError()

    def split_structure(self, evalutaion_mechanizm):
        raise NotImplementedError()