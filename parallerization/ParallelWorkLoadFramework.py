from abc import ABC
from stream.Stream import Stream
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from evaluation.EvaluationMechanism import EvaluationMechanism
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters, EvaluationMechanismTypes


class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units: int = 1, is_data_splitted: bool = False, is_tree_splitted: bool = False,
                 num_of_data: int = 1):
        self._execution_units = execution_units
        self._is_data_splitted = is_data_splitted
        self._is_tree_splitted = is_tree_splitted
        self._num_of_data_parts = num_of_data
        self._source_eval_mechanism = None

    def get_execution_units(self):
        return self._execution_units

    def get_is_data_splitted(self):
        return self._is_data_splitted

    def get_is_tree_splitted(self):
        return self._is_tree_splitted

    def get_num_of_data(self):
        return self._num_of_data_parts

    def set_source_eval_mechanism(self, eval_mechanism: EvaluationMechanism):
        self._source_eval_mechanism = eval_mechanism

    def get_source_eval_mechanism(self):
        return self._source_eval_mechanism

    # example:
    # map ={1,2}
    # evaluation_mechanism_list[em1,em2,em3]
    # spllited_data = [d1,d2]
    # result would be: em1.eval(d1), em2.eval(d2), em3.eval(d2)
    def get_multiple_data_to_multiple_execution_units_index(self):
        raise NotImplementedError()

    def get_masters(self):
        if not self.get_is_tree_splitted():
            return [self.split_structure(self.get_source_eval_mechanism())]
        else:
            raise NotImplementedError()

    def split_data(self, input_stream: Stream, eval_mechanism: EvaluationMechanism,
                   eval_mechanism_type: EvaluationMechanismTypes, eval_params: EvaluationMechanismParameters):
        #the output needs to be a list of streams of size <= execution_units
        raise NotImplementedError()

    # the output needs to be a list of evaluation mechanisms that implements ParallelExecutionFramework
    def split_structure(self, evaluation_mechanism: EvaluationMechanism,
                        eval_mechanism_type: EvaluationMechanismTypes = None,
                        eval_params: EvaluationMechanismParameters = None):
        self.set_source_eval_mechanism(evaluation_mechanism)
        return [ParallelExecutionFramework(evaluation_mechanism)]




