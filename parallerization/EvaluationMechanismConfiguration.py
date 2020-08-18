from typing import List
from base import Pattern
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismTypes,
    EvaluationMechanismFactory,
)

class EvaluationMechanismConfiguration:

    def __init__(self, func, distributed: bool, num_of_servers : int, num_of_processes : int, pattern: Pattern,
                 eval_mechanism_type: EvaluationMechanismTypes, eval_mechanism_params: EvaluationMechanismParameters):

        self._splitted_data = distributed
        self._data_split_function = func

        self._distributed = distributed
        self._num_of_servers = num_of_servers
        self._num_of_processes = num_of_processes
        self._eval_mechanism_type = eval_mechanism_type
        self._eval_mechanism_params = eval_mechanism_params
        self._pattern = pattern


    def input_data_check(self):
        raise NotImplementedError()


