from typing import List
from base import Pattern
from evaluation.EvaluationMechanismFactory import (
    EvaluationMechanismParameters,
    EvaluationMechanismTypes,
    EvaluationMechanismFactory,
)
from parallerization import InputParallelParameters

class EvaluationMechanismConfiguration:

    def __init__(self, pattern: Pattern, parallel_params: InputParallelParameters, eval_mechanism_type: EvaluationMechanismTypes,
                 eval_mechanism_params: EvaluationMechanismParameters):

        self.parallel_params = parallel_params
        self.eval_mechanism_type = eval_mechanism_type
        self.eval_mechanism_params = eval_mechanism_params
        self.pattern = pattern#TODO add support to multi patterns


    def input_data_check(self):
        raise NotImplementedError()


