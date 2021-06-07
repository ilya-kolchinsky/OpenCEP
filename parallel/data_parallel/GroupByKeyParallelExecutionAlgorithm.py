from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.PatternMatch import *
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform
from misc.Utils import is_int, is_float


class GroupByKeyParallelExecutionAlgorithm(DataParallelExecutionAlgorithm):
    """
    Implements the key-based partitioning algorithm.
    """

    def __init__(self,
                 units_number,
                 patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform: ParallelExecutionPlatform,
                 key: str):
        super().__init__(units_number, patterns, eval_mechanism_params, platform)
        self.__key = key

    def _check_first_event(self, first_event: Event):
        value = first_event.payload.get(self.__key)
        if value is None:
            raise Exception('key not exists')
        elif not is_int(value) and not is_float(value):
            raise Exception('Non numeric key')

    def _classifier(self, event: Event):
        """
        return list of a single unit that matches the modulo of the key
        """
        value = event.payload.get(self.__key)
        return [int(value) % self.units_number]



