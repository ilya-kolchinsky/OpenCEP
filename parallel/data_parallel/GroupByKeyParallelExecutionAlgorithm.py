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
        self._key = key

    def _classifier(self, event: Event):
        """
        return list of a single unit that matches the modulo of the key
        """
        value = event.payload.get(self._key)
        if value is None:
            raise Exception(f"attribute {self._key} is not existing in type {event.type}")
        elif not is_int(value) and not is_float(value):
            raise Exception(f"Non numeric key {self._key} = {value}")
        return [int(value) % self.units_number]



