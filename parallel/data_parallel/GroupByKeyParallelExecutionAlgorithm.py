from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from base.PatternMatch import *
from parallel.platform.ParallelExecutionPlatform import ParallelExecutionPlatform
from misc.Utils import is_int, is_float
from typing import Set


class GroupByKeyParallelExecutionAlgorithm(DataParallelExecutionAlgorithm):
    """
    Implements the key-based partitioning algorithm.
    Gets a key and a unit-number - assigns each event to an execution unit based on the modulo value of the provided key

    event[key](%units_number) --> designated unit

    All patterns must include == comparison between all attributes with the given "key" argument,
    to enforce matches on the same unit id.
    """

    def __init__(self,
                 units_number,
                 patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters,
                 platform: ParallelExecutionPlatform,
                 key: str):
        super().__init__(units_number, patterns, eval_mechanism_params, platform)
        self._key = key

 #   def _classifier(self, event: Event) -> Set[int]:
 #       """
 #       return list of a single unit that matches the modulo of the key
 #       """
 #       value = event.payload.get(self._key)
 #       if value is None:
 #           #raise Exception(f"attribute {self._key} is not existing in type {event.type}")
 #       elif not is_int(value) and not is_float(value):
#            # raise Exception(f"Non numeric key {self._key} = {value}")
#        return {int(value) % self.units_number}


    def _classifier(self, event: Event) -> Set[int]:
        """
        return list of a single unit that matches the modulo of the key
        """
        value = event.payload.get(self._key)
        if value is not None and (is_int(value) or is_float(value)):
            return {int(value) % self.units_number}


    def _create_skip_item(self, unit_id: int):
        """
        Creates and returns FilterStream object.
        """

        def skip_item(item: PatternMatch):
            return False

        return skip_item

