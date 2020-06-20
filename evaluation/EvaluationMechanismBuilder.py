from abc import ABC
from typing import List

from base.Pattern import Pattern
from evaluation.Storage import TreeStorageParameters


class EvaluationMechanismBuilder(ABC):
    """
    A generic class for creating an evaluation mechanism out of the given pattern specifications
    and/or other parameters.
    """

    def build_single_pattern_eval_mechanism(self, pattern: Pattern, storage_params: TreeStorageParameters):
        pass

    def build_multi_pattern_eval_mechanism(self, patterns: List[Pattern]):
        pass
