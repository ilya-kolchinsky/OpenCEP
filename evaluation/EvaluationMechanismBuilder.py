from abc import ABC
from typing import List

from base.Pattern import Pattern


class EvaluationMechanismBuilder(ABC):
    """
    A generic class for creating an evaluation mechanism out of the given pattern specifications
    and/or other parameters.
    """
    def build_single_pattern_eval_mechanism(self, pattern: Pattern):
        pass

    def build_multi_pattern_eval_mechanism(self, patterns: List[Pattern]):
        pass
