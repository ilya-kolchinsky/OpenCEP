from typing import List

from misc import DefaultConfig


class PatternPreprocessingParameters:
    """
    Parameters for preprocessing CEP patterns.
    As of now, the only required pattern is the selection/order of transformation rules to be applied.
    """
    def __init__(self, transformation_rules: List = DefaultConfig.PREPROCESSING_RULES_ORDER):
        self.transformation_rules = transformation_rules
