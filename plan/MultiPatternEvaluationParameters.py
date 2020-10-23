from enum import Enum


class MultiPatternEvaluationApproach(Enum):
    """
    The various approaches for constructing a multi-pattern evaluation mechanism.
    TRIVIAL: gets a list of patterns and builds a separate tree for each pattern,
             while sharing equivalent leaves from different patterns
    SUBTREES_UNION: gets a list of patterns and builds a unified tree, while sharing
                    equivalent subtrees of different patterns
    """

    TRIVIAL_SHARING_LEAVES = 0,
    SUBTREES_UNION = 1


class MultiPatternEvaluationParameters:
    """
    Parameters for multi-pattern evaluation mode
    """

    def __init__(self, multi_pattern_eval_approach: MultiPatternEvaluationApproach = MultiPatternEvaluationApproach.TRIVIAL_SHARING_LEAVES):
        self.approach = multi_pattern_eval_approach

