from enum import Enum


class MultiPatternEvaluationApproaches(Enum):
    """
    The various approaches for constructing a multi-pattern evaluation mechanism.
    TRIVIAL: gets a list of patterns and builds a separate tree for each pattern, while sharing equivalent leaves
    from different patterns.
    SUBTREES_UNION: gets a list of patterns and builds a unified tree by sharing equivalent subtrees of different
    patterns.
    """
    TRIVIAL_SHARING_LEAVES = 0,
    SUBTREES_UNION = 1
