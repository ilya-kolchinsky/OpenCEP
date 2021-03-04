from enum import Enum


class TreeEvaluationMechanismTypes(Enum):
    """
    The currently supported CEP tree based evaluation mechanisms.
    """
    TRIVIAL_TREE_EVALUATION = 0,
    SIMULTANEOUS_TREE_EVALUATION = 1,