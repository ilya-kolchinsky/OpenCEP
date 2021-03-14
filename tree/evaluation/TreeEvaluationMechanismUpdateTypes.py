from enum import Enum


class TreeEvaluationMechanismUpdateTypes(Enum):
    """
    The currently supported mechanisms for replacing a CEP evaluation tree on-the-fly.
    """

    # drops the old tree immediately and replays events from the start of the current window on the new tree
    TRIVIAL_TREE_EVALUATION = 0,

    # maintains both the old and the new tree simultaneously until the current window expires
    SIMULTANEOUS_TREE_EVALUATION = 1,
