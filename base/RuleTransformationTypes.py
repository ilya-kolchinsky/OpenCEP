from enum import Enum


class RuleTransformationTypes(Enum):
    """
    Rules of pattern transformation supported by the system.
    """
    AND_AND_PATTERN = 1
    SEQ_OR_PATTERN = 2
    SEQ_NOT_AND_PATTERN = 3
