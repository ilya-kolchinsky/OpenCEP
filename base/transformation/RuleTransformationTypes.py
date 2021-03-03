from enum import Enum


class RuleTransformationTypes(Enum):
    """
    Rules of pattern transformation supported by the system.
    """
    AND_AND_PATTERN = 1
    NOT_OR_PATTERN = 2
    NOT_AND_PATTERN = 3
    TOPMOST_OR_PATTERN = 4
    INNER_OR_PATTERN = 5
    NOT_NOT_PATTERN = 6
