from enum import Enum


class PatternTransformationRules(Enum):
    """
    This enum lists the currently supported transformation rules for CEP patterns.
    """
    AND_AND_PATTERN = 1  # flatten nested ANDs
    NOT_OR_PATTERN = 2  # OR-based De-Morgan
    NOT_AND_PATTERN = 3  # AND-based De-Morgan
    TOPMOST_OR_PATTERN = 4  # push a nested OR to the top level
    INNER_OR_PATTERN = 5  # split a disjunction pattern into multiple patterns
    NOT_NOT_PATTERN = 6   # remove duplicate negations
