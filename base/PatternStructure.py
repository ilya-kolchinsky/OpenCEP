"""
These classes are used to represent a "parameter" to the PATTERN clause.
These classes represent a SEQ/AND/OR/KL operator and the atomic argument QItem.
The classes support nesting. Every operator class has its list of arguments.
The QItem class has an event type and its name. The name is referred to in
a pattern matching condition, represented as formula.
"""

from abc import ABC
from typing import List

KC_MIN_SIZE = 1
KC_MAX_SIZE = None


class PatternStructure(ABC):
    def get_top_operator(self):
        return type(self)


class QItem(PatternStructure):
    def __init__(self, event_type: str, name: str):
        self.event_type = event_type
        self.name = name


class AndOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args


class OrOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args


class SeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args


class KleeneClosureOperator(PatternStructure):
    def __init__(self, arg: PatternStructure, min_size=KC_MIN_SIZE, max_size=KC_MAX_SIZE):
        if min_size <= 0:
            raise Exception("Invalid Argument: KleeneClosure node min_size <= 0!")
        # enforce min_size <= max_size
        if max_size is not None and max_size < min_size:
            raise Exception("Invalid Argument: KleeneClosure node max_size < min_size!")
        self.args = [arg]
        self.min_size = min_size
        self.max_size = max_size


class NegationOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg
