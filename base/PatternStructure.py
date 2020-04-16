"""
These classes are used to represent a "parameter" to the PATTERN clause.
These classes represent a SEQ/AND/OR/KL operator and the atomic argument QItem.
The classes support nesting. Every operator class has its list of arguments.
The QItem class has an event type and its name. The name is referred to in
a pattern matching condition, represented as formula.
"""

from abc import ABC
from typing import List


class PatternStructure(ABC):
    def get_top_operator(self):
        return type(self)


class QItem(PatternStructure):
    def __init__(self, event_type: str, name: str):
        self.event_type = event_type
        self.name = name

    def __repr__(self):
        return "{} of type {}".format(self.name, self.event_type)


class AndOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def __repr__(self):
        return "AND({})".format(self.args)


class OrOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def __repr__(self):
        return "OR({})".format(self.args)


class SeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def __repr__(self):
        return "SEQ({})".format(self.args)


class KleeneClosureOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg


class NegationOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg
