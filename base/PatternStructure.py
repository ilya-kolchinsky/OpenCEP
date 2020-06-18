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
    def __init__(self, event_type: str, name: str, strict: bool = False, skip: bool = False):
        self.event_type = event_type
        self.name = name
        self.strict = strict #Second mechanism: requiring an event to immediately follow the previous one in a sequence ("strict" sequence)
        self.skip = skip #Third mechanism: prohibiting any pattern matching while a single partial match is active


class AndOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args


class OrOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args


class SeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args


class StrictSeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args


class KleeneClosureOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg


class NegationOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg
