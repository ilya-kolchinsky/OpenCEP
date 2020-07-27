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

    def get_args(self):
        raise NotImplementedError()

    def add_arg(self, pattern):
        raise NotImplementedError()

    def get_event_name(self):
        raise NotImplementedError()

    def duplicate_top_operator(self):
        if self.get_top_operator() == SeqOperator:
            return SeqOperator([])
        elif self.get_top_operator() == AndOperator:
            return AndOperator([])
        elif self.get_top_operator() == OrOperator:
            return OrOperator([])
        elif self.get_top_operator() == KleeneClosureOperator:
            raise NotImplementedError()
        elif self.get_top_operator() == NegationOperator:
            raise Exception()  # should not happen since we use this on a structure free of negative events
        else:
            raise Exception()  # should not happen


class QItem(PatternStructure):
    def __init__(self, event_type: str, name: str):
        self.event_type = event_type
        self.name = name

    def get_event_name(self):
        return self.name


class AndOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):
        return self.args

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)


class OrOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):
        return self.args

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)


class SeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):
        return self.args

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)


class KleeneClosureOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg

    def get_args(self):
        return self.arg

    def add_arg(self, pattern: PatternStructure):
        raise NotImplementedError()


class NegationOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg

    def get_args(self):
        return self.arg

    def get_event_name(self):
        if type(self.arg) == QItem:
            return self.get_args().get_event_name()
        else:
            raise NotImplementedError()  # should not happen for simple patterns

