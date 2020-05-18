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

    def duplicate(self):
        raise NotImplementedError()

    def copy(self):
        raise NotImplementedError()

    def create_top_operator(self):
        if self.get_top_operator() == SeqOperator:
            return SeqOperator([])
        elif self.get_top_operator() == AndOperator:
            return AndOperator([])
        elif self.get_top_operator() == OrOperator:
            return OrOperator([])

class QItem(PatternStructure):
    def __init__(self, event_type: str, name: str):
        self.event_type = event_type
        self.name = name


class AndOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def duplicate(self):
        ret = AndOperator(list(self.args))
        #ret.args = list(self.args)
        return ret

    def get_args(self):#EVA_17.05
        return self.args

    def remove_arg(self, pattern: PatternStructure):
        self.args.remove(pattern)

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)

class OrOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):#EVA_17.05
        return self.args

    def remove_arg(self, pattern: PatternStructure):
        self.args.remove(pattern)

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)

    def duplicate(self):
        ret = OrOperator(list(self.args))
        return ret

class SeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

        """
        for pattern in args:
            print(pattern)
            if isinstance(pattern, NegationOperator):
                args.remove(pattern)
        pos_args = [arg for arg in args if not isinstance(arg, NegationOperator)]
        neg_args = [arg for arg in args if isinstance(arg, NegationOperator)]

        neg_args = []
        for arg in args:
            if isinstance(arg, NegationOperator):
                neg_args.append(arg)
                args.remove(arg)
            try:
                neg_args.append(arg.neg_args)
            except AttributeError:
                pass
        """

    def get_args(self):#EVA_17.05
        return self.args

    def remove_arg(self, pattern: PatternStructure):
        self.args.remove(pattern)

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)

    def duplicate(self):
        ret = SeqOperator(list(self.args))
        return ret

class KleeneClosureOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg


class NegationOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg

