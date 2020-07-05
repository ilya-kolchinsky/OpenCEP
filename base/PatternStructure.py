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

    def create_top_operator(self):
        if self.get_top_operator() == SeqOperator:
            return SeqOperator([])
        elif self.get_top_operator() == AndOperator:
            return AndOperator([])
        elif self.get_top_operator() == OrOperator:
            return OrOperator([])

class QItem(PatternStructure):

    def __init__(self, event_type: str, name: str, index: int = None):
        self.event_type = event_type
        self.name = name
        self.index = index

    def set_qitem_index(self, index: int):
        self.index = index

    def get_event_name(self):
        return self.name

    def get_event_index(self):
        return self.index

class AndOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def duplicate(self):
        ret = AndOperator(list(self.args))
        return ret

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

    def duplicate(self):
        ret = OrOperator(list(self.args))
        return ret

class SeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):
        return self.args

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

        #To support composite patterns, change this to a list of all events_name and their type
        self.name = self.get_event_name()
        self.event_type = self.get_event_type()

    def get_args(self):
        return self.arg

    def get_event_name(self):
        if type(self.arg) == QItem:
            return self.get_args().name
        #To support composite patterns
        elif type(self.arg) == SeqOperator:
            return self.get_args().name

    def get_event_type(self):
        if type(self.arg) == QItem:
            return self.get_args().event_type
        # To support composite patterns
        elif type(self.arg) == SeqOperator:
            return self.get_args().event_type

    def get_event_index(self):
        if type(self.arg) == QItem:
            return self.get_args().get_event_index()


    def set_qitem_index(self, index: int):
        if type(self.arg) == QItem:
            return self.arg.set_qitem_index(index)
