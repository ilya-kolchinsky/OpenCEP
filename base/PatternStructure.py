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

    def set_qitem_index(self, index: int):
        """
        Set unique index to each qitem, to have events_def in the right order
        Support composite pattern : each QItem_index in suboperator is unique in all the structure
        Implemented by subclasses
        """
        raise NotImplementedError()

class QItem(PatternStructure):
    def __init__(self, event_type: str, name: str, index: int = None):
        self.event_type = event_type
        self.name = name
        self.index = index

    def set_qitem_index(self, index: int):
        self.index = index
        return index + 1

    def get_event_name(self):
        return self.name

    def get_event_index(self):
        return self.index


class AndOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):
        return self.args

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)

    def set_qitem_index(self, index: int):
        for i in range(len(self.args)):
            self.args[i].set_qitem_index(index)
            index = index + 1
        return index

class OrOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):
        return self.args

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)

    def set_qitem_index(self, index: int):
        for i in range(len(self.args)):
            self.args[i].set_qitem_index(index)
            index = index + 1
        return index

class SeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):
        return self.args

    def add_arg(self, pattern: PatternStructure):
        self.args.append(pattern)

    def set_qitem_index(self, index: int):
        for i in range(len(self.args)):
            self.args[i].set_qitem_index(index)
            index = index + 1
        return index

class KleeneClosureOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg

    def get_args(self):
        return self.arg

    def add_arg(self, pattern: PatternStructure):
        raise NotImplementedError()

    def set_qitem_index(self, index: int):
        raise NotImplementedError()

class NegationOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg

        # # To support composite patterns, change this to a list of all events_name and their type
        # self.name = self.get_event_name()
        # self.event_type = self.get_event_type()

    def get_args(self):
        return self.arg

    # def get_event_name(self):
    #     if type(self.arg) == QItem:
    #         return self.get_args().name
    #     # To support composite patterns add for the other operators, implement else
    #     else:
    #         raise NotImplementedError()  # should not happen for simple patterns
    #
    # def get_event_type(self):
    #     if type(self.arg) == QItem:
    #         return self.get_args().event_type
    #     # To support composite patterns add for the other operators
    #     else:
    #         raise NotImplementedError()  # should not happen for simple patterns

    def get_event_index(self):
        if type(self.arg) == QItem:
            return self.get_args().get_event_index()
        else:
            raise NotImplementedError()  # should not happen for simple patterns

    def set_qitem_index(self, index: int):
        # if type(self.arg) == QItem:
        #     return self.arg.set_qitem_index(index)
        # else:
        #     raise NotImplementedError() #should not happen for simple patterns

        return self.arg.set_qitem_index(index)
