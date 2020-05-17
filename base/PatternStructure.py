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

   # def remove_arg(self, pattern_struct: ABC):
   #     raise NotImplementedError()

    def duplicate(self):
        raise NotImplementedError()

    def copy(self):
        raise NotImplementedError()

class QItem(PatternStructure):
    def __init__(self, event_type: str, name: str):
        #print("event:", event_type)
        #print("name:", name)
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

class OrOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):#EVA_17.05
        return self.args

    def remove_arg(self, pattern: PatternStructure):
        self.args.remove(pattern)

    def duplicate(self):
        ret = OrOperator(list(self.args))
        #ret.args = list(self.args)
        return ret

class SeqOperator(PatternStructure):
    def __init__(self, args: List[PatternStructure]):
        #for pattern in args:
            #print(pattern)
            #if isinstance(pattern, NegationOperator):
                #args.remove(pattern)
        #args = [arg for arg in args if not isinstance(arg, NegationOperator)]
        self.args = args
        """print(" ")
        for pattern in args:
            print(pattern.name)"""
    def get_args(self):#EVA_17.05
        return self.args

    def remove_arg(self, pattern: PatternStructure):
        self.args.remove(pattern)

    def duplicate(self):
        ret = SeqOperator(list(self.args))
        #ret.args = list(self.args)
        return ret

class KleeneClosureOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg


class NegationOperator(PatternStructure):
    def __init__(self, arg: PatternStructure):
        self.arg = arg

