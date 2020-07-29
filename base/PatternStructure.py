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
    """
    The top class in the pattern structure hierarchy.
    """

    def get_top_operator(self):
        """
        Returns the operator type at the top of this pattern structure.
        """
        return type(self)

    def duplicate(self):
        """
        Returns a deep copy of this pattern structure.
        """
        raise NotImplementedError()

    def contains_event(self, event_name):
        """
        Returns True if this structure contains an event specified by the given name and False otherwise.
        """
        raise NotImplementedError()


class QItem(PatternStructure):
    """
    Represents a simple primitive event, defined by a type and a name.
    """
    def __init__(self, event_type: str, name: str):
        self.type = event_type
        self.name = name

    def duplicate(self):
        return QItem(self.type, self.name)

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name

    def contains_event(self, event_name):
        return self.name == event_name


class UnaryStructure(PatternStructure, ABC):
    """
    Represents a pattern structure with an unary operator at the top level.
    """
    def __init__(self, arg):
        self.arg = arg

    def __eq__(self, other):
        return type(self) == type(other) and self.arg == other.arg

    def contains_event(self, event_name):
        return self.arg.contains_event(event_name)


class CompositeStructure(PatternStructure, ABC):
    """
    Represents a pattern structure with a multinary operator at the top level.
    """
    def __init__(self, args: List[PatternStructure]):
        self.args = args

    def get_args(self):
        return self.args

    def duplicate(self):
        new_structure = self.duplicate_top_operator()
        new_structure.args = [arg.duplicate() for arg in self.args]
        return new_structure

    def duplicate_top_operator(self):
        raise NotImplementedError()

    def __eq__(self, other):
        if type(self) != type(other) or len(self.args) != len(other.args):
            return False
        for i in range(len(self.args)):
            if self.args[i] != other.args[i]:
                return False
        return True

    def contains_event(self, event_name):
        for arg in self.args:
            if arg.contains_event(event_name):
                return True
        return False


class AndOperator(CompositeStructure):
    def duplicate_top_operator(self):
        return AndOperator([])


class OrOperator(CompositeStructure):
    def duplicate_top_operator(self):
        return OrOperator([])


class SeqOperator(CompositeStructure):
    def duplicate_top_operator(self):
        return SeqOperator([])


class KleeneClosureOperator(UnaryStructure):
    def duplicate(self):
        return KleeneClosureOperator(self.arg.duplicate())


class NegationOperator(UnaryStructure):
    def duplicate(self):
        return NegationOperator(self.arg.duplicate())
