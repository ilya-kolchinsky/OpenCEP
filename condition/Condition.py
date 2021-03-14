"""
This file contains the basic Condition classes.
"""
from abc import ABC, abstractmethod
from enum import Enum
from adaptive.statistics.StatisticsTypes import StatisticsTypes
from adaptive.statistics.StatisticsCollector import StatisticsCollector


class RelopTypes(Enum):
    """
    The various relation operations for a binary condition.
    """
    Equal = 0,
    NotEqual = 1,
    Greater = 2,
    GreaterEqual = 3,
    Smaller = 4,
    SmallerEqual = 5

    @staticmethod
    def get_opposite_relop_type(relop_type):
        """
        Returns the relation operation type which is antisymmetric to the given one.
        """
        if relop_type == RelopTypes.Greater:
            return RelopTypes.Smaller
        if relop_type == RelopTypes.Smaller:
            return RelopTypes.Greater
        if relop_type == RelopTypes.GreaterEqual:
            return RelopTypes.SmallerEqual
        if relop_type == RelopTypes.SmallerEqual:
            return RelopTypes.GreaterEqual
        return None


class EquationSides(Enum):
    left = 0,
    right = 1


class Variable:
    """
    This class represents a variable in an event-related condition.
    Typically, it will be of the form "x.y" where "X" corresponds to a known event name and y is an attribute available
    for events of x's type.
    """
    def __init__(self, name: str, getattr_func: callable):
        self.name = name
        # this callback function is used to fetch the attribute value from an event payload dict
        self.getattr_func = getattr_func

    def eval(self, binding: dict = None):
        """
        Retrieves the value corresponding to the event name / attribute combination describing this variable from
        the given dict and returns it.
        """
        if not type(binding) == dict or self.name not in binding:
            raise NameError("Name %s is not bound to a value" % self.name)
        return self.getattr_func(binding[self.name])

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return type(other) == Variable and self.name == other.name and \
               (self.getattr_func == other.getattr_func or
                self.getattr_func.__code__.co_code == other.getattr_func.__code__.co_code)


class Condition(ABC):
    """
    The base abstract class of the condition classes hierarchy.
    """
    def eval(self, binding: dict or list = None):
        """
        Returns True if the provided binding satisfies this condition and False otherwise.
        Variables in the condition are bound to values according to the predefined binding scheme.
        """
        raise NotImplementedError()

    def extract_atomic_conditions(self):
        """
        Returns all atomic conditions comprising this condition.
        """
        raise NotImplementedError()

    def get_event_names(self):
        """
        Returns the event names associated with this condition.
        """
        return set()


class AtomicCondition(Condition, ABC):
    """
    Represents an atomic (non-composite) condition.
    An atomic condition may contain a statistic collector object that collects its evaluation history to estimate
    the condition selectivity.
    """
    def __init__(self):
        # currently used to update the selectivity statistics if they are present in the statistics collector
        self._statistics_collector = None

    def eval(self, binding: dict or list = None):
        result = self._eval(binding)
        # updates the selectivity statistics based on the evaluated atomic condition result
        data = (self, result)
        if self._statistics_collector is not None:
            self._statistics_collector.update_statistics_by_type(StatisticsTypes.SELECTIVITY_MATRIX, data)
        return result

    @abstractmethod
    def _eval(self, binding):
        """
        An abstract method for the actual eval.
        """
        raise NotImplementedError()

    def is_condition_of(self, names: set):
        """
        Returns True if all variable names participating in this condition appear in the given set and False otherwise.
        """
        raise NotImplementedError()

    def extract_atomic_conditions(self):
        return [self]

    def set_statistics_collector(self, statistics_collector: StatisticsCollector):
        """
        Sets the statistic collector object for registering successful and failed condition evaluations.
        """
        self._statistics_collector = statistics_collector


class TrueCondition(AtomicCondition):
    """
    Represents a Boolean True condition.
    """
    def _eval(self, binding: dict = None):
        return True

    def __repr__(self):
        return "True Condition"

    def extract_atomic_conditions(self):
        return []

    def is_condition_of(self, names: set):
        return False

    def __eq__(self, other):
        return type(other) == TrueCondition


class SimpleCondition(AtomicCondition):
    """
    A simple (non-composite) condition over N operands (either variables or constants).
    """
    def __init__(self, *terms, relation_op: callable):
        super().__init__()
        self.terms = terms
        self.relation_op = relation_op

    def _eval(self, binding: dict = None):
        rel_terms = []
        for term in self.terms:
            rel_terms.append(term.eval(binding) if isinstance(term, Variable) else term)
        return self.relation_op(*rel_terms)

    def is_condition_of(self, names: set):
        for term in self.terms:
            if term.name not in names:
                return False
        return True

    def __repr__(self):
        term_list = []
        for term in self.terms:
            term_list.append(term.__repr__())
        separator = ', '
        return "[" + separator.join(term_list) + "]"

    def __eq__(self, other):
        return self == other or type(self) == type(other) and self.terms == other.terms and self.relation_op == other.relation_op

    def get_event_names(self):
        """
        Returns the event names associated with this condition.
        """
        return set(term.name for term in self.terms)


class BinaryCondition(SimpleCondition):
    """
    A binary condition containing no logic operators (e.g., A < B).
    This is a special case of a simple n-ary condition constrained to two operands.
    """
    def __init__(self, left_term, right_term, relation_op: callable):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of BinaryCondition!")
        elif not isinstance(left_term, Variable):
            super().__init__(right_term, relation_op=relation_op)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, relation_op=relation_op)
        else:
            super().__init__(left_term, right_term, relation_op=relation_op)

    def get_left_term(self):
        """
        Returns the left term of this condition.
        """
        if len(self.terms) < 1:
            return None
        return self.terms[0]

    def get_right_term(self):
        """
        Returns the right term of this condition.
        """
        if len(self.terms) < 2:
            return None
        return self.terms[1]
