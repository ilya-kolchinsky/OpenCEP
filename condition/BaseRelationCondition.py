"""
This file contains the basic relation condition classes.
"""
from abc import ABC

from condition.Condition import BinaryCondition, Variable, RelopTypes


class BaseRelationCondition(BinaryCondition, ABC):
    """
    This class serves as a base for commonly used binary relations: >, >=, <, <=, ==, !=.
    """
    def __init__(self, left_term, right_term, relation_op: callable, relop_type):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of BaseRelationCondition!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, relation_op=relation_op(left_term))
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, relation_op=relation_op(right_term))
        else:
            super().__init__(left_term, right_term, relation_op)
        self.relop_type = relop_type
        self.left_term_repr = left_term
        self.right_term_repr = right_term

    def __repr__(self):
        raise NotImplementedError()

    def __eq_same_type(self, other):
        """
        Returns True if self and other are of the same basic relation types and represent the same condition.
        """
        return isinstance(other, BaseRelationCondition) and self.relop_type == other.relop_type \
            and self.left_term_repr == other.left_term_repr and self.right_term_repr == other.right_term_repr

    def __eq_opposite_type(self, other):
        """
        Returns True if self and other are of the opposite basic relation types and represent the same condition
        (e.g., a < b and b > a).
        """
        if not isinstance(other, BaseRelationCondition):
            return False
        opposite_type = RelopTypes.get_opposite_relop_type(self.relop_type)
        if opposite_type is None:
            return False
        return other.relop_type == opposite_type and \
            self.left_term_repr == other.right_term_repr and self.right_term_repr == other.left_term_repr

    def __eq__(self, other):
        return id(self) == id(other) or self.__eq_same_type(other) or self.__eq_opposite_type(other)


class EqCondition(BaseRelationCondition):
    """
    Binary Equal Condition; ==
    This class can be called either with terms or a number:
    Examples:
        EqCondition(Variable("a", lambda x: x["Opening Price"]), 135)
        EqCondition(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of EqCondition!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x == y, RelopTypes.Equal)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y == x, RelopTypes.Equal)
        else:
            super().__init__(left_term, right_term, lambda x, y: x == y, RelopTypes.Equal)

    def __repr__(self):
        return "{} == {}".format(self.left_term_repr, self.right_term_repr)


class NotEqCondition(BaseRelationCondition):
    """
    Binary Not Equal Condition; !=
    Examples:
        NotEqCondition(Variable("a", lambda x: x["Opening Price"]), 135)
        NotEqCondition(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of NotEqCondition!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x != y, RelopTypes.NotEqual)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y != x, RelopTypes.NotEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x != y, RelopTypes.NotEqual)

    def __repr__(self):
        return "{} != {}".format(self.left_term_repr, self.right_term_repr)


class GreaterThanCondition(BaseRelationCondition):
    """
    Binary greater than condition; >
    Examples:
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 135)
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of GreaterThanCondition!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x > y, RelopTypes.Greater)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y > x, RelopTypes.Greater)
        else:
            super().__init__(left_term, right_term, lambda x, y: x > y, RelopTypes.Greater)

    def __repr__(self):
        return "{} > {}".format(self.left_term_repr, self.right_term_repr)


class SmallerThanCondition(BaseRelationCondition):
    """
    Binary smaller than condition; <
    Examples:
        SmallerThanCondition(Variable("a", lambda x: x["Opening Price"]), 135)
        SmallerThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of SmallerThanCondition!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x < y, RelopTypes.Smaller)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y < x, RelopTypes.Smaller)
        else:
            super().__init__(left_term, right_term, lambda x, y: x < y, RelopTypes.Smaller)

    def __repr__(self):
        return "{} < {}".format(self.left_term_repr, self.right_term_repr)


class GreaterThanEqCondition(BaseRelationCondition):
    """
    Binary greater and equal than condition; >=
    Examples:
        GreaterThanEqCondition(Variable("a", lambda x: x["Opening Price"]), 135)
        GreaterThanEqCondition(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of GreaterThanEqCondition!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x >= y, RelopTypes.GreaterEqual)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y >= x, RelopTypes.GreaterEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x >= y, RelopTypes.GreaterEqual)

    def __repr__(self):
        return "{} >= {}".format(self.left_term_repr, self.right_term_repr)


class SmallerThanEqCondition(BaseRelationCondition):
    """
    Binary smaller and equal than condition; <=
    Examples:
        SmallerThanEqCondition(Variable("a", lambda x: x["Opening Price"]), 135)
        SmallerThanEqCondition(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of SmallerThanEqCondition!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x <= y, RelopTypes.SmallerEqual)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y <= x, RelopTypes.SmallerEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x <= y, RelopTypes.SmallerEqual)

    def __repr__(self):
        return "{} <= {}".format(self.left_term_repr, self.right_term_repr)
