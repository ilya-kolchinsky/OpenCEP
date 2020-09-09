from abc import ABC  # Abstract Base Class
import copy
from enum import Enum
from typing import List


class RelopTypes(Enum):
    """
    The various RELOPs for a condition in a formula.
    """
    Equal = 0,
    NotEqual = 1,
    Greater = 2,
    GreaterEqual = 3,
    Smaller = 4,
    SmallerEqual = 5


class EquationSides(Enum):
    left = 0,
    right = 1


class Term(ABC):
    """
    Evaluates to the term's value.
    If there are variables (identifiers) in the term, a name-value binding shall be inputted.
    """
    def eval(self, binding: dict = None):
        raise NotImplementedError()

    def get_term_of(self, names: set):
        raise NotImplementedError()


class AtomicTerm(Term):
    """
    An atomic term of a formula in a condition (e.g., in "x*2 < y + 7" the atomic terms are 2 and 7).
    """
    def __init__(self, value: object):
        self.value = value

    def eval(self, binding: dict = None):
        return self.value

    def get_term_of(self, names: set):
        return self

    def __repr__(self):
        return str(self.value)


class IdentifierTerm(Term):
    """
    A term of a formula representing a single variable (e.g., in "x*2 < y + 7" the atomic terms are x and y).
    """
    def __init__(self, name: str, getattr_func: callable):
        self.name = name
        self.getattr_func = getattr_func

    def eval(self, binding: dict = None):
        if not type(binding) == dict or self.name not in binding:
            raise NameError("Name %s is not bound to a value" % self.name)
        return self.getattr_func(binding[self.name])

    def get_term_of(self, names: set) :
        if self.name in names:
            return self
        return None

    def __repr__(self):
        return self.name


class BinaryOperationTerm(Term):
    """
    A term representing a binary operation.
    """
    def __init__(self, lhs: Term, rhs: Term, binary_op: callable):
        self.lhs = lhs
        self.rhs = rhs
        self.binary_op = binary_op

    def eval(self, binding: dict = None):
        return self.binary_op(self.lhs.eval(binding), self.rhs.eval(binding))

    def get_term_of(self, names: set):
        raise NotImplementedError()


class PlusTerm(BinaryOperationTerm):
    def __init__(self, lhs: Term, rhs: Term):
        super().__init__(lhs, rhs, lambda x, y: x + y)

    def get_term_of(self, names: set):
        lhs = self.lhs.get_term_of(names)
        rhs = self.rhs.get_term_of(names)
        if lhs and rhs:
            return PlusTerm(lhs, rhs)
        return None

    def __repr__(self):
        return "{}+{}".format(self.lhs, self.rhs)


class MinusTerm(BinaryOperationTerm):
    def __init__(self, lhs: Term, rhs: Term):
        super().__init__(lhs, rhs, lambda x, y: x - y)

    def get_term_of(self, names: set):
        lhs = self.lhs.get_term_of(names)
        rhs = self.rhs.get_term_of(names)
        if lhs and rhs:
            return MinusTerm(lhs, rhs)
        return None

    def __repr__(self):
        return "{}-{}".format(self.lhs, self.rhs)


class MulTerm(BinaryOperationTerm):
    def __init__(self, lhs: Term, rhs: Term):
        super().__init__(lhs, rhs, lambda x, y: x * y)

    def get_term_of(self, names: set):
        lhs = self.lhs.get_term_of(names)
        rhs = self.rhs.get_term_of(names)
        if lhs and rhs:
            return MulTerm(lhs, rhs)
        return None

    def __repr__(self):
        return "{}*{}".format(self.lhs, self.rhs)


class DivTerm(BinaryOperationTerm):
    def __init__(self, lhs: Term, rhs: Term):
        super().__init__(lhs, rhs, lambda x, y: x / y)

    def get_term_of(self, names: set):
        lhs = self.lhs.get_term_of(names)
        rhs = self.rhs.get_term_of(names)
        if lhs and rhs:
            return DivTerm(lhs, rhs)
        return None

    def __repr__(self):
        return "{}/{}".format(self.lhs, self.rhs)


class Formula(ABC):
    """
    Returns whether the parameters satisfy the formula. It evaluates to True or False.
    If there are variables (identifiers) in the formula, a name-value binding shall be inputted.
    """
    def eval(self, binding: dict = None):
        raise NotImplementedError()

    def get_formula_of(self, names: set):
        raise NotImplementedError()

    def extract_atomic_formulas(self):
        raise NotImplementedError()


class AtomicFormula(Formula, ABC):  # RELOP: < <= > >= == !=
    """
    An atomic formula containing no logic operators (e.g., A < B).
    """
    def __init__(self, left_term: Term, right_term: Term, relation_op: callable):
        self.left_term = left_term
        self.right_term = right_term
        self.relation_op = relation_op
        
    def eval(self, binding: dict = None):
        return self.relation_op(self.left_term.eval(binding), self.right_term.eval(binding))

    def __repr__(self):
        return "{} {} {}".format(self.left_term, self.relation_op, self.right_term)

    def extract_atomic_formulas(self):
        return [self]


class CustomFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term, relation_op: callable):
        super().__init__(left_term, right_term, relation_op)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return self
        return None


class EqFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x == y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return EqFormula(left_term, right_term)
        return None

    def __repr__(self):
        return "{} == {}".format(self.left_term, self.right_term)

    def get_relop(self):
        return RelopTypes.Equal


class NotEqFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x != y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return NotEqFormula(left_term, right_term)
        return None

    def __repr__(self):
        return "{} != {}".format(self.left_term, self.right_term)

    def get_relop(self):
        return RelopTypes.NotEqual


class GreaterThanFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x > y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return GreaterThanFormula(left_term, right_term)
        return None

    def __repr__(self):
        return "{} > {}".format(self.left_term, self.right_term)

    def get_relop(self):
        return RelopTypes.Greater


class SmallerThanFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x < y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return SmallerThanFormula(left_term, right_term)
        return None

    def __repr__(self):
        return "{} < {}".format(self.left_term, self.right_term)

    def get_relop(self):
        return RelopTypes.Smaller


class GreaterThanEqFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x >= y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return GreaterThanEqFormula(left_term, right_term)
        return None

    def __repr__(self):
        return "{} >= {}".format(self.left_term, self.right_term)

    def get_relop(self):
        return RelopTypes.GreaterEqual


class SmallerThanEqFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x <= y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return SmallerThanEqFormula(left_term, right_term)
        return None

    def __repr__(self):
        return "{} <= {}".format(self.left_term, self.right_term)

    def get_relop(self):
        return RelopTypes.SmallerEqual


class BinaryLogicOpFormula(Formula, ABC):  # AND: A < B AND C < D
    """
    A formula composed of a logic operator and two nested formulas.
    """
    def __init__(self, left_formula: Formula, right_formula: Formula, binary_logic_op: callable):
        self.left_formula = left_formula
        self.right_formula = right_formula
        self.binary_logic_op = binary_logic_op

    def eval(self, binding: dict = None):
        return self.binary_logic_op(self.left_formula.eval(binding), self.right_formula.eval(binding))

    def extract_atomic_formulas(self):
        """
        given a BinaryLogicOpFormula returns its atomic formulas as a list, in other
        words, from the formula (f1 or f2 and f3) returns [f1,f2,f3]
        """
        # a preorder path in a tree (taking only leafs (atomic formulas))
        formulas_stack = [self.right_formula, self.left_formula]
        atomic_formulas = []
        while len(formulas_stack) > 0:
            curr_form = formulas_stack.pop()
            if isinstance(curr_form, AtomicFormula):
                atomic_formulas.append(curr_form)
            else:
                formulas_stack.append(curr_form.right_formula)
                formulas_stack.append(curr_form.left_formula)
        return atomic_formulas


class AndFormula(BinaryLogicOpFormula):  # AND: A < B AND C < D
    def __init__(self, left_formula: Formula, right_formula: Formula):
        super().__init__(left_formula, right_formula, lambda x, y: x and y)

    def get_formula_of(self, names: set):
        right_formula = self.right_formula.get_formula_of(names)
        left_formula = self.left_formula.get_formula_of(names)
        if left_formula is not None and right_formula is not None:
            return AndFormula(left_formula, right_formula)
        if left_formula:
            return left_formula
        if right_formula:
            return right_formula
        return None

    def __repr__(self):
        return "{} AND {}".format(self.left_formula, self.right_formula)


class TrueFormula(Formula):
    def eval(self, binding: dict = None):
        return True

    def __repr__(self):
        return "True Formula"

    def extract_atomic_formulas(self):
        return []

    def get_formula_of(self, names):
        return self


class CompositeFormula(Formula, ABC):
    """
    This class represents BinaryLogicOp formulas that support a list of formulas within them.
    This is used to support different syntax for the formula creating, instead of using a full tree of formulas
    it will enable passing a list of formulas (Atomic or Non-Atomic) and apply the encapsulating operator on all of them
    And stops at the first FALSE from the evaluation and returns False.
    Or stops at the first TRUE from the evaluation and return true
    """
    def __init__(self, formula_list: List[Formula], terminating_condition):
        self.__formulas = formula_list
        self.__terminating_result = terminating_condition

    def eval(self, binding: dict = None):
        for formula in self.__formulas:
            if formula.eval(binding) == self.__terminating_result:
                return self.__terminating_result
        return not self.__terminating_result

    def get_formula_of(self, names: set):
        result_formulas = []
        for f in self.__formulas:
            current_formula = f.get_formula_of(names)
            if current_formula:
                result_formulas |= current_formula
        return result_formulas

    def extract_atomic_formulas(self):
        result = []
        for f in self.__formulas:
            result |= f.extract_atomic_formulas()
        return result


class CompositeAnd(CompositeFormula):
    """
    This class uses CompositeFormula with the terminating condition False, which complies with AND operator logic.
    """
    def __init__(self, formula_list: List[Formula]):
        super().__init__(formula_list, False)

    def get_formula_of(self, names: set):
        result_formulas = super().get_formula_of(names)
        # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        if result_formulas:
            return CompositeAnd(result_formulas)
        else:
            return None


class CompositeOr(CompositeFormula):
    """
        This class uses CompositeFormula with the terminating condition True, which complies with OR operator logic.
        """
    def __init__(self, formula_list: List[Formula]):
        super().__init__(formula_list, True)

    def get_formula_of(self, names: set):
        result_formulas = super().get_formula_of(names)
        # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        if result_formulas:
            return CompositeOr(result_formulas)
        else:
            return None
