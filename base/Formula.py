from abc import ABC  # Abstract Base Class
import copy
from enum import Enum
from typing import List
import traceback


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


class IdentifierTerm:
    """
    A term of a formula representing a single variable (e.g., in "x*2 < y + 7" the identifier terms are x and y).
    """
    def __init__(self, name: str, getattr_func: callable):
        self.name = name
        self.getattr_func = getattr_func

    def eval(self, binding: dict = None):
        if not type(binding) == dict or self.name not in binding:
            raise NameError("Name %s is not bound to a value" % self.name)
        return self.getattr_func(binding[self.name])

    def get_term_of(self, names: set):
        if self.name in names:
            return self
        return None

    def __repr__(self):
        return self.name


class Formula(ABC):
    """
    Returns whether the parameters satisfy the formula. It evaluates to True or False.
    If there are variables (identifiers) in the formula, a name-value binding shall be inputted.
    """
    def eval(self, binding: dict or list = None):
        raise NotImplementedError()

    def get_formula_of(self, names: set, ignore_kc=True):
        raise NotImplementedError()

    def extract_atomic_formulas(self):
        raise NotImplementedError()


class TrueFormula(Formula):
    def eval(self, binding: dict = None):
        return True

    def __repr__(self):
        return "True Formula"

    def extract_atomic_formulas(self):
        return []

    def get_formula_of(self, names, ignore_kc=True):
        # return nothing to KC nodes
        if ignore_kc is False:
            return None
        return self

    def extract_atomic_formulas_new(self):
        return {}

    def consume_formula_of(self, names: set, ignore_kc=True):
        raise NotImplementedError()


class NaryFormula(Formula):
    def __init__(self, *terms, relation_op: callable):
        self._terms = terms
        self.relation_op = relation_op

    def eval(self, binding: dict = None):
        rel_terms = []
        for term in self._terms:
            rel_terms.append(term.eval(binding))
        rel_terms = tuple(rel_terms)
        return self.relation_op(*rel_terms)


    def get_formula_of(self, names: set, ignore_kc=True):
        # return nothing to KC nodes
        if ignore_kc is False:
            return None
        for term in self._terms:
            cur_term = term.get_term_of(names)
            if cur_term is None:
                return None
        return self

    def extract_atomic_formulas(self):
        return [self]


class BinaryFormula(NaryFormula):
    """
    An binary formula containing no logic operators (e.g., A < B).
    """
    def __init__(self, left_term, right_term, relation_op: callable):
        if not isinstance(left_term, IdentifierTerm) and not isinstance(right_term, IdentifierTerm):
            raise Exception("Invalid use of BinaryFormula!")
        elif not isinstance(left_term, IdentifierTerm):
            super().__init__(right_term, relation_op=relation_op)
        elif not isinstance(right_term, IdentifierTerm):
            super().__init__(left_term, relation_op=relation_op)
        else:
            super().__init__(left_term, right_term, relation_op=relation_op)

    def get_left_term(self):
        if len(self._terms) < 1:
            return None
        return self._terms[0]

    def get_right_term(self):
        if len(self._terms) < 2:
            return None
        return self._terms[1]

    def extract_atomic_formulas(self):
        return [self]


class BaseRelationFormula(BinaryFormula, ABC):
    def __init__(self, left_term, right_term, relation_op: callable, relop_type):
        if not isinstance(left_term, IdentifierTerm) and not isinstance(right_term, IdentifierTerm):
            raise Exception("Invalid use of BaseRelationFormula!")
        elif not isinstance(left_term, IdentifierTerm):
            super().__init__(left_term, right_term, relation_op=relation_op(left_term))
        elif not isinstance(right_term, IdentifierTerm):
            super().__init__(left_term, right_term, relation_op=relation_op(right_term))
        else:
            super().__init__(left_term, right_term, relation_op)
        self.relop_type = relop_type

    def get_relop(self):
        return self.relop_type

    def __repr__(self):
        raise NotImplementedError()


class EqFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, IdentifierTerm) and not isinstance(right_term, IdentifierTerm):
            raise Exception("Invalid use of EqFormula!")
        elif not isinstance(left_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: x == y, RelopTypes.Equal)
        elif not isinstance(right_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: y == x, RelopTypes.Equal)
        else:
            super().__init__(left_term, right_term, lambda x, y: x == y, RelopTypes.Equal)

    def __repr__(self):
        return "{} == {}".format(self.get_left_term(), self.get_right_term())


class NotEqFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, IdentifierTerm) and not isinstance(right_term, IdentifierTerm):
            raise Exception("Invalid use of NotEqFormula!")
        elif not isinstance(left_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: x != y, RelopTypes.NotEqual)
        elif not isinstance(right_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: y != x, RelopTypes.NotEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x != y, RelopTypes.NotEqual)

    def __repr__(self):
        return "{} != {}".format(self.get_left_term(), self.get_right_term())


class GreaterThanFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, IdentifierTerm) and not isinstance(right_term, IdentifierTerm):
            raise Exception("Invalid use of GreaterThanFormula!")
        elif not isinstance(left_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: x > y, RelopTypes.Greater)
        elif not isinstance(right_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: y > x, RelopTypes.Greater)
        else:
            super().__init__(left_term, right_term, lambda x, y: x > y, RelopTypes.Greater)

    def __repr__(self):
        return "{} > {}".format(self.get_left_term(), self.get_right_term())


class SmallerThanFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, IdentifierTerm) and not isinstance(right_term, IdentifierTerm):
            raise Exception("Invalid use of SmallerThanFormula!")
        elif not isinstance(left_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: x < y, RelopTypes.Smaller)
        elif not isinstance(right_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: y < x, RelopTypes.Smaller)
        else:
            super().__init__(left_term, right_term, lambda x, y: x < y, RelopTypes.Smaller)

    def __repr__(self):
        return "{} < {}".format(self.get_left_term(), self.get_right_term())


class GreaterThanEqFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, IdentifierTerm) and not isinstance(right_term, IdentifierTerm):
            raise Exception("Invalid use of GreaterThanEqFormula!")
        elif not isinstance(left_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: x >= y, RelopTypes.GreaterEqual)
        elif not isinstance(right_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: y >= x, RelopTypes.GreaterEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x >= y, RelopTypes.GreaterEqual)

    def __repr__(self):
        return "{} >= {}".format(self.get_left_term(), self.get_right_term())


class SmallerThanEqFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, IdentifierTerm) and not isinstance(right_term, IdentifierTerm):
            raise Exception("Invalid use of SmallerThanEqFormula!")
        elif not isinstance(left_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: x <= y, RelopTypes.SmallerEqual)
        elif not isinstance(right_term, IdentifierTerm):
            super().__init__(left_term, right_term, lambda x: lambda y: y <= x, RelopTypes.SmallerEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x <= y, RelopTypes.SmallerEqual)

    def __repr__(self):
        return "{} <= {}".format(self.get_left_term(), self.get_right_term())


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

    def get_formula_of(self, names: set, ignore_kc=True):
        result_formulas = []
        for f in self.__formulas:
            current_formula = f.get_formula_of(names, ignore_kc)
            if current_formula and ignore_kc != isinstance(current_formula, KCFormula):
                result_formulas.extend([current_formula])
        return CompositeFormula(result_formulas, self.__terminating_result)

    def consume_formula_of(self, names: set, ignore_kc=True):
        formulas_to_remove = []
        # find what formulas are needed to be removed from the formulas list
        for i, f in enumerate(self.__formulas):
            current_formula = f.get_formula_of(names, ignore_kc)
            if current_formula:
                # consume nested formulas
                if isinstance(f, CompositeFormula):
                    f.consume_formula_of(names, ignore_kc)
                # mark this formula only if it matches ignore_kc logic
                if ignore_kc != isinstance(current_formula, KCFormula):
                    formulas_to_remove.append(i)
        # remove the formulas that match given names
        for i in reversed(formulas_to_remove):
            # regular (Nary/Binary/KC/etc...) formula found
            if not isinstance(self.__formulas[i], CompositeFormula):
                self.__formulas.pop(i)
            else:
                # CompositeFormula found
                nested_formula = self.__formulas[i]
                if nested_formula.get_num_formulas() == 0:
                    self.__formulas.pop(i)

    def get_num_formulas(self):
        return len(self.__formulas)

    def get_formulas_list(self):
        return self.__formulas

    def extract_atomic_formulas(self):
        result = []
        for f in self.__formulas:
            result.extend(f.extract_atomic_formulas())
        return result


class AndFormula(CompositeFormula):
    """
    This class uses CompositeFormula with the terminating condition False, which complies with AND operator logic.
    """
    def __init__(self, formula_list: List[Formula]):
        super().__init__(formula_list, False)

    def get_formula_of(self, names: set, ignore_kc=True):
        comp_formula = super().get_formula_of(names, ignore_kc)
        # result_formulas = [formula for formula in result_formulas if CompositeFormula.filter_formula(formula, get_KC)]
        # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        if comp_formula:
            return AndFormula(comp_formula.get_formulas_list())
        else:
            return None


class OrFormula(CompositeFormula):
    """
    This class uses CompositeFormula with the terminating condition True, which complies with OR operator logic.
    """
    def __init__(self, formula_list: List[Formula]):
        super().__init__(formula_list, True)
        raise NotImplementedError()

    def get_formula_of(self, names: set, ignore_kc=True):
        raise NotImplementedError()
        # # an example of OR logic implementation
        # comp_formula = super().get_formula_of(names, ignore_kc)
        # # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        # if result_formulas:
        #     return OrFormula(comp_formula.get_formulas_list())
        # else:
        #     return None


class KCFormula(Formula, ABC):
    def __init__(self, names, getattr_func, relation_op):
        self._names = names
        self._getattr_func = getattr_func
        self._relation_op = relation_op

    def get_formula_of(self, names: set, ignore_kc=False):
        if ignore_kc == isinstance(self, KCFormula):
            return None
        if names == self._names:
            return self
        if len(names) != len(self._names):
            return None
        for name in names:
            if not any([name in local_name for local_name in self._names]):
                return None
        return self

    @staticmethod
    def validate_index(index, iterable: list):
        if index < 0 or index >= len(iterable):
            return False
        return True

    def eval(self, iterable: list = None):
        raise NotImplementedError()

    def extract_atomic_formulas(self):
        return [self]


class KCIndexFormula(KCFormula):
    def __init__(self, names, getattr_func, relation_op, index_1=None, index_2=None, offset=None):
        # enforce getting 1 of 2 options:
        # 1) index_1 and index_2 to compare
        # 2) offset to compare every 2 items that meet offset requirement (either positive or negative)
        if not self.__validate_params(index_1, index_2, offset):
            raise Exception("Invalid use of KCIndex formula.\nboth index and offset are not None\n refer to comment")
        super().__init__(names, getattr_func, relation_op)
        self._index_1 = index_1
        self._index_2 = index_2
        self._offset = offset

    def eval(self, iterable: list or dict = None):
        if self._offset is not None:
            return self.eval_by_offset(iterable)
        else:
            return self.eval_by_index(iterable)

    def eval_by_index(self, iterable: list):
        if not self.validate_index(self._index_1, iterable) or not self.validate_index(self._index_2, iterable):
            return False
        item_1 = iterable[self._index_1]
        item_2 = iterable[self._index_2]
        if not self._relation_op(self._getattr_func(item_1), self._getattr_func(item_2)):
            return False
        return True

    # very time consuming process on large power-sets
    def eval_by_offset(self, iterable):
        if self._offset >= len(iterable):
            return False
        for i in range(len(iterable)):
            if not self.validate_index(i + self._offset, iterable):
                continue
            if not self._relation_op(self._getattr_func(iterable[i]),
                                     self._getattr_func(iterable[i + self._offset])):
                return False
        return True

    @staticmethod
    def __validate_params(index_1, index_2, offset):
        return not (                                                                     # idx1 idx2 offset
                (index_1 is None and index_2 is None and offset is None) or              # 0     0     0
                (index_1 is not None and index_2 is not None and offset is not None) or  # 1     1     1
                (index_1 is not None and offset is not None) or                          # 1     0     1
                (index_2 is not None and offset is not None) or                          # 0     1     1
                (index_1 is None and index_2 is not None) or                             # 1     0     0
                (index_1 is not None and index_2 is None)                                # 0     1     0
        )


class KCValueFormula(KCFormula):
    def __init__(self, names, getattr_func, relation_op, value, index=None):
        super().__init__(names, getattr_func, relation_op)
        self._value = value
        self._index = index

    def eval(self, iterable: list or dict = None):
        if self._index is not None and not self.validate_index(self._index, iterable):
            return False
        if self._index is None:
            for item in iterable:
                if not self._relation_op(self._getattr_func(item), self._value):
                    return False
        else:
            if not self._relation_op(self._getattr_func(iterable[self._index]), self._value):
                return False
        return True

