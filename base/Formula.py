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


class IdentifierTerm:
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
    def eval(self, binding: dict = None):
        raise NotImplementedError()

    def get_formula_of(self, names: set, get_KC = False):
        raise NotImplementedError()

    def extract_atomic_formulas(self):
        raise NotImplementedError()

    #TODO: Daniel new extract atomic formulas
    def extract_atomic_formulas_new(self):
        raise NotImplementedError()


class TrueFormula(Formula):
    #TODO: Daniel not sure if we need it in the new implementation
    def eval(self, binding: dict = None):
        return True

    def __repr__(self):
        return "True Formula"

    def extract_atomic_formulas(self):
        return []

    def get_formula_of(self, names, get_KC = False):
        return self

    def extract_atomic_formulas_new(self):
        return {}

    def consume_formula_of(self, names: set, consume_KC = False):
        raise NotImplementedError()


class NaryFormula(Formula):
    def __init__(self, *terms, relation_op: callable):
        self.terms = terms
        self.relation_op = relation_op

    def eval(self, binding: dict = None):
        try:
            rel_terms = []
            for term in self.terms:
                rel_terms.append(term.eval(binding))
            rel_terms = tuple(rel_terms)
            return self.relation_op(*rel_terms)
        except Exception as e:
            return False

    def get_formula_of(self, names: set, get_KC = False):
        for term in self.terms:
            cur_term = term.get_term_of(names)
            if cur_term is None:
                return None
        return self

    def extract_atomic_formulas(self):
        return [self]

    #TODO: Daniel new extract_atomic_formulas
    def extract_atomic_formulas_new(self):
        ret_dict = {}
        if len(self.terms) is not 0:
            ret_dict[self.terms[0]] = []
            ret_dict[self.terms[0]].append((self.terms[1:], self.relation_op))
        return ret_dict


class BinaryFormula(NaryFormula):
    """
    An binary formula containing no logic operators (e.g., A < B).
    """
    def __init__(self, left_term, right_term, relation_op: callable):
        super().__init__(left_term, right_term, relation_op=relation_op)

        #TODO: Daniel, Placeholder to not break the code till a proper conditions upon a tree will be established.
        self.left_term = left_term
        self.right_term = right_term

    def extract_atomic_formulas(self):
        return [self]

    def extract_atomic_formulas_new(self):
        ret_dict = {self.left_term: [(self.right_term, self.relation_op)]}
        return ret_dict


class BaseRelationFormula(BinaryFormula, ABC):
    def __init__(self, left_term, right_term, relation_op: callable, relop_type):
        super().__init__(left_term, right_term, relation_op)
        #TODO: Daniel, once conditions in the tree are set, remove relop_type from the code.
        self.relop_type = relop_type

    def get_relop(self):
        return self.relop_type

    def __repr__(self):
        raise NotImplementedError()


class EqFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        super().__init__(left_term, right_term, lambda x, y: x == y, RelopTypes.Equal)

    def __repr__(self):
        return "{} == {}".format(self.left_term, self.right_term)


class NotEqFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        super().__init__(left_term, right_term, lambda x, y: x != y, RelopTypes.NotEqual)

    def __repr__(self):
        return "{} != {}".format(self.left_term, self.right_term)


class GreaterThanFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        super().__init__(left_term, right_term, lambda x, y: x > y, RelopTypes.Greater)

    def __repr__(self):
        return "{} > {}".format(self.left_term, self.right_term)


class SmallerThanFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        super().__init__(left_term, right_term, lambda x, y: x < y, RelopTypes.Smaller)

    def __repr__(self):
        return "{} < {}".format(self.left_term, self.right_term)


class GreaterThanEqFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        super().__init__(left_term, right_term, lambda x, y: x >= y, RelopTypes.GreaterEqual)

    def __repr__(self):
        return "{} >= {}".format(self.left_term, self.right_term)


class SmallerThanEqFormula(BaseRelationFormula):
    def __init__(self, left_term, right_term):
        super().__init__(left_term, right_term, lambda x, y: x <= y, RelopTypes.SmallerEqual)

    def __repr__(self):
        return "{} <= {}".format(self.left_term, self.right_term)


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

    def get_formula_of(self, names: set, get_KC = False):
        result_formulas = []
        for f in self.__formulas:
            current_formula = f.get_formula_of(names)
            if current_formula:
                if get_KC == isinstance(current_formula, KCFormula):
                    result_formulas.extend([current_formula])
        return result_formulas

    def consume_formula_of(self, names: set, consume_KC = False):
        formulas_to_remove = []
        for i, f in enumerate(self.__formulas):
            current_formula = f.get_formula_of(names)
            if current_formula:
                if consume_KC == isinstance(current_formula, KCFormula):
                    formulas_to_remove.append(i)
        for i in reversed(formulas_to_remove):
            if not isinstance(self.__formulas[i], CompositeFormula):
                self.__formulas.pop(i)
            else:
                nested_formula = self.__formulas[i]
                nested_formula.consume_formula_of(names)

    def extract_atomic_formulas(self):
        result = []
        for f in self.__formulas:
            result.extend(f.extract_atomic_formulas())
        return result

    #TODO: Daniel   New extract atomic_formulas`
    def extract_atomic_formulas_new(self):
        ret_dict = {}
        for f in self.__formulas:
            if len(ret_dict) is 0:
                ret_dict = f.extract_atomic_formulas_new()
                continue
            extracted_dict = f.extract_atomic_formulas_new()
            for key in extracted_dict:
                if key not in ret_dict.keys():
                    ret_dict[key] = []
                ret_dict[key].extend(extracted_dict[key])
        return ret_dict


class AndFormula(CompositeFormula):
    """
    This class uses CompositeFormula with the terminating condition False, which complies with AND operator logic.
    """
    def __init__(self, formula_list: List[Formula]):
        super().__init__(formula_list, False)

    def get_formula_of(self, names: set, get_KC = False):
        result_formulas = super().get_formula_of(names)
        # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        if result_formulas:
            return AndFormula(result_formulas)
        else:
            return None


class OrFormula(CompositeFormula):
    """
        This class uses CompositeFormula with the terminating condition True, which complies with OR operator logic.
        """
    def __init__(self, formula_list: List[Formula]):
        super().__init__(formula_list, True)

    def get_formula_of(self, names: set, get_KC = False):
        result_formulas = super().get_formula_of(names)
        # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        if result_formulas:
            return OrFormula(result_formulas)
        else:
            return None


class KCFormula:
    def __init__(self, identifier, relation_op):
        self._identifier = identifier
        self._relation_op = relation_op

    def get_formula_of(self, names: set, get_KC = False):
        if self._identifier.get_term_of(names):
            return self


class KCOffsetFormula(KCFormula):
    def __init__(self, identifier, index_offset, relation_op):
        super().__init__(identifier, relation_op)
        self._offset = index_offset

    def eval(self, iterable):
        for i in range(len(iterable)):
            if i - self._offset < 0 or i - self._offset > len(iterable) - 1 \
                    or i + self._offset < 0 or i + self._offset > len(iterable) - 1:
                continue
            if not self._relation_op(self._identifier.eval((iterable[i])),
                                     self._identifier.eval((iterable[i+self._offset]))):
                return False
        return True


class KCIndexFormula(KCFormula):
    def __init__(self, identifier, index_1, index_2, relation_op):
        super().__init__(identifier, relation_op)
        self._index_1 = index_1
        self._index_2 = index_2

    def eval(self, iterable):
        if self._index_1 < 0 or self._index_1 > len(iterable) - 1 \
                or self._index_2 < 0 or self._index_2 > len(iterable) - 1:
            return False
        if not self._relation_op(self._identifier.eval((iterable[self._index_1])),
                                 self._identifier.eval((iterable[self._index_2]))):
            return False
        return True


class KCValueFormula(KCFormula):
    def __init__(self, identifier, index_1, value, relation_op):
        super().__init__(identifier, relation_op)
        self._index = index_1
        self._value = value

    def eval(self, iterable):
        if self._index < 0 or self._index > len(iterable) - 1:
            return False
        if not self._relation_op(self._identifier.eval((iterable[self._index])), self._value):
            return False
        return True
