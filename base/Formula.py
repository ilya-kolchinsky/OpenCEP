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
        # return nothing to KC nodes
        if get_KC is True:
            return None
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
        # return nothing to KC nodes
        if get_KC is True:
            return None
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
            current_formula = f.get_formula_of(names, get_KC)
            if current_formula and get_KC == isinstance(current_formula, KCFormula):
                result_formulas.extend([current_formula])
        return result_formulas

    def consume_formula_of(self, names: set, consume_KC = False):
        formulas_to_remove = []
        for i, f in enumerate(self.__formulas):
            current_formula = f.get_formula_of(names, consume_KC)
            if current_formula:
                if consume_KC == isinstance(current_formula, KCFormula):
                    formulas_to_remove.append(i)
        for i in reversed(formulas_to_remove):
            if not isinstance(self.__formulas[i], CompositeFormula):
                self.__formulas.pop(i)
            else:
                nested_formula = self.__formulas[i]
                nested_formula.consume_formula_of(names)
        # clear empty composite formulas
        # self.__formulas = [formula for formula in self.__formulas if self.filter_formula(formula, consume_KC)]

    def get_num_formulas(self):
        return len(self.__formulas)

    def extract_atomic_formulas(self):
        result = []
        for f in self.__formulas:
            result.extend(f.extract_atomic_formulas())
        return result

    @staticmethod
    def filter_formula(formula, ignore_KC):
        if isinstance(formula, CompositeFormula) and formula.get_num_formulas() == 0:
            return False
        elif (ignore_KC and isinstance(formula, KCFormula)) or \
                (not ignore_KC) and not isinstance(formula, KCFormula):
            return False
        return True

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

    def get_formula_of(self, names: set, get_KC=False):
        result_formulas = super().get_formula_of(names, get_KC)
        # result_formulas = [formula for formula in result_formulas if CompositeFormula.filter_formula(formula, get_KC)]
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
        result_formulas = super().get_formula_of(names, get_KC)
        # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        if result_formulas:
            return OrFormula(result_formulas)
        else:
            return None


class KCFormula(ABC):
    def __init__(self, names, getattr_func, relation_op):
        self._names = names
        self._getattr_func = getattr_func
        self._relation_op = relation_op

    def get_formula_of(self, names: set, get_KC = False):
        if names == self._names:
            return self
        if len(names) != len(self._names):
            return None
        for name in names:
            if not any([name in local_name for local_name in self._names]):
                return None
        return self

    @staticmethod
    def validate_index(index, iterable):
        if index < 0 or index >= len(iterable):
            return False
        return True

    def eval(self, iterable):
        raise NotImplementedError()


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

    def eval(self, iterable):
        if self._offset is not None:
            return self.eval_by_offset(iterable)
        else:
            return self.eval_by_index(iterable)

    def eval_by_index(self, iterable):
        if not self.validate_index(self._index_1, iterable) or not self.validate_index(self._index_2, iterable):
            return False
        item_1 = iterable[self._index_1]
        item_2 = iterable[self._index_2]
        if not self._relation_op(self._getattr_func(item_1), self._getattr_func(item_2)):
            return False
        return True

    # very time consuming process on large power-sets -- to be discussed
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
        return ((index_1 is not None and index_2 is not None) or
                offset is not None) and not (index_1 is not None and index_2 is not None and offset is not None)

class KCValueFormula(KCFormula):
    def __init__(self, names, getattr_func, relation_op, value, index=None):
        super().__init__(names, getattr_func, relation_op)
        self._value = value
        self._index = index

    def eval(self, iterable):
        try:
            if not self.validate_index(self._index, iterable):
                return False
            if self._index is None:
                for item in iterable:
                    if not self._relation_op(self._getattr_func(item), self._value):
                        return False
            else:
                if not self._relation_op(self._getattr_func(iterable[self._index]), self._value):
                    return False
            return True
        # catch any exception during the evaluation of abstract types with no type safety. this should return
        # RuntimeError for expected exceptions, but I do not know what to expect so any exception is caught
        # on error - we print stack trace and return False as default value.
        # this will allow the program to run while also printing the failure.
        except Exception as e:
            traceback.print_stack(e)
            return False
