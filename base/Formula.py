from abc import ABC
from enum import Enum


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


class Variable:
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

    def __repr__(self):
        return self.name


class Formula(ABC):
    """
    Returns whether the parameters satisfy the formula. It evaluates to True or False.
    If there are variables (identifiers) in the formula, a name-value binding shall be inputted.
    """
    def eval(self, binding: dict or list = None):
        raise NotImplementedError()

    def extract_atomic_formulas(self):
        raise NotImplementedError()


class AtomicFormula(Formula, ABC):
    """
    Represents an atomic (non-composite) formula.
    """
    def is_formula_of(self, names: set):
        raise NotImplementedError()

    def extract_atomic_formulas(self):
        return [self]


class TrueFormula(AtomicFormula):
    def eval(self, binding: dict = None):
        return True

    def __repr__(self):
        return "True Formula"

    def extract_atomic_formulas(self):
        return []

    def is_formula_of(self, names: set):
        return False


class NaryFormula(AtomicFormula):
    """
    An Nary formula containing terms and a boolean callable operator.
    """
    def __init__(self, *terms, relation_op: callable):
        self._terms = terms
        self.relation_op = relation_op

    def eval(self, binding: dict = None):
        rel_terms = []
        for term in self._terms:
            rel_terms.append(term.eval(binding) if isinstance(term, Variable) else term)
        rel_terms = tuple(rel_terms)
        return self.relation_op(*rel_terms)

    def is_formula_of(self, names: set):
        for term in self._terms:
            if term.name not in names:
                return False
        return True

    def __repr__(self):
        term_list = []
        for term in self._terms:
            term_list.append(term.__repr__())
        separator = ', '
        return "[" + separator.join(term_list) + "]"


class BinaryFormula(NaryFormula):
    """
    A binary formula containing no logic operators (e.g., A < B).
    """
    def __init__(self, left_term, right_term, relation_op: callable):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of BinaryFormula!")
        elif not isinstance(left_term, Variable):
            super().__init__(right_term, relation_op=relation_op)
        elif not isinstance(right_term, Variable):
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


class BaseRelationFormula(BinaryFormula, ABC):
    """
        An abstract binary formula class which is a base for already implemented and commonly used binary relations
        such as: >, >=, <, <=, ==, !=.
    """
    def __init__(self, left_term, right_term, relation_op: callable, relop_type):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of BaseRelationFormula!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, relation_op=relation_op(left_term))
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, relation_op=relation_op(right_term))
        else:
            super().__init__(left_term, right_term, relation_op)
        self.relop_type = relop_type
        self.left_term_repr = left_term
        self.right_term_repr = right_term

    def get_relop(self):
        return self.relop_type

    def __repr__(self):
        raise NotImplementedError()


class EqFormula(BaseRelationFormula):
    """
        Binary Equal Formula; ==
        This class can be called either with terms or a number:
        Examples:
            EqFormula(Variable("a", lambda x: x["Opening Price"]), 135)
            EqFormula(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of EqFormula!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x == y, RelopTypes.Equal)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y == x, RelopTypes.Equal)
        else:
            super().__init__(left_term, right_term, lambda x, y: x == y, RelopTypes.Equal)

    def __repr__(self):
        return "{} == {}".format(self.left_term_repr, self.right_term_repr)


class NotEqFormula(BaseRelationFormula):
    """
        Binary Not Equal Formula; !=
        Examples:
            NotEqFormula(Variable("a", lambda x: x["Opening Price"]), 135)
            NotEqFormula(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of NotEqFormula!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x != y, RelopTypes.NotEqual)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y != x, RelopTypes.NotEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x != y, RelopTypes.NotEqual)

    def __repr__(self):
        return "{} != {}".format(self.left_term_repr, self.right_term_repr)


class GreaterThanFormula(BaseRelationFormula):
    """
        Binary greater than formula; >
        Examples:
            GreaterThanFormula(Variable("a", lambda x: x["Opening Price"]), 135)
            GreaterThanFormula(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of GreaterThanFormula!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x > y, RelopTypes.Greater)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y > x, RelopTypes.Greater)
        else:
            super().__init__(left_term, right_term, lambda x, y: x > y, RelopTypes.Greater)

    def __repr__(self):
        return "{} > {}".format(self.left_term_repr, self.right_term_repr)


class SmallerThanFormula(BaseRelationFormula):
    """
        Binary smaller than formula; <
        Examples:
            SmallerThanFormula(Variable("a", lambda x: x["Opening Price"]), 135)
            SmallerThanFormula(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of SmallerThanFormula!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x < y, RelopTypes.Smaller)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y < x, RelopTypes.Smaller)
        else:
            super().__init__(left_term, right_term, lambda x, y: x < y, RelopTypes.Smaller)

    def __repr__(self):
        return "{} < {}".format(self.left_term_repr, self.right_term_repr)


class GreaterThanEqFormula(BaseRelationFormula):
    """
        Binary greater and equal than formula; >=
        Examples:
            GreaterThanEqFormula(Variable("a", lambda x: x["Opening Price"]), 135)
            GreaterThanEqFormula(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of GreaterThanEqFormula!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x >= y, RelopTypes.GreaterEqual)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y >= x, RelopTypes.GreaterEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x >= y, RelopTypes.GreaterEqual)

    def __repr__(self):
        return "{} >= {}".format(self.left_term_repr, self.right_term_repr)


class SmallerThanEqFormula(BaseRelationFormula):
    """
        Binary smaller and equal than formula; <=
        Examples:
            SmallerThanEqFormula(Variable("a", lambda x: x["Opening Price"]), 135)
            SmallerThanEqFormula(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))
    """
    def __init__(self, left_term, right_term):
        if not isinstance(left_term, Variable) and not isinstance(right_term, Variable):
            raise Exception("Invalid use of SmallerThanEqFormula!")
        elif not isinstance(left_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: x <= y, RelopTypes.SmallerEqual)
        elif not isinstance(right_term, Variable):
            super().__init__(left_term, right_term, lambda x: lambda y: y <= x, RelopTypes.SmallerEqual)
        else:
            super().__init__(left_term, right_term, lambda x, y: x <= y, RelopTypes.SmallerEqual)

    def __repr__(self):
        return "{} <= {}".format(self.left_term_repr, self.right_term_repr)


class CompositeFormula(Formula, ABC):
    """
    This class represents BinaryLogicOp formulas that support a list of formulas within them.
    This is used to support different syntax for the formula creating, instead of using a full tree of formulas
    it will enable passing a list of formulas (Atomic or Non-Atomic) and apply the encapsulating operator on all of them
    And stops at the first FALSE from the evaluation and returns False.
    Or stops at the first TRUE from the evaluation and return true
    """
    def __init__(self, terminating_condition: bool, *formula_list):
        self.__formulas = list(formula_list)
        self.__terminating_result = terminating_condition

    def eval(self, binding: dict = None):
        if self.get_num_formulas() == 0:
            return True
        for formula in self.__formulas:
            if formula.eval(binding) == self.__terminating_result:
                return self.__terminating_result
        return not self.__terminating_result

    def get_formula_of(self, names: set, get_kc_formulas_only=False, consume_returned_formulas=False):
        result_formulas = []
        formulas_to_remove = []
        for index, current_formula in enumerate(self.__formulas):
            if isinstance(current_formula, CompositeFormula):
                inner_formula = current_formula.get_formula_of(names, get_kc_formulas_only, consume_returned_formulas)
                if inner_formula.get_num_formulas() > 0:
                    # non-empty nested condition was returned
                    result_formulas.append(inner_formula)
                if consume_returned_formulas and current_formula.get_num_formulas() == 0:
                    # all previously existing nested conditions were probably consumed - consume this empty condition
                    formulas_to_remove.append(index)
                continue
            # this is a simple condition
            if not current_formula.is_formula_of(names):
                continue
            if get_kc_formulas_only != isinstance(current_formula, KCFormula):
                # either this is a KC condition and we asked for non-KC conditions,
                # or this is a non-KC condition and we asked for KC conditions
                continue
            result_formulas.append(current_formula)
            if consume_returned_formulas:
                formulas_to_remove.append(index)

        # remove the conditions at previously saved indices
        for index in reversed(formulas_to_remove):
            self.__formulas.pop(index)

        return CompositeFormula(self.__terminating_result, *result_formulas)

    def get_num_formulas(self):
        return len(self.__formulas)

    def get_formulas_list(self):
        return self.__formulas

    def extract_atomic_formulas(self):
        result = []
        for f in self.__formulas:
            result.extend(f.extract_atomic_formulas())
        return result

    def add_atomic_formula(self, formula: Formula):
        """
        Adds a new atomic formula to this composite formula.
        """
        self.__formulas.append(formula)

    def __repr__(self):
        res_list = []
        for formula in self.__formulas:
            res_list.append(formula.__repr__())
        return res_list


class AndFormula(CompositeFormula):
    """
    This class uses CompositeFormula with the terminating condition False, which complies with AND operator logic.
    """
    def __init__(self, *formula_list):
        super().__init__(False, *formula_list)

    def get_formula_of(self, names: set, get_kc_formulas_only=False, consume_returned_formulas=False):
        comp_formula = super().get_formula_of(names, get_kc_formulas_only, consume_returned_formulas)
        # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        if comp_formula:
            return AndFormula(*comp_formula.get_formulas_list())
        return None

    def __repr__(self):
        return " AND ".join(super().__repr__())


class OrFormula(CompositeFormula):
    """
    This class uses CompositeFormula with the terminating condition True, which complies with OR operator logic.
    """
    def __init__(self, *formula_list):
        super().__init__(True, *formula_list)

    def get_formula_of(self, names: set, get_kc_formulas_only=False, consume_returned_formulas=False):
        comp_formula = super().get_formula_of(names, get_kc_formulas_only, consume_returned_formulas)
        # at-least 1 formula was retrieved using get_formula_of for the list of formulas
        if comp_formula:
            return OrFormula(*comp_formula.get_formulas_list())
        return None

    def __repr__(self):
        return " OR ".join(super().__repr__())


class KCFormula(AtomicFormula, ABC):
    """
    This class represents the base class for KleeneClosure formulas.
    """
    def __init__(self, names: set, getattr_func: callable, relation_op: callable):
        self._names = names
        self._getattr_func = getattr_func
        self._relation_op = relation_op

    def is_formula_of(self, names: set):
        if names == self._names:
            return True
        if len(names) != len(self._names):
            return False
        for name in names:
            if not any([name in n for n in self._names]):
                return False
        return True

    @staticmethod
    def _validate_index(index: int, iterable: list):
        """
        :param index: requested index
        :param iterable: requested event_list
        :return: true if the index is a valid index to query
        """
        return 0 <= index < len(iterable)

    def __repr__(self):
        return "KC [" + ", ".join(self._names) + "]"


class KCIndexFormula(KCFormula):
    """
    This class represents KCFormulas that perform operations between 2 indexes of the KleeneClosure events
    It supports comparisons of 2 types:
        - first_index and second_index will compare 2 specific indexes from the KC events
        - offset will compare every 2 items in KC events that meet the offset requirement. Supports negative offsets.

    If the offset is larger than the length of the list for offset mechanism,
        or 1 of the indexes is negative or out of bounds for index mechanism,
        the formula returns False.
    """
    def __init__(self, names: set, getattr_func: callable, relation_op: callable,
                 first_index=None, second_index=None, offset=None):
        """
        :param names: names to match for future evaluations
        :param getattr_func: method to extract attribute from payload
        :param relation_op: the relation which the formula represents
        :param first_index: first index to compare
        :param second_index: second index to compare
        :param offset: offset to compare. Supports negative offsets.

        Enforce getting 1 of 2 activations types ONLY:
            1) first_index and index_2 to compare
            2) offset to compare every 2 items that meet offset requirement (either positive or negative)
        Further activation types may be implemented for convenience.
        """
        if not self.__validate_params(first_index, second_index, offset):
            raise Exception("Invalid use of KCIndex formula.\nboth index and offset are not None\n refer to comment")
        super().__init__(names, getattr_func, relation_op)
        self.__first_index = first_index
        self.__second_index = second_index
        self.__offset = offset

    def eval(self, event_list: list = None):
        """
        :param event_list: the list of events to compare events from
        :return: True or False based on evaluation mechanism
        """
        # offset is active - choose evaluation by offset mechanism
        if self.__offset is not None:
            return self.__eval_by_offset(event_list)
        # offset is not active - choose evaluation by index mechanism
        return self.__eval_by_index(event_list)

    def __eval_by_index(self, event_list: list):
        """
        :param event_list: the list of events to compare evnets from
        :return: True or False based on index evaluation
        """
        # validate both indexes
        if not self._validate_index(self.__first_index, event_list) or \
                not self._validate_index(self.__second_index, event_list):
            return False
        # get the items
        item_1 = event_list[self.__first_index]
        item_2 = event_list[self.__second_index]
        # execute the relation op on both items
        if not self._relation_op(self._getattr_func(item_1), self._getattr_func(item_2)):
            return False
        return True

    # very time consuming process on large power-sets
    def __eval_by_offset(self, event_list: list):
        """
        :param event_list: the list of events to compare events from
        :return: True or False based on offset evaluation mechanism
        """
        # offset too large restriction
        if self.__offset >= len(event_list):
            return False

        for i in range(len(event_list)):
            # test if i + offset meets index requirements ( 0 <= i <= len(event_list) - 1)
            if not self._validate_index(i + self.__offset, event_list):
                continue
            # use AND logic to return True if EVERY two items that meet offset requirement return True. (early Abort)
            if not self._relation_op(self._getattr_func(event_list[i]),
                                     self._getattr_func(event_list[i + self.__offset])):
                return False
        return True

    @staticmethod
    def __validate_params(index_1, index_2, offset):
        """
        :param index_1: first_index provided to constructor
        :param index_2: second_index provided to constructor
        :param offset: offset privided to constructor
        :return: True if unsupported activation patterns are inserted to constructor.
        Current supported patterns allow (first_index AND second_index) OR (offset) AND (NOT BOTH).
        Disqualification semantics used to allow easier extensions in the future - simply remove the newly supported
        patterns from the disqualified patterns.
        """
        return not (                                                                     # idx1 idx2 offset
                (index_1 is None and index_2 is None and offset is None) or              # 0     0     0
                (index_1 is not None and index_2 is not None and offset is not None) or  # 1     1     1
                (index_1 is not None and offset is not None) or                          # 1     0     1
                (index_2 is not None and offset is not None) or                          # 0     1     1
                (index_1 is None and index_2 is not None) or                             # 1     0     0
                (index_1 is not None and index_2 is None)                                # 0     1     0
        )

    def __repr__(self):
        if self.__first_index is not None:
            return "KCIndex first_index={}, second_index={} [".format(self.__first_index, self.__second_index) + \
                   ", ".join(self._names) + "]"
        else:
            return "KCIndex offset={} [".format(self.__offset) + ", ".join(self._names) + "]"


class KCValueFormula(KCFormula):
    """
    This class represents KCFormulas that perform operations between events from the KleeneClosure events
    and an arbitrary value.
    It supports comparisons of 2 types:
        - value only comparison will compare all the items in KC events to a specific value
        - value and index comparison will compare a specific index from KC events to a specific value
    """
    def __init__(self, names: set, getattr_func: callable, relation_op: callable, value, index: int = None):
        """
        :param names: names to match for future evaluations
        :param getattr_func: method to extract attribute from payload
        :param relation_op: the relation which the formula represents
        :param value: the value to compare event items with
        :param index: the index to compare the item with. default mode is value only comparison
        """
        super().__init__(names, getattr_func, relation_op)
        self.__value = value
        self.__index = index

    def eval(self, event_list: list = None):
        """
        :param event_list: the list of events to compare events from
        :return: True or False based on evaluation mechanism
        """
        # index comparison method and invalid index - Abort.
        if self.__index is not None and not self._validate_index(self.__index, event_list):
            return False

        if self.__index is None:
            # no index used for comparison - compare all elements
            for item in event_list:
                # use AND logic to return True if EVERY item returns True when being compared to formula's value.
                if not self._relation_op(self._getattr_func(item), self.__value):
                    return False
        else:
            # compare 1 element from the list of events to the formula's value
            if not self._relation_op(self._getattr_func(event_list[self.__index]), self.__value):
                return False
        return True

    def __repr__(self):
        return "KCValue, index={}, value={} [".format(self.__index, self.__value) + ", ".join(self._names) + "]"
