from abc import ABC  # Abstract Base Class
import copy


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
        self.simplifiable = True
        self.abstract_terms = [{"sign": 1, "term": self, "is_id": False}]

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
        self.simplifiable = True
        self.abstract_terms = [{"sign": 1, "term": self, "is_id": True}]

    def eval(self, binding: dict = None):
        if not type(binding) == dict or self.name not in binding:
            raise NameError("Name %s is not bound to a value" % self.name)
        return self.getattr_func(binding[self.name])

    def get_term_of(self, names: set):
        if self.name in names:
            return self
        return None

    def __repr__(self):  # MUH
        return self.name


class BinaryOperationTerm(Term):
    """
    A term representing a binary operation.
    """

    def __init__(self, lhs: Term, rhs: Term, binary_op: callable, is_Minus=False):
        self.lhs = lhs
        self.rhs = rhs
        self.binary_op = binary_op
        self.simplifiable = lhs.simplifiable and rhs.simplifiable
        new_rhs_terms = copy.deepcopy(rhs.abstract_terms)
        new_lhs_terms = copy.deepcopy(lhs.abstract_terms)
        if is_Minus:
            for item in new_rhs_terms:
                item["sign"] = -item["sign"]
        self.abstract_terms = new_lhs_terms + new_rhs_terms

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

    def __repr__(self):  # MUH
        return "{}+{}".format(self.lhs, self.rhs)


class MinusTerm(BinaryOperationTerm):
    def __init__(self, lhs: Term, rhs: Term):
        super().__init__(lhs, rhs, lambda x, y: x - y, is_Minus=True)

    def get_term_of(self, names: set):
        lhs = self.lhs.get_term_of(names)
        rhs = self.rhs.get_term_of(names)
        if lhs and rhs:
            return MinusTerm(lhs, rhs)
        return None

    def __repr__(self):  # MUH
        return "{}-{}".format(self.lhs, self.rhs)


class MulTerm(BinaryOperationTerm):
    def __init__(self, lhs: Term, rhs: Term):
        super().__init__(lhs, rhs, lambda x, y: x * y)
        self.simplifiable = False

    def get_term_of(self, names: set):
        lhs = self.lhs.get_term_of(names)
        rhs = self.rhs.get_term_of(names)
        if lhs and rhs:
            return MulTerm(lhs, rhs)
        return None

    def __repr__(self):  # MUH
        return "{}*{}".format(self.lhs, self.rhs)


class DivTerm(BinaryOperationTerm):
    def __init__(self, lhs: Term, rhs: Term):
        super().__init__(lhs, rhs, lambda x, y: x / y)
        self.simplifiable = False

    def get_term_of(self, names: set):
        lhs = self.lhs.get_term_of(names)
        rhs = self.rhs.get_term_of(names)
        if lhs and rhs:
            return DivTerm(lhs, rhs)
        return None

    def __repr__(self):  # MUH
        return "{}/{}".format(self.lhs, self.rhs)


class Formula(ABC):
    """
    Returns whether the parameters satisfy the formula. It evaluates to True or False.
    If there are variables (identifiers) in the formula, a name-value binding shall be inputted.
    """

    def eval(self, binding: dict = None):
        pass

    def get_formula_of(self, names: set):
        pass

    def simplify_formula(self, lhs_vars: set, rhs_vars: set):

        """
        Returns a simplified formula where the lhs term consist only of lhs_vars, 
        and rhs term from only rhs_vars.
        returns None if simplification is complicated (one term contains div for example)
        """
        return None


class AtomicFormula(Formula):  # RELOP: < <= > >= == !=
    """
    An atomic formula containing no logic operators (e.g., A < B).
    """

    def __init__(self, left_term: Term, right_term: Term, relation_op: callable):
        self.left_term = left_term
        self.right_term = right_term
        self.relation_op = relation_op

    def eval(self, binding: dict = None):
        return self.relation_op(self.left_term.eval(binding), self.right_term.eval(binding))

    def __repr__(self):  # MUH
        return "{} {} {}".format(self.left_term, self.relation_op, self.right_term)

    def simplify_formula(self, lhs_vars: set, rhs_vars: set):
        lhs_term_vars = set()
        rhs_term_vars = set()

        for item in self.left_term.abstract_terms:
            if item["is_id"] == True:
                lhs_term_vars.add(item["term"].name)

        for item in self.right_term.abstract_terms:
            if item["is_id"] == True:
                rhs_term_vars.add(item["term"].name)

        # check if already simple : set() is for removing duplicates and empty set cases
        if set(lhs_vars) == set(lhs_term_vars) and set(rhs_vars) == set(rhs_term_vars):
            return self

        # check if a possible simplification exists
        if not (self.left_term.simplifiable and self.right_term.simplifiable):
            return None

        new_lhs_term, new_rhs_term = self.rearrange_terms(lhs_vars, rhs_vars)
        return AtomicFormula(new_lhs_term, new_rhs_term, self.relation_op)

    def rearrange_terms(self, lhs_vars, rhs_vars):
        new_lhs_term = AtomicTerm(0)
        new_rhs_term = AtomicTerm(0)

        (new_lhs_term, new_rhs_term) = self.consume_terms(
            self.left_term.abstract_terms, new_lhs_term, new_rhs_term, lhs_vars
        )
        (new_rhs_term, new_lhs_term) = self.consume_terms(
            self.right_term.abstract_terms, new_rhs_term, new_lhs_term, rhs_vars
        )

        return (new_lhs_term, new_rhs_term)

    def consume_terms(self, terms, same_side_term, other_side_term, curr_side_vars):
        for cur_term in terms:
            if cur_term["is_id"]:
                if cur_term["sign"] == 1:  # plus
                    if cur_term["term"].name in curr_side_vars:
                        same_side_term = PlusTerm(same_side_term, cur_term["term"])
                    else:  # opposite side of the equation, gets opposite sign
                        other_side_term = MinusTerm(other_side_term, cur_term["term"])
                else:  # minus
                    if cur_term["term"].name in curr_side_vars:
                        same_side_term = MinusTerm(same_side_term, cur_term["term"])
                    else:  # opposite side of the equation, gets opposite sign
                        other_side_term = PlusTerm(other_side_term, cur_term["term"])
            else:  # atomic term
                if cur_term["sign"] == 1:  # plus
                    same_side_term = PlusTerm(same_side_term, cur_term["term"])
                else:  # minus
                    same_side_term = MinusTerm(same_side_term, cur_term["term"])
        return (same_side_term, other_side_term)


    def dismantle(self):
        return (
            self.left_term,
            self.convert_to_relop(self.relation_op),
            self.right_term,
        )

    def convert_to_relop(self, relation_op: callable):
        if relation_op(5, 5):
            if relation_op(5, 6):
                return "<="
            elif relation_op(5, 4):
                return ">="
            return "=="
        else:
            if relation_op(5, 6):
                return "<"
            elif relation_op(5, 4):
                return ">"
            return "!="


class EqFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x == y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return EqFormula(left_term, right_term)
        return None

    def __repr__(self):  # MUH
        return "{} == {}".format(self.left_term, self.right_term)


class NotEqFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x != y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return NotEqFormula(left_term, right_term)
        return None

    def __repr__(self):  # MUH
        return "{} != {}".format(self.left_term, self.right_term)


class GreaterThanFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x > y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return GreaterThanFormula(left_term, right_term)
        return None

    def __repr__(self):  # MUH
        return "{} > {}".format(self.left_term, self.right_term)


class SmallerThanFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x < y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return SmallerThanFormula(left_term, right_term)
        return None

    def __repr__(self):  # MUH
        return "{} < {}".format(self.left_term, self.right_term)


class GreaterThanEqFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x >= y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return GreaterThanEqFormula(left_term, right_term)
        return None

    def __repr__(self):  # MUH
        return "{} >= {}".format(self.left_term, self.right_term)


class SmallerThanEqFormula(AtomicFormula):
    def __init__(self, left_term: Term, right_term: Term):
        super().__init__(left_term, right_term, lambda x, y: x <= y)

    def get_formula_of(self, names: set):
        right_term = self.right_term.get_term_of(names)
        left_term = self.left_term.get_term_of(names)
        if left_term and right_term:
            return SmallerThanEqFormula(left_term, right_term)
        return None

    def __repr__(self):  # MUH
        return "{} <= {}".format(self.left_term, self.right_term)


class BinaryLogicOpFormula(Formula):  # AND: A < B AND C < D
    """
    A formula composed of a logic operator and two nested formulas.
    """

    def __init__(self, left_formula: Formula, right_formula: Formula, binary_logic_op: callable):
        self.left_formula = left_formula
        self.right_formula = right_formula
        self.binary_logic_op = binary_logic_op

    def eval(self, binding: dict = None):
        return self.binary_logic_op(self.left_formula.eval(binding), self.right_formula.eval(binding))


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

    def __repr__(self):  # MUH
        return "{} AND {}".format(self.left_formula, self.right_formula)


class TrueFormula(Formula):
    def eval(self, binding: dict = None):
        return True

    def __repr__(self):  # MUH
        return "True Formula"
