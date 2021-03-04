from abc import ABC
from copy import deepcopy
from base.PatternStructure import PatternStructure, AndOperator, OrOperator, SeqOperator, NegationOperator, \
    PrimitiveEventStructure, KleeneClosureOperator

"""
The Rule Transformation class consists of a few rules intended to simplify the pattern structure.
Rules directive lists rules in the required priority while only enabled rules are listed.
"""

class TransformationRule(ABC):
    """
    The base class for pattern transformation rules.
    """
    def transform_pattern_structure_list(self, pattern_structure_list: list):
        """
        Apply the corresponding transformation rule on multiple pattern structures.
        Returns a list of pattern structures.
        """
        transformed_pattern_structure_list = []
        for pattern_structure in pattern_structure_list:
            tmp_pattern_structures = self.transform_pattern_structure(pattern_structure)
            if type(tmp_pattern_structures) != list:
                tmp_pattern_structures = [tmp_pattern_structures]
            for tmp_pattern_structure in tmp_pattern_structures:
                if tmp_pattern_structure not in transformed_pattern_structure_list:
                    transformed_pattern_structure_list.append(tmp_pattern_structure)
        return transformed_pattern_structure_list
    
    def transform_pattern_structure(self, pattern_structure: PatternStructure):
        """
        Apply the corresponding transformation rule on a single pattern structure.
        Returns a list of pattern structures.
        """
        if type(pattern_structure) == KleeneClosureOperator:
            return [pattern_structure]
        if type(pattern_structure) == PrimitiveEventStructure:
            return [pattern_structure]
        if pattern_structure.get_top_operator() in self.primary_operators:
            expanded_pattern_structure_list = self.validate_list(self.expand_operator(pattern_structure))
            if self.is_expanded(pattern_structure, expanded_pattern_structure_list):  # structure was expanded
                new_pattern_structure_list = []
                for expanded_pattern_structure in expanded_pattern_structure_list:
                    new_pattern_structure_list.extend(
                        self.validate_list(self.transform_pattern_structure(expanded_pattern_structure)))
                return new_pattern_structure_list
        new_pattern_structure = deepcopy(pattern_structure)
        if type(pattern_structure) == NegationOperator:
            pattern_args_list = pattern_structure.get_arg()
            pattern_args_list = [pattern_args_list]
        else:
            pattern_args_list = new_pattern_structure.get_args()
        new_args_lists = [[]]  # List of lists
        if type(pattern_args_list) == PrimitiveEventStructure:
            for new_args_list in new_args_lists:
                new_args_list.append(pattern_args_list)
        else:
            for pattern_arg in pattern_args_list:
                for new_args_list in new_args_lists:
                    trans = self.validate_list(self.transform_pattern_structure(pattern_arg))
                    new_args_list.extend(trans)
        new_pattern_structure_list = []
        if type(new_pattern_structure) == NegationOperator:
            for new_args_list in new_args_lists:
                new_pattern_structure = type(new_pattern_structure)(new_args_list[0])
                new_pattern_structure_list.append(new_pattern_structure)
        else:
            for new_args_list in new_args_lists:
                new_pattern_structure = type(new_pattern_structure)()
                new_pattern_structure.add_args(new_args_list)
                new_pattern_structure_list.append(new_pattern_structure)
        return new_pattern_structure_list

    def is_expanded(self, pattern_structure: PatternStructure, pattern_structure_list: list):
        if type(pattern_structure_list) == list:
            if len(pattern_structure_list) > 1:
                return True
            if not pattern_structure_list:
                return False
            if pattern_structure_list[0] != pattern_structure:
                return True
            return False
        if pattern_structure != pattern_structure_list:
            return True
        return False

    def validate_list(self, potential_list):
        if type(potential_list) == list:
            return potential_list
        return [potential_list]


class AndAndRule(TransformationRule):
    """
    Represents the transformation rule:
    AND(A,AND(B,C),D) -> AND(A,B,C,D)
    """
    def __init__(self):
        self.primary_operators = [AndOperator]

    def expand_operator(self, pattern_structure: PatternStructure):
        """
        Returns a modified pattern structure applying only the rule:
        AND(A,AND(B,C),D) -> AND(A,B,C,D)
        """
        new_args = []
        for arg in pattern_structure.get_args():
            if type(arg) == AndOperator:
                new_args.extend(self.expand_operator(arg).get_args())
            else:
                new_args.append(arg)
        new_pattern_structure = type(pattern_structure)()
        new_pattern_structure.add_args(new_args)
        return new_pattern_structure


class NotAndRule(TransformationRule):
    """
    Represents the transformation rule:
    OP(X,NOT(AND(A,B,...)),Y) -> OP(X,OR(NOT(A),NOT(B),...),Y)
    """
    def __init__(self):
        self.primary_operators = [NegationOperator]

    def expand_operator(self, pattern_structure: PatternStructure):
        """
        Apply the NOT(AND()) rule on a single pattern structure and return new pattern
        """
        operator_arg = pattern_structure.get_arg()
        if type(operator_arg) != AndOperator:
            return pattern_structure
        new_args_list = []
        and_args = operator_arg.get_args()
        for and_arg in and_args:
            tmp_pattern_structure = NegationOperator(and_arg)
            new_args_list.append(tmp_pattern_structure)
        new_pattern_structure = OrOperator()
        new_pattern_structure.add_args(new_args_list)
        return new_pattern_structure


class NotOrRule(TransformationRule):
    """
    Represents the transformation rule:
    OP(X,NOT(OR(A,B,...)),Y) -> OP(X,AND(NOT(A),NOT(B),...),Y)
    """
    def __init__(self):
        self.primary_operators = [NegationOperator]

    def expand_operator(self, pattern_structure: PatternStructure):
        """
        Apply the NOT(OR()) rule on a single pattern structure and return new pattern
        """
        operator_arg = pattern_structure.get_arg()
        if type(operator_arg) != OrOperator:
            return pattern_structure
        new_args_list = []
        and_args = operator_arg.get_args()
        for and_arg in and_args:
            tmp_pattern_structure = NegationOperator(and_arg)
            new_args_list.append(tmp_pattern_structure)
        new_pattern_structure = AndOperator()
        new_pattern_structure.add_args(new_args_list)
        return new_pattern_structure


class NotNotRule(TransformationRule):
    """
    Represents the transformation rule:
    OP(X,NOT(NOT(A),Y) -> OP(X,A,Y)
    """
    def __init__(self):
        self.primary_operators = [NegationOperator]

    def expand_operator(self, pattern_structure: PatternStructure):
        """
        Apply the NOT(NOT()) rule on a single pattern structure and return new pattern
        """
        operator_arg = pattern_structure.get_arg()
        if type(operator_arg) != NegationOperator:
            return pattern_structure
        return operator_arg.get_arg()


class InnerOrRule(TransformationRule):
    """
    Represents the transformation rule:
    OR(A,B,...) -> OR(A),OR(B),...
    """
    def __init__(self):
        self.primary_operators = [AndOperator, SeqOperator, OrOperator]

    def expand_operator(self, pattern_structure: PatternStructure):
        """
        Apply the inner OR rule on a single pattern structure and return a list of pattern structures
        """
        if pattern_structure.get_top_operator() == OrOperator:
            return pattern_structure.get_args()
        new_args_lists = [[]]
        for operator_arg in pattern_structure.get_args():
            if type(operator_arg) == OrOperator:
                tmp_args_lists = deepcopy(new_args_lists)
                new_args_lists.clear()
                for or_arg in operator_arg.get_args():
                    for tmp_args_list in tmp_args_lists:
                        tmp_args_list.append(or_arg)
                        new_args_lists.append(deepcopy(tmp_args_list))
                        tmp_args_list.pop()
            else:
                for new_args_list in new_args_lists:
                    new_args_list.append(operator_arg)
        new_pattern_structure_list = []
        for new_args_list in new_args_lists:
            new_pattern_structure = type(pattern_structure)()
            new_pattern_structure.add_args(new_args_list)
            new_pattern_structure_list.append(new_pattern_structure)
        return new_pattern_structure_list


class TopmostOrRule(TransformationRule):
    """
    Represents the transformation rule:
    OP(X,OR(A,B,...),Y) -> OR(OP(X,A,Y),OP(X,B,Y),...)
    """
    def __init__(self):
        self.primary_operators = [AndOperator, SeqOperator]

    def expand_operator(self, pattern_structure: PatternStructure):
        """
        Apply the topmost OR rule on a single pattern structure and return a new pattern structure
        """
        rule = InnerOrRule()
        rule.primary_operators = self.primary_operators
        new_pattern_structure_list = rule.transform_pattern_structure(pattern_structure)
        if len(new_pattern_structure_list) > 1:
            new_pattern_structure = OrOperator()
            new_pattern_structure.add_args(new_pattern_structure_list)
            return new_pattern_structure
        return pattern_structure


