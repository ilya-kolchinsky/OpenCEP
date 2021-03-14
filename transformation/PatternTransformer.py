from abc import ABC

from base.Pattern import Pattern
from base.PatternStructure import PatternStructure, AndOperator, OrOperator, NegationOperator, \
    UnaryStructure, CompositeStructure, PrimitiveEventStructure


class PatternTransformer(ABC):
    """
    An abstract class for transforming pattern structure according to a predefined rule.
    """
    def transform(self, pattern: Pattern):
        """
        Applies a transformation rule on the given pattern.
        """
        new_structures = self._transform_structure(pattern.full_structure)
        return [self.__create_pattern_for_new_structure(structure, pattern) for structure in new_structures]

    @staticmethod
    def __create_pattern_for_new_structure(structure: PatternStructure, pattern: Pattern):
        """
        Creates a new pattern for the given transformed structure based on the given pattern.
        """
        if structure == pattern.full_structure:
            return pattern
        condition_for_new_pattern = pattern.condition.get_condition_of(structure.get_all_event_names())
        return Pattern(structure, condition_for_new_pattern, pattern.window, pattern.consumption_policy, None,
                       pattern.confidence, pattern.statistics)

    def _transform_structure(self, pattern_structure):
        """
        Performs the transformation - to be implemented by subclasses.
        """
        raise NotImplementedError()


class RecursivePatternTransformer(PatternTransformer):
    """
    An abstract class for rules that are to be applied in a recursive manner.
    The rules are restricted to only include 1:1 transformations.
    """
    def _transform_structure(self, pattern_structure):
        return [self.__recursive_transform_structure(pattern_structure)]

    def __recursive_transform_structure(self, pattern_structure):
        """
        Recursively applies the transformation rule.
        """
        if isinstance(pattern_structure, PrimitiveEventStructure):
            return pattern_structure
        if isinstance(pattern_structure, UnaryStructure):
            pattern_structure.arg = self.__recursive_transform_structure(pattern_structure.arg)
        if isinstance(pattern_structure, CompositeStructure):
            pattern_structure.args = [self.__recursive_transform_structure(arg) for arg in pattern_structure.args]
        return self._actually_transform_structure(pattern_structure)

    def _actually_transform_structure(self, pattern_structure):
        """
        Performs the actual transformation - to be implemented by subclasses.
        """
        raise NotImplementedError()


class InnerOrTransformer(PatternTransformer):
    """
    Splits a disjunctive pattern structure into multiple pattern structures: OR(A,B,...) -> A,B,...
    """
    def _transform_structure(self, pattern_structure):
        if pattern_structure.get_top_operator() != OrOperator:
            # not a disjunction pattern - nothing to be done
            return [pattern_structure]
        return pattern_structure.args


class AndAndTransformer(RecursivePatternTransformer):
    """
    Flattens the nested appearances of the same operator, e.g., AND(A,AND(B,C),D) -> AND(A,B,C,D).
    """
    def _actually_transform_structure(self, pattern_structure):
        if not isinstance(pattern_structure, CompositeStructure):
            return pattern_structure
        top_operator = pattern_structure.get_top_operator()
        new_args = []
        for arg in pattern_structure.args:
            if arg.get_top_operator() == top_operator:
                new_args.extend(arg.args)
            else:
                new_args.append(arg)
        pattern_structure.args = new_args
        return pattern_structure


class TopmostOrTransformer(RecursivePatternTransformer):
    """
    Pulls the disjunction operator to the top level, e.g., AND(X,OR(A,B,...),Y) -> OR(AND(X,A,Y),AND(X,B,Y),...).
    """
    def _actually_transform_structure(self, pattern_structure):
        if not isinstance(pattern_structure, CompositeStructure) or pattern_structure.get_top_operator() == OrOperator:
            return pattern_structure
        or_operator_index = None
        or_operator = None
        for index, current_operator in enumerate(pattern_structure.args):
            if current_operator.get_top_operator() == OrOperator:
                or_operator_index = index
                or_operator = current_operator
                break
        if or_operator_index is None:
            return pattern_structure
        new_args = []
        for arg_under_or in or_operator.args:
            new_operator = pattern_structure.duplicate_top_operator()
            new_operator.args = pattern_structure.args[:or_operator_index] + [arg_under_or] + \
                                pattern_structure.args[or_operator_index+1:]
            new_args.append(new_operator)
        return OrOperator(*new_args)


class NotAndTransformer(RecursivePatternTransformer):
    """
    Applies the AND-based De-Morgan rule, e.g., NOT(AND(A,B,...) -> OR(NOT(A),NOT(B),...).
    """
    def _actually_transform_structure(self, pattern_structure):
        if pattern_structure.get_top_operator() != NegationOperator or \
                pattern_structure.arg.get_top_operator() != AndOperator:
            return pattern_structure
        new_args = [NegationOperator(arg) for arg in pattern_structure.arg.args]
        return OrOperator(*new_args)


class NotOrTransformer(RecursivePatternTransformer):
    """
    Applies the OR-based De-Morgan rule, e.g., NOT(OR(A,B,...) -> AND(NOT(A),NOT(B),...).
    """
    def _actually_transform_structure(self, pattern_structure):
        if pattern_structure.get_top_operator() != NegationOperator or \
                pattern_structure.arg.get_top_operator() != OrOperator:
            return pattern_structure
        new_args = [NegationOperator(arg) for arg in pattern_structure.arg.args]
        return AndOperator(*new_args)


class NotNotTransformer(RecursivePatternTransformer):
    """
    Removes duplicate negation operators, e.g., NOT(NOT(A) -> A.
    """
    def _actually_transform_structure(self, pattern_structure):
        if pattern_structure.get_top_operator() != NegationOperator or \
                pattern_structure.arg.get_top_operator() != NegationOperator:
            return pattern_structure
        return pattern_structure.arg.arg
