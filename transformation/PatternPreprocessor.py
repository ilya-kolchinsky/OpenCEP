from copy import deepcopy
from typing import List

from transformation.PatternPreprocessingParameters import PatternPreprocessingParameters
from transformation.PatternTransformationRules import PatternTransformationRules
from transformation.PatternTransformer import *


class PatternPreprocessor:
    """
    This class receives the patterns initially supplied by a system user and transforms them to simplify
    their structure. The preprocessing is handled by iteratively applying the specified list of transformation rules.
    """
    def __init__(self, pattern_preprocessing_params: PatternPreprocessingParameters = None):
        if pattern_preprocessing_params is None:
            pattern_preprocessing_params = PatternPreprocessingParameters()
        self.__pattern_transformers = None
        self.__init_pattern_transformers(pattern_preprocessing_params.transformation_rules)
        self.__max_pattern_id = 0

    def __init_pattern_transformers(self, transformation_rules: List[PatternTransformationRules]):
        """
        Initializes the modules for pattern transformation.
        """
        if transformation_rules is None:
            return
        self.__pattern_transformers = []
        for rule in transformation_rules:
            self.__pattern_transformers.append(self.__create_transformer_by_rule(rule))

    @staticmethod
    def __create_transformer_by_rule(rule: PatternTransformationRules):
        """
        Initializes a pattern transformer according to the given rule.
        """
        if rule == PatternTransformationRules.AND_AND_PATTERN:
            return AndAndTransformer()
        if rule == PatternTransformationRules.NOT_OR_PATTERN:
            return NotOrTransformer()
        if rule == PatternTransformationRules.NOT_AND_PATTERN:
            return NotAndTransformer()
        if rule == PatternTransformationRules.TOPMOST_OR_PATTERN:
            return TopmostOrTransformer()
        if rule == PatternTransformationRules.INNER_OR_PATTERN:
            return InnerOrTransformer()
        if rule == PatternTransformationRules.NOT_NOT_PATTERN:
            return NotNotTransformer()
        raise Exception("Unknown transformation rule specified: %s" % (rule,))

    @staticmethod
    def __are_patterns_modified(previous_patterns: List[Pattern], current_patterns: List[Pattern]):
        """
        Returns True if the two patterns lists are not equal (that is, the patterns in current_patterns were modified
        during the most recent transformation iteration) and False otherwise.
        """
        if previous_patterns is None:
            return True  # no processing was done yet
        if len(previous_patterns) != len(current_patterns):
            return True
        for previous_pattern, current_pattern in zip(previous_patterns, current_patterns):
            if previous_pattern.full_structure != current_pattern.full_structure:
                return True
        return False

    def transform_patterns(self, patterns: Pattern or List[Pattern]):
        """
        Transforms the given list of patterns according to the predefined transformation rules.
        """
        if patterns is None or len(patterns) == 0:
            raise Exception("No patterns are provided")
        if self.__pattern_transformers is None:
            # preprocessing is disabled
            return patterns
        if type(patterns) == Pattern:
            patterns = [patterns]

        previous_patterns = None
        current_patterns = deepcopy(patterns)
        while self.__are_patterns_modified(previous_patterns, current_patterns):
            previous_patterns = deepcopy(current_patterns)
            transformed_pattern_sublists = [self.__transform_pattern(p) for p in current_patterns]
            current_patterns = [p for sublist in transformed_pattern_sublists for p in sublist]

        return current_patterns

    def __transform_pattern(self, pattern: Pattern):
        """
        Transforms the given pattern using the predefined transformers.
        """
        transformed_patterns = [pattern]  # some transformers could turn a pattern into multiple patterns
        for transformer in self.__pattern_transformers:
            transformed_pattern_sublists = [transformer.transform(p) for p in transformed_patterns]
            transformed_patterns = [p for sublist in transformed_pattern_sublists for p in sublist]
        return transformed_patterns
