from condition import Condition
from base.Pattern import Pattern
from base.transformation.RuleTransformationParameters import RuleTransformationParameters
from base.transformation.RuleTransformationTypes import RuleTransformationTypes
from base.transformation.TransformationRules import *

"""
The Rule Transformation class consists of a few rules intended to simplify the pattern structure.
Rules directive lists rules in the required priority while only enabled rules are listed.
"""


class PatternTransformation():
    """
    The base class for pattern transformation application.
    """
    def __init__(self, pattern_list: list,
                 rules_directive: list = RuleTransformationParameters()):
        self.max_id = self.calculate_max_id(pattern_list)
        self.pattern_list = deepcopy(pattern_list)
        self.rules_directive = rules_directive.rules_directive.copy()

    def calculate_max_id(self, pattern_list: list):
        """
        Returns the max ID for all patterns in list.
        If no IDs were set, 0 will be returned.
        """
        max_id = 0
        for pattern in pattern_list:
            if type(pattern.id) == int:
                if pattern.id > max_id:
                    max_id = pattern.id
        return max_id

    def generate_id(self):
        """
        Returns next available ID.
        """
        self.max_id = self.max_id + 1
        return self.max_id

    def get_max_id(self):
        return self.max_id

    def transform_patterns(self):
        """
        Iterate over a list of pattern and apply transformation rules.
        Returns a list of new patterns.
        """
        if not self.pattern_list:
            return
        new_pattern_list = deepcopy(self.pattern_list)
        for i in range(len(new_pattern_list)):
            old_pattern = new_pattern_list.pop(0)
            old_pattern_structure = old_pattern.full_structure
            new_pattern_structures = self.apply_transformation_rules(old_pattern_structure)

            if type(new_pattern_structures) != list:
                new_pattern_structures = [new_pattern_structures]
            for new_pattern_structure in new_pattern_structures:
                new_pattern_id = self.generate_id()
                new_pattern_condition = self.extract_min_condition(new_pattern_structure, old_pattern.condition)
                new_pattern = Pattern(new_pattern_structure, new_pattern_condition,
                                      old_pattern.window, old_pattern.consumption_policy, new_pattern_id,
                                      old_pattern.statistics_type, old_pattern.statistics)
                new_pattern_list.append(new_pattern)
        return new_pattern_list

    def extract_min_condition(self, pattern_structure: PatternStructure, condition: Condition):
        """
        Returns a modified condition, eliminating events that do not appear in pattern structure.
        """
        # TODO: Implement
        # pattern_event_names = pattern_structure.get_all_event_names()
        return condition

    def apply_transformation_rules(self, pattern_structure: PatternStructure):
        """
        Method receives a single pattern structure
        and transforms it to a list of pattern structures according to the enabled rules.
        """
        transformed_pattern_structures_list = [pattern_structure.duplicate()]
        i = True
        for rule_name in self.rules_directive:
            if rule_name == RuleTransformationTypes.AND_AND_PATTERN:
                rule = AndAndRule()
            elif rule_name == RuleTransformationTypes.NOT_OR_PATTERN:
                rule = NotOrRule()
            elif rule_name == RuleTransformationTypes.NOT_AND_PATTERN:
                rule = NotAndRule()
            elif rule_name == RuleTransformationTypes.TOPMOST_OR_PATTERN:
                rule = TopmostOrRule()
            elif rule_name == RuleTransformationTypes.INNER_OR_PATTERN:
                rule = InnerOrRule()
            elif rule_name == RuleTransformationTypes.NOT_NOT_PATTERN:
                rule = NotNotRule()
            else:
                i = False
                raise NotImplementedError()
            if i:
                transformed_pattern_structures_list = rule.transform_pattern_structure_list(
                    transformed_pattern_structures_list)
            i = True
        return transformed_pattern_structures_list

