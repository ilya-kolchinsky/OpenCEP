from copy import deepcopy
from base.Pattern import Pattern
from misc.DefaultConfig import *
from base.PatternStructure import PatternStructure, AndOperator, OrOperator, SeqOperator

"""
The Rule Transformation class consists of a few rules intended to simplify the pattern structure.
Each rule can be enabled or disabled, and each has a set priority (in configuration file).
"""

# class RuleTransformation:
def pattern_transformation(pattern : Pattern):
    """
    Method recieves a single pattern and transforms it to a list of patterns according to the enabled rules.
    """
    transformed_pattern_structures_list = []
    transformed_pattern_structures_list.append(pattern.full_structure.duplicate())

    rules_priority_list = get_priority_list()
    for rule_name in rules_priority_list:
        if rule_name == "AND_AND":
            transformed_pattern_structures_list = transform_and_and(transformed_pattern_structures_list)
        elif rule_name == "SEQ_OR":
            transformed_pattern_structures_list = transform_seq_or(transformed_pattern_structures_list)
        # elif rule_name == "SEQ_NOT_AND":
        #     pass

    pattern_list = []
    for transformed_pattern_structure in transformed_pattern_structures_list:
        pattern_list.append(Pattern(transformed_pattern_structure, pattern.condition, pattern.window, pattern.consumption_policy, pattern.id))
    return pattern_list

def get_priority_list():
    """
    Returns pattern transformation rule priority list (first rule in list should be applied first)
    """
    priority_list = []
    tmp_priority_dictionary = PRIORITY_PATTERN_TRANSFORMATION.copy()
    for i in range(len(PRIORITY_PATTERN_TRANSFORMATION)):
        min_priority = PRIORITY_PATTERN_TRANSFORMATION_MIN
        min_rule = ''
        for key in tmp_priority_dictionary.keys():
            value = tmp_priority_dictionary[key]
            if value < min_priority:
                min_priority = value
                min_rule = key
        priority_list.append(min_rule)
        del tmp_priority_dictionary[min_rule]
    return priority_list

def transform_and_and(pattern_structures_list : list):
    """
    Returns modified pattern structures applying only the AND(AND()) rule
    """
    pattern_structures_list_copy = deepcopy(pattern_structures_list)
    # expand AND operators (nested inside AND)
    if EXPAND_PATTERN_AND_AND:
        for i in range(len(pattern_structures_list)):
            old_pattern_structure = pattern_structures_list_copy.pop(0)
            if old_pattern_structure.get_top_operator() == AndOperator:
                tmp_pattern_structure = type(old_pattern_structure)()
                tmp_pattern_structure.add_args(expand_and_operator(old_pattern_structure))
                pattern_structures_list_copy.append(tmp_pattern_structure)
            else:
                pattern_structures_list_copy.append(old_pattern_structure)
    return pattern_structures_list_copy

def transform_seq_or(pattern_structures_list : list):
    """
    Returns modified pattern structures applying only the SEQ(OR()) rule
    """
    pattern_structures_list_copy = deepcopy(pattern_structures_list)
    # expand OR operators (nested inside SEQ)
    if EXPAND_PATTERN_SEQ_OR:
        for i in range(len(pattern_structures_list)):
            old_pattern_structure = pattern_structures_list_copy.pop(0)
            if old_pattern_structure.get_top_operator() == SeqOperator:
                new_args_list = expand_or_operator(old_pattern_structure)
                for new_args in new_args_list:
                    tmp_pattern_structure = type(old_pattern_structure)()
                    tmp_pattern_structure.add_args(new_args)
                    pattern_structures_list_copy.append(tmp_pattern_structure)
            else:
                pattern_structures_list_copy.append(old_pattern_structure)
    return pattern_structures_list_copy

def transform_seq_not_and(pattern_structures_list : list):
    """
    Returns modified pattern structures applying only the SEQ(NOT(AND())) rule
    """
    pattern_structures_list_copy = deepcopy(pattern_structures_list)

def expand_and_operator(pattern_structure : PatternStructure):
    """
    Apply the AND rule on a single pattern structure and return args only
    """
    new_args = []
    for arg in pattern_structure.get_args():
        if type(arg) == AndOperator:
            new_args.extend(expand_and_operator(arg))
        else:
            new_args.append(arg)
    return new_args

def expand_or_operator(pattern_structure : PatternStructure):
    """
    Apply the OR rule on a single pattern structure and return args only
    """
    new_args = []
    for arg in pattern_structure.get_args():
        if type(arg) == OrOperator:
            tmp_args_list = deepcopy(new_args)
            new_args.clear()
            for or_arg in arg.get_args():
                tmp_args_list.append(or_arg)
                new_args.append(deepcopy(tmp_args_list))
                tmp_args_list.pop()
        else:
            new_args.append(arg)
    return new_args


