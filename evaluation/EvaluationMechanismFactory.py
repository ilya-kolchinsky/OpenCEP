from typing import List

from base.Pattern import Pattern
from evaluation.EvaluationMechanismTypes import EvaluationMechanismTypes
from misc import DefaultConfig
from plan.MPT_neighborhood import algoA
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreePlanBuilderFactory
from plan.UnifiedTreeBuilder import UnifiedTreeBuilder
from plan.multi.MultiPatternEvaluationParameters import MultiPatternEvaluationParameters
from plan.multi.MultiPatternUnifiedTreePlanApproaches import MultiPatternTreePlanUnionApproaches
from tree.PatternMatchStorage import TreeStorageParameters
from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism


class EvaluationMechanismParameters:
    """
    Parameters required for evaluation mechanism creation.
    """

    def __init__(self, eval_mechanism_type: EvaluationMechanismTypes = DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE):
        self.type = eval_mechanism_type


class TreeBasedEvaluationMechanismParameters(EvaluationMechanismParameters):
    """
    Parameters for the creation of a tree-based evaluation mechanism.
    """

    def __init__(self, tree_plan_params: TreePlanBuilderParameters = TreePlanBuilderParameters(),
                 storage_params: TreeStorageParameters = TreeStorageParameters(),
                 multi_pattern_eval_params: MultiPatternEvaluationParameters = MultiPatternEvaluationParameters()):
        super().__init__(EvaluationMechanismTypes.TREE_BASED)
        self.tree_plan_params = tree_plan_params
        self.storage_params = storage_params
        self.multi_pattern_eval_params = multi_pattern_eval_params


class EvaluationMechanismFactory:
    """
    Creates an evaluation mechanism given its specification.
    """

    @staticmethod
    def build_single_pattern_eval_mechanism(eval_mechanism_params: EvaluationMechanismParameters,
                                            pattern: Pattern):
        if eval_mechanism_params is None:
            eval_mechanism_params = EvaluationMechanismFactory.__create_default_eval_parameters()
        if eval_mechanism_params.type == EvaluationMechanismTypes.TREE_BASED:
            return EvaluationMechanismFactory.__create_tree_based_eval_mechanism(eval_mechanism_params, pattern)
        raise Exception("Unknown evaluation mechanism type: %s" % (eval_mechanism_params.type,))

    @staticmethod
    def build_multi_pattern_eval_mechanism(eval_mechanism_params: EvaluationMechanismParameters,
                                           patterns: List[Pattern]):
        if eval_mechanism_params is None:
            eval_mechanism_params = EvaluationMechanismFactory.__create_default_eval_parameters()
        if eval_mechanism_params.type == EvaluationMechanismTypes.TREE_BASED:
            return EvaluationMechanismFactory.__create_tree_based_eval_mechanism(eval_mechanism_params, patterns)
        raise Exception("Unknown evaluation mechanism type: %s" % (eval_mechanism_params.type,))

    @staticmethod
    def __create_tree_based_eval_mechanism(eval_mechanism_params: TreeBasedEvaluationMechanismParameters,
                                           patterns: Pattern or List[Pattern]):
        """
        Instantiates a tree-based CEP evaluation mechanism according to the given configuration.
        in this function we fix the given implementation by merging treePattern and not trees , then we create
        TreeBasedEvaluationMechanism from the merged treePlans
        """
        tree_plan_builder = TreePlanBuilderFactory.create_tree_plan_builder(eval_mechanism_params.tree_plan_params)
        if isinstance(patterns, Pattern):
            patterns = [patterns]
            pattern_to_tree_plan_map = {pattern: tree_plan_builder.build_tree_plan(pattern) for pattern in patterns}
            tree = TreeBasedEvaluationMechanism(pattern_to_tree_plan_map, eval_mechanism_params.storage_params,
                                                        eval_mechanism_params.multi_pattern_eval_params)
            return tree

        pattern_to_tree_plan_map = {pattern: tree_plan_builder.build_tree_plan(pattern) for pattern in patterns}
        unified_tree_map = {}
        if eval_mechanism_params.tree_plan_params.tree_plan_union_type == MultiPatternTreePlanUnionApproaches.TREE_PLAN_LOCAL_SEARCH_ANNEALING:
            unified_tree_map = algoA.construct_subtrees_local_search_tree_plan(pattern_to_tree_plan_map,
                                                                               eval_mechanism_params.tree_plan_params.tree_plan_local_search_params)
        else:
            union_builder = UnifiedTreeBuilder()
            unified_tree_map = union_builder._union_tree_plans(pattern_to_tree_plan_map.copy(),
                                                           eval_mechanism_params.tree_plan_params.tree_plan_union_type)

        unified_tree = TreeBasedEvaluationMechanism(unified_tree_map, eval_mechanism_params.storage_params,
                                                    eval_mechanism_params.multi_pattern_eval_params)
        return unified_tree

    @staticmethod
    def __create_default_eval_parameters():
        """
        Uses the default configuration to create evaluation mechanism parameters.
        """
        if DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE == EvaluationMechanismTypes.TREE_BASED:
            return TreeBasedEvaluationMechanismParameters()
        raise Exception("Unknown evaluation mechanism type: %s" % (DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE,))
