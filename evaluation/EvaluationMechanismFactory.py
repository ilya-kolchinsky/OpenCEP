from typing import List, Dict

from base.Pattern import Pattern
from evaluation.EvaluationMechanismTypes import EvaluationMechanismTypes
from misc import DefaultConfig
from plan.multi.ShareLeavesTreePlanMerger import ShareLeavesTreePlanMerger
from plan.multi.SubTreeSharingTreePlanMerger import SubTreeSharingTreePlanMerger
from plan.TreePlan import TreePlan
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters, TreePlanBuilderFactory
from plan.multi.MultiPatternTreePlanMergeApproaches import MultiPatternTreePlanMergeApproaches
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
                 storage_params: TreeStorageParameters = TreeStorageParameters()):
        super().__init__(EvaluationMechanismTypes.TREE_BASED)
        self.tree_plan_params = tree_plan_params
        self.storage_params = storage_params


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
        if isinstance(patterns, Pattern) or len(patterns) == 1:
            # a single-pattern case
            pattern = patterns if isinstance(patterns, Pattern) else patterns[0]
            pattern_to_tree_plan_map = {pattern: tree_plan_builder.build_tree_plan(pattern)}
            return TreeBasedEvaluationMechanism(pattern_to_tree_plan_map, eval_mechanism_params.storage_params)

        pattern_to_tree_plan_map = {pattern: tree_plan_builder.build_tree_plan(pattern) for pattern in patterns}
        unified_tree_map = EvaluationMechanismFactory.__merge_tree_plans(
            pattern_to_tree_plan_map, eval_mechanism_params.tree_plan_params.tree_plan_merge_type)
        unified_tree = TreeBasedEvaluationMechanism(unified_tree_map, eval_mechanism_params.storage_params)
        return unified_tree

    @staticmethod
    def __merge_tree_plans(pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                           tree_plan_merge_approach: MultiPatternTreePlanMergeApproaches):
        """
        Merges the given tree plans of individual tree plans into a global shared structure.
        """
        if tree_plan_merge_approach == MultiPatternTreePlanMergeApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES:
            return ShareLeavesTreePlanMerger().merge_tree_plans(pattern_to_tree_plan_map)
        if tree_plan_merge_approach == MultiPatternTreePlanMergeApproaches.TREE_PLAN_SUBTREES_UNION:
            return SubTreeSharingTreePlanMerger().merge_tree_plans(pattern_to_tree_plan_map)
        if tree_plan_merge_approach == MultiPatternTreePlanMergeApproaches.TREE_PLAN_LOCAL_SEARCH:
            # TODO: not yet implemented
            pass
        raise Exception("Unsupported multi-pattern merge algorithm %s" % (tree_plan_merge_approach,))

    @staticmethod
    def __create_default_eval_parameters():
        """
        Uses the default configuration to create evaluation mechanism parameters.
        """
        if DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE == EvaluationMechanismTypes.TREE_BASED:
            return TreeBasedEvaluationMechanismParameters()
        raise Exception("Unknown evaluation mechanism type: %s" % (DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE,))
