from plan.negation.NegationAlgorithmTypes import NegationAlgorithmTypes
from plan.BushyTreeBuilders import *
from plan.invariant.InvariantBushyTreeBuilder import InvariantAwareZStreamTreeBuilder
from plan.invariant.InvariantLeftDeepTreeBuilder import InvariantAwareGreedyTreeBuilder
from plan.LeftDeepTreeBuilders import *
from plan.TreeCostModels import TreeCostModels
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes


class TreePlanBuilderParameters:
    """
    Parameters for the tree plan builder.
    """
    def __init__(self, builder_type: TreePlanBuilderTypes = DefaultConfig.DEFAULT_TREE_PLAN_BUILDER,
                 cost_model_type: TreeCostModels = DefaultConfig.DEFAULT_TREE_COST_MODEL,
                 negation_algorithm_type: NegationAlgorithmTypes = DefaultConfig.DEFAULT_NEGATION_ALGORITHM,
                 tree_plan_merger_type=DefaultConfig.DEFAULT_TREE_PLAN_MERGE):
        self.builder_type = builder_type
        self.cost_model_type = cost_model_type
        self.negation_algorithm_type = negation_algorithm_type
        self.tree_plan_merge_type = tree_plan_merger_type


class IterativeImprovementTreePlanBuilderParameters(TreePlanBuilderParameters):
    """
    Parameters for tree plan builders based on local search include the number of search steps, the
    choice of the neighborhood (step) function, and the way to generate the initial state.
    """
    def __init__(self, cost_model_type: TreeCostModels, step_limit: int,
                 ii_type: IterativeImprovementType = DefaultConfig.ITERATIVE_IMPROVEMENT_TYPE,
                 init_type: IterativeImprovementInitType = DefaultConfig.ITERATIVE_IMPROVEMENT_INIT_TYPE):
        super().__init__(TreePlanBuilderTypes.LOCAL_SEARCH_LEFT_DEEP_TREE, cost_model_type)
        self.ii_type = ii_type
        self.init_type = init_type
        self.step_limit = step_limit


class TreePlanBuilderFactory:
    """
    Creates a tree plan builder according to the specification.
    """
    @staticmethod
    def create_tree_plan_builder(tree_plan_params: TreePlanBuilderParameters):
        if tree_plan_params.builder_type == TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE:
            return TrivialLeftDeepTreeBuilder(tree_plan_params.cost_model_type,
                                              tree_plan_params.negation_algorithm_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE:
            return AscendingFrequencyTreeBuilder(tree_plan_params.cost_model_type,
                                                 tree_plan_params.negation_algorithm_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE:
            return GreedyLeftDeepTreeBuilder(tree_plan_params.cost_model_type,
                                             tree_plan_params.negation_algorithm_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.LOCAL_SEARCH_LEFT_DEEP_TREE:
            return IterativeImprovementLeftDeepTreeBuilder(tree_plan_params.cost_model_type,
                                                           tree_plan_params.negation_algorithm_type,
                                                           tree_plan_params.step_limit,
                                                           tree_plan_params.ii_type,
                                                           tree_plan_params.init_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE:
            return DynamicProgrammingLeftDeepTreeBuilder(tree_plan_params.cost_model_type,
                                                         tree_plan_params.negation_algorithm_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE:
            return DynamicProgrammingBushyTreeBuilder(tree_plan_params.cost_model_type,
                                                      tree_plan_params.negation_algorithm_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE:
            return ZStreamTreeBuilder(tree_plan_params.cost_model_type,
                                      tree_plan_params.negation_algorithm_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE:
            return ZStreamOrdTreeBuilder(tree_plan_params.cost_model_type,
                                         tree_plan_params.negation_algorithm_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.INVARIANT_AWARE_GREEDY_LEFT_DEEP_TREE:
            return InvariantAwareGreedyTreeBuilder(tree_plan_params.cost_model_type,
                                                   tree_plan_params.negation_algorithm_type)
        if tree_plan_params.builder_type == TreePlanBuilderTypes.INVARIANT_AWARE_ZSTREAM_BUSHY_TREE:
            return InvariantAwareZStreamTreeBuilder(tree_plan_params.cost_model_type,
                                                    tree_plan_params.negation_algorithm_type)
        raise Exception("Unknown tree plan builder type: %s" % (tree_plan_params.builder_type,))
