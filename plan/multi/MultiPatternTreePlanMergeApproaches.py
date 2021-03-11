from enum import Enum


class MultiPatternTreePlanMergeApproaches(Enum):
    """
    The various approaches for constructing a merged tree plan builder.
    TRIVIAL_SHARING_LEAVES: only merges common leaves of the different tree plans.
    SUBTREES_UNION: builds a unified multi-tree by sharing equivalent subtrees of the different tree plans.
    TREE_PLAN_LOCAL_SEARCH: applies a local search algorithm to find even more efficient global plans by modifying
    the original individual plans.
    """
    TREE_PLAN_TRIVIAL_SHARING_LEAVES = 0,
    TREE_PLAN_SUBTREES_UNION = 1,
    TREE_PLAN_LOCAL_SEARCH = 2  # not yet implemented
