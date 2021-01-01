from enum import Enum


class MultiPatternTreePlanUnionApproaches(Enum):
    """
    The various approaches for constructing a unified tree plan builder.
    TRIVIAL_SHARING_LEAVES: gets a list of patterns and builds a merged tree for each pattern, while sharing equivalent leaves
    from different tree plans.
    SUBTREES_UNION: gets a list of patterns and builds a unified tree by sharing equivalent subtrees of different tree plans.
    TREE_PLAN_DIFFERENT_TOPOLOGY_UNION: as SUBTREES_UNION, but builds a unified tree by modifying the tree topology trying to get maximum intersection
    """
    TREE_PLAN_TRIVIAL_SHARING_LEAVES = 0,
    TREE_PLAN_SUBTREES_UNION = 1,
    TREE_PLAN_CHANGE_TOPOLOGY_UNION = 2
