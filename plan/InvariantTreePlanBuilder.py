from abc import ABC

from base.Pattern import Pattern
from plan.TreePlan import TreePlan
from plan.TreePlanBuilder import TreePlanBuilder


class InvariantTreePlanBuilder(TreePlanBuilder, ABC):
    """
    Base class for an invariant tree plan builders
    """
    def build_tree_plan(self, statistics: dict, pattern: Pattern):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        tree_topology, invariants = self._create_tree_topology(statistics, pattern)
        return TreePlan(tree_topology), invariants
