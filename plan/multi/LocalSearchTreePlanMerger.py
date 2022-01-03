from typing import Dict

from base.Pattern import Pattern
from plan.TreePlan import TreePlanNode, TreePlanLeafNode, TreePlan
from plan.multi.MultiPatternGraph import MultiPatternGraph
from plan.multi.RecursiveTraversalTreePlanMerger import RecursiveTraversalTreePlanMerger


# constants for simulated annealing search
ALPHA = 0.99
I = 1e3

# constants for Tabu Search
L = 1e2
C = 1e4


class LocalSearchTreePlanMerger(RecursiveTraversalTreePlanMerger):

    def merge_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan]):
        """
                Merges the given tree plans of individual tree plans into a global shared structure.
                """
        mpg = MultiPatternGraph(list(pattern_to_tree_plan_map.keys()))


        raise NotImplementedError()

    def _are_suitable_for_share(self, first_node: TreePlanNode, second_node: TreePlanNode):
        """
        This algorithm restricts all shareable nodes to be tree plan leaves.
        """
        return False
