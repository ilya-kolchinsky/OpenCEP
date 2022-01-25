from typing import Dict

from adaptive.optimizer.Optimizer import Optimizer
from base.Pattern import Pattern
from plan.TreePlan import TreePlanNode, TreePlan
from plan.multi.local_search.LocalSearchFactory import LocalSearchParameters, LocalSearchFactory
from plan.multi.RecursiveTraversalTreePlanMerger import RecursiveTraversalTreePlanMerger


class LocalSearchTreePlanMerger(RecursiveTraversalTreePlanMerger):

    def merge_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                         local_search_parameters: LocalSearchParameters, optimizer: Optimizer):
        """
        Merges the given tree plans of individual tree plans into a global shared structure.
        """
        local_search = LocalSearchFactory.build_local_search(pattern_to_tree_plan_map, optimizer,
                                                             local_search_parameters)
        return local_search.get_best_solution()
        # TODO: Should call super().merge_tree_plans()?

    def _are_suitable_for_share(self, first_node: TreePlanNode, second_node: TreePlanNode):
        """
        This algorithm allows any type of node to be shared.
        """
        return first_node.is_equivalent(second_node)
