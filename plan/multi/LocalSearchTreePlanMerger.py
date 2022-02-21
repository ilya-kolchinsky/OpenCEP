from typing import Dict

from adaptive.optimizer.Optimizer import Optimizer
from base.Pattern import Pattern
from plan.TreePlan import TreePlanNode, TreePlan
from plan.multi.local_search.LocalSearchFactory import LocalSearchParameters, LocalSearchFactory
from plan.multi.RecursiveTraversalTreePlanMerger import RecursiveTraversalTreePlanMerger


class LocalSearchTreePlanMerger(RecursiveTraversalTreePlanMerger):

    def merge_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                         local_search_parameters: LocalSearchParameters, optimizer: Optimizer):
        local_search = LocalSearchFactory.build_local_search(pattern_to_tree_plan_map, optimizer,
                                                             local_search_parameters)
        return local_search.get_best_solution()
