from typing import Dict

from base.Pattern import Pattern
from misc import DefaultConfig
from plan.TreePlan import TreePlanNode, TreePlan
from plan.multi.local_search.LocalSearch import TabuSearch, SimulatedAnnealingSearch
from plan.multi.local_search.LocalSearchApproaches import LocalSearchApproaches
from plan.multi.local_search.MultiPatternGraph import MultiPatternGraph
from plan.multi.RecursiveTraversalTreePlanMerger import RecursiveTraversalTreePlanMerger


class LocalSearchTreePlanMerger(RecursiveTraversalTreePlanMerger):

    def merge_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                         eval_mechanism_params):
        """
        Merges the given tree plans of individual tree plans into a global shared structure.
        """

        default_params = {'search_approach': DefaultConfig.DEFAULT_SEARCH_TYPE,
                          'time_limit': DefaultConfig.LOCAL_SEARCH_TIME_LIMIT,
                          'steps_threshold': DefaultConfig.LOCAL_SEARCH_STEPS_THRESHOLD,
                          'tabu_search_neighborhood_size': DefaultConfig.TABU_SEARCH_NEIGHBORHOOD_SIZE,
                          'tabu_search_capacity': DefaultConfig.TABU_SEARCH_CAPACITY,
                          'sa_initial_value': DefaultConfig.SIMULATED_ANNEALING_INITIAL_THRESHOLD,
                          'sa_multiplier': DefaultConfig.SIMULATED_ANNEALING_MULTIPLIER,
                          'sa_final_threshold': DefaultConfig.SIMULATED_ANNEALING_C_THRESHOLD,
                          }
        local_search_tree_params = eval_mechanism_params.optimizer_params.local_search_tree_params or {}
        local_search_tree_params = {**default_params, **local_search_tree_params}

        local_search_alg = None
        if local_search_tree_params.get('search_approach') == LocalSearchApproaches.TABU_SEARCH:
            local_search_alg = TabuSearch(
                steps_threshold=local_search_tree_params.get('steps_threshold'),
                time_threshold=local_search_tree_params.get('time_limit'),
                initial_capacity=local_search_tree_params.get('tabu_search_capacity'),
                lookup_radius=local_search_tree_params.get('tabu_search_neighborhood_size'),
                pattern_to_tree_plan_map=pattern_to_tree_plan_map,
                eval_mechanism_params=eval_mechanism_params
            )
        elif local_search_tree_params.get('search_approach') == LocalSearchApproaches.SIMULATED_ANNEALING_SEARCH:
            local_search_alg = SimulatedAnnealingSearch(
                steps_threshold=local_search_tree_params.get('steps_threshold'),
                time_threshold=local_search_tree_params.get('time_limit'),
                alpha=local_search_tree_params.get('multiplier'),
                c_threshold=local_search_tree_params.get('sa_final_threshold'),
                c_0=local_search_tree_params.get('sa_initial_value'),
                pattern_to_tree_plan_map=pattern_to_tree_plan_map,
                eval_mechanism_params=eval_mechanism_params
            )
        else:
            raise ValueError('')

        return local_search_alg.get_best_solution()

    def _are_suitable_for_share(self, first_node: TreePlanNode, second_node: TreePlanNode):
        """
        This algorithm restricts all shareable nodes to be tree plan leaves.
        """
        return False
