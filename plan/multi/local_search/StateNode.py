import random
from typing import Dict, List

from adaptive.optimizer.Optimizer import Optimizer
from base.Pattern import Pattern
from plan.TreeCostModel import IntermediateResultsTreeCostModel
from plan.TreePlan import TreePlan
from plan.multi.local_search.MultiPatternGraph import MultiPatternGraph


class StateNode:
    """
    Describes a state in the local search algorithm. The final state will be the chosen global plan.
    StateNode stores the current solution and its cost, and also allows transition between states
    using the Multi-Pattern Graph.
    """
    def __init__(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                 mpg: MultiPatternGraph,
                 optimizer: Optimizer,
                 shared_sub_trees: Dict[Pattern, List[TreePlan]] = None):

        self.__pattern_to_tree_plan_map = pattern_to_tree_plan_map
        self.__mpg = mpg
        self.__optimizer = optimizer
        self.__shared_sub_trees = shared_sub_trees or {}
        self.__cost = None

    def __eq__(self, other):
        return self.__pattern_to_tree_plan_map == other.__pattern_to_tree_plan_map

    @property
    def pattern_to_tree_plan_map(self):
        return self.__pattern_to_tree_plan_map

    def get_cost(self):
        """
        Get the total cost of the current solution.
        A visited dict is passed to the inner cost function, in order to avoid calculating the same node twice.
        """
        if self.__cost is None:
            cost_model = IntermediateResultsTreeCostModel()
            # Keep a set of the visited nodes, to avoid calculating the same subtree twice
            visited = set()
            total_cost = 0
            for pattern, plan in self.__pattern_to_tree_plan_map.items():
                total_cost += cost_model.get_plan_cost(pattern, plan.root, pattern.statistics, visited)
            self.__cost = total_cost
        return self.__cost

    def __repr__(self):
        return str(self.__pattern_to_tree_plan_map)

    def __hash__(self):
        return hash(str(self))

    def get_neighbor(self, neighborhood_vertex_size: int):
        """
        Return a neighbor of the current state, based on the neighborhood_vertex_size param.
        The algorithm chooses a random common subpattern in the multi pattern graph, and patterns that share it.
        Then, it tries to build a new plan that uses this shared subpattern, which generates a new global plan.
        """
        # Choose a random max common sub pattern and its patterns according to neighborhood-vertex-k decision
        tup = self.__mpg.get_random_max_pattern_and_peers(neighborhood_vertex_size)
        if tup is None:
            return None
        max_sub_pattern, chosen_patterns = tup

        # create a random sub pattern out of max_sub_pattern
        event_names = set(max_sub_pattern.get_primitive_event_names())
        rand_size = random.randint(0, len(event_names))
        filtered_events = random.sample(event_names, rand_size)
        random_sub_pattern = max_sub_pattern.get_sub_pattern(filtered_events)
        if random_sub_pattern is None:
            return None
        # create tree plan for sub pattern
        sub_pattern_plan = self.__optimizer.build_new_plan(random_sub_pattern.statistics, random_sub_pattern)

        # create new tree plans for the patterns involved with this subpattern
        # List for choosing randomly if preserve or not subtrees for each pattern
        pattern_to_tree_plan_map = self.__pattern_to_tree_plan_map.copy()
        preserve_subtrees = [True, False]
        modified_tree = False
        all_shared_subtrees = self.__shared_sub_trees.copy()  # will hold mapping of pattern -> shared sub trees
        for pattern in chosen_patterns:
            pattern_old_shared_subtrees = self.__shared_sub_trees.get(pattern, [])
            # If this plan is already shared with this pattern, ignore
            if sub_pattern_plan in pattern_old_shared_subtrees:
                continue

            pattern_new_shared_sub_trees = [sub_pattern_plan]
            if pattern_old_shared_subtrees:
                preserve = random.choice(preserve_subtrees)
                if preserve:
                    # get subtrees of pattern, pass them to generate new plan
                    pattern_new_shared_sub_trees += pattern_old_shared_subtrees
            # new tree plan for pattern
            if len(pattern_new_shared_sub_trees) > 0:
                all_shared_subtrees[pattern] = pattern_new_shared_sub_trees
                new_pattern_plan = self.__optimizer.build_new_plan(pattern.statistics, pattern,
                                                                   pattern_new_shared_sub_trees)
                pattern_to_tree_plan_map[pattern] = new_pattern_plan
                modified_tree = True

        # Return a state with the new plan, also store the shared subtrees in a mapping
        if modified_tree:
            return StateNode(pattern_to_tree_plan_map=pattern_to_tree_plan_map, mpg=self.__mpg,
                             optimizer=self.__optimizer, shared_sub_trees=all_shared_subtrees)
        else:
            return None
