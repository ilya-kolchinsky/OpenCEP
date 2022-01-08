import random
from copy import copy

from base.Pattern import Pattern
from plan.TreePlan import TreePlan, TreePlanNode
from plan.multi.MultiPatternGraph import MultiPatternGraph
from tree.MultiPatternTree import MultiPatternTree


# G = (V, E)

# V = {all possible MPTs}

# E = {}


class StateNode:
    def __init__(self, mpt: MultiPatternTree,
                 last_modified_node: TreePlanNode,
                 mpg: MultiPatternGraph,
                 last_used):
        self.__mpt = mpt
        self.__mpg = mpg
        self.__last_used = None
        pass
        # (pattern, pattern) ->

    def __eq__(self, other):
        return self.__mpt == other.mpt

    @property
    def mpt(self):
        return self.__mpt

    def get_cost(self):
        return self.__mpt.get_cost()

    def get_neighbors(self):
        while True:
            pattern_a, pattern_b, max_sub_pattern = self.__mpg.get_random_edge()
            events = set(max_sub_pattern.get_primitive_events())
            event_names = set(map(lambda event: event.name, events))

            rand_size = random.randint(0, len(event_names) + 1)
            filtered_events = random.sample(event_names, rand_size)
            filtered_events = set(filtered_events)
            patterns_set = frozenset([pattern_a, pattern_b])

            if filtered_events != self.__last_used.get(patterns_set):
                break

        new_last_used = {**self.__last_used, patterns_set: filtered_events}
        new_struct = max_sub_pattern.full_structure.get_structure_projection(event_names)
        new_cond = max_sub_pattern.condition.get_projection(event_names)
        Pattern(pattern_structure=new_struct,
                pattern_matching_condition=new_cond,
                time_window=max_sub_pattern.window)


        pass


# instead of calculating the common subpattern in each mpt, we'll create one in the initial node and based on this make all the dessicions