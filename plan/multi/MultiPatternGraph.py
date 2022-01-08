import random
from copy import deepcopy
from typing import List, Dict

from condition.Condition import Condition
from misc.Utils import mapped_cache
from base.Pattern import Pattern
from itertools import product


class MultiPatternGraph:
    def __init__(self, patterns: List[Pattern]):
        self._patterns_list = patterns
        self._pattern_to_maximal_common_subpattern_list =\
            self.__make_pattern_to_maximal_common_subpattern_peers()  # Pattern -> list of peers
        self._maximal_subpattern_to_pattern_list =\
            self.__make_maximal_subpattern_to_patterns_dict()  # MP -> Constructing Patterns


    def __make_maximal_subpattern_to_patterns_dict(self):
        """
        creates mapping between distinct maximal sub-patterns and the sets of patterns containing them
        """
        mapping = dict()
        for i, pattern_a in enumerate(self._patterns_list):
            for j, pattern_b in enumerate(self._patterns_list):
                if i != j:
                    maximal_sub_patterns = self.__get_maximal_common_sub_pattern(pattern_a, pattern_b)
                    [mapping.setdefault(maximal_sub_pattern, set()).update([pattern_a, pattern_b])
                     for maximal_sub_pattern in maximal_sub_patterns]

        return dict(map(lambda k, v: (k, list(v)), mapping))
        # transfer it from pattern -> set[patterns] to pattern -> list[patterns]

    def __make_pattern_to_maximal_common_subpattern_peers(self):
        # TODO: check if needed to modify this(dict of dicts)
        pattern_to_peers = {
            pattern_a: [(self.__get_maximal_common_sub_pattern(pattern_a, pattern_b), pattern_b)
                        for pattern_b in self._patterns_list
                        if pattern_a != pattern_b]
            for pattern_a in self._patterns_list
        }
        return pattern_to_peers
        # pattern_a
        # pattern_a -> [(maximal(pattern_a, pattern_b), pattern_b) for pattern_b in pattern]

    def get_random_edge(self):
        max_sub_pattern, peers_list = random.choice(list(self._maximal_subpattern_to_pattern_list.items()))
        pattern_a, pattern_b = random.sample(peers_list, 2)
        return pattern_a, pattern_b, max_sub_pattern

    @mapped_cache(mapping=lambda x: frozenset(list(x)))  # TODO: Move to Utils/Pattern file
    def __get_maximal_common_sub_pattern(self, pattern_a: Pattern, pattern_b: Pattern) -> List[Pattern]:

        if pattern_a == pattern_b:
            return [deepcopy(pattern_a)]

        events_intersection = set(pattern_a.get_primitive_events()) & set(pattern_b.get_primitive_events())

        event_names = set(map(lambda event: event.name, events_intersection))

        conditions_a = pattern_a.condition.get_projection(event_names)
        conditions_b = pattern_b.condition.get_projection(event_names)

        cond_intersection = conditions_a

        if conditions_a != conditions_b:
            # Todo: reduce E_ij
            cond_intersection = None
            event_names = cond_intersection.get_event_names()

        # Reducing Si and Sj
        structure_a = pattern_a.full_structure.get_structure_projection(event_names)
        structure_b = pattern_b.full_structure.get_structure_projection(event_names)

        if structure_a != structure_b:
            # According to Ilya's assumption
            return []

        window = min(pattern_a.window, pattern_b.window)
        stat = {**pattern_a.statistics, **pattern_b.statistics}

        maximal = Pattern(pattern_structure=structure_a, pattern_matching_condition=cond_intersection,
                          time_window=window)
        # For now all the other parameters will not be present until discussed with Ilya
        return [maximal]

