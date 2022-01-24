import random
from copy import deepcopy
from typing import List
from itertools import combinations

from base.Pattern import Pattern


class MultiPatternGraph:
    def __init__(self, patterns: List[Pattern]):
        self.__patterns_list = patterns
        self.__pattern_to_maximal_common_sub_patterns = None  # dict of Pattern -> set of max common sub patterns
        self.__maximal_sub_pattern_to_patterns = None  # dict of max common sub pattern -> set of patterns
        self.__set_max_sub_patterns()  # prepare the dicts above

    @property
    def patterns(self):
        return self.__patterns_list

    def __set_max_sub_patterns(self):
        """
        creates mapping between distinct maximal sub-patterns and the sets of patterns containing them.
        Also, for each pattern save its max common sub patterns in a dict.
        """
        max_sub_pattern_to_patterns = dict()
        pattern_to_max_sub_patters = dict()
        # Iterate over all pairs of patterns and calculate max common sub patterns
        for pattern_a, pattern_b in combinations(self.__patterns_list, 2):
            maximal_sub_patterns = self.__get_maximal_common_sub_patterns(pattern_a, pattern_b)
            # Add the mapping max sub pattern -> [pattern_a, pattern_b]
            [max_sub_pattern_to_patterns.setdefault(maximal_sub_pattern, set()).update([pattern_a, pattern_b])
             for maximal_sub_pattern in maximal_sub_patterns]
            # Add the mapping pattern -> max sub patterns for pattern_a and pattern_b
            [pattern_to_max_sub_patters.setdefault(pattern, set()).update(maximal_sub_patterns)
             for pattern in [pattern_a, pattern_b]]

        self.__maximal_sub_pattern_to_patterns = max_sub_pattern_to_patterns
        self.__pattern_to_maximal_common_sub_patterns = pattern_to_max_sub_patters

    def get_random_max_pattern_and_peers(self, k: int):
        """
        Choose randomly a pattern, and then choose a random max sub pattern of it in the graph.
        From all the x patterns that share this sub pattern, choose randomly min(k,x) patterns.
        Return a tuple of (max sub pattern, chosen_patterns)
        """
        random_pattern = random.choice(self.__patterns_list)
        random_max_sub_pattern = self.__pattern_to_maximal_common_sub_patterns.get(random_pattern)
        containing_patterns = self.__maximal_sub_pattern_to_patterns.get(random_max_sub_pattern)
        chosen_patterns = random.sample(containing_patterns, min(k, len(containing_patterns)))
        return random_max_sub_pattern, chosen_patterns

    def __get_maximal_common_sub_patterns(self, pattern_a: Pattern, pattern_b: Pattern) -> List[Pattern]:
        """
        Return the maximal common subpattern between pattern_a and pattern_b.
        """
        if pattern_a == pattern_b:
            return [deepcopy(pattern_a)]

        events_intersection = set(pattern_a.get_primitive_event_names()) & set(pattern_b.get_primitive_event_names())

        conditions_a = pattern_a.condition.get_condition_projection(events_intersection)
        conditions_b = pattern_b.condition.get_condition_projection(events_intersection)

        cond_intersection = conditions_a

        if conditions_a != conditions_b:
            cond_intersection = conditions_a.intersection(conditions_b)
            if cond_intersection is None:
                return []
            events_intersection = cond_intersection.get_event_names()

        # Reducing Si and Sj
        structure_a = pattern_a.full_structure.get_structure_projection(events_intersection)
        structure_b = pattern_b.full_structure.get_structure_projection(events_intersection)

        if structure_a != structure_b:
            # According to Ilya's assumption
            return []

        window = min(pattern_a.window, pattern_b.window)
        confidence = min(pattern_a.confidence, pattern_b.confidence)

        maximal = Pattern(pattern_structure=structure_a, pattern_matching_condition=cond_intersection,
                          time_window=window, consumption_policy=pattern_a.consumption_policy,
                          confidence=confidence, statistics=pattern_a.statistics)
        return [maximal]
