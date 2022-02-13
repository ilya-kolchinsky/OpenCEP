import random
from copy import deepcopy
from typing import List
from itertools import combinations

from base.Pattern import Pattern
from base.PatternStructure import NegationOperator


class MultiPatternGraph:
    """
    Data structure that is capable of efficiently retrieving information about the mutual
    sub patterns between a list of patterns.
    This is a compact representation of the graph, according to Appendix A in the article:
    https://assaf.net.technion.ac.il/files/2019/03/Real-Time-Multi-Pattern-Detection-over-Event-Streams.pdf
    """
    def __init__(self, patterns: List[Pattern]):
        self.__patterns_list = patterns
        self.__pattern_to_maximal_common_sub_patterns = None  # dict of Pattern -> set of max common sub patterns
        self.__maximal_sub_pattern_to_patterns = None  # dict of max common sub pattern -> set of patterns
        self.__build_graph()  # build the dicts above

    @property
    def patterns(self):
        return self.__patterns_list

    def __build_graph(self):
        """
        Implements the Multi Pattern Graph for the local search algorithm.
        Creates a mapping between distinct maximal sub-patterns and the sets of patterns containing them.
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

    def get_random_max_pattern_and_peers(self, neighborhood: int):
        """
        Choose randomly a pattern, and then choose a random max sub pattern of it in the graph.
        From all the x patterns that share this sub pattern, choose randomly min(neighborhood,x) patterns.
        Return a tuple of (max sub pattern, chosen_patterns)
        """
        random_pattern = random.choice(self.__patterns_list)
        random_max_sub_pattern_set = self.__pattern_to_maximal_common_sub_patterns.get(random_pattern)
        if not random_max_sub_pattern_set:
            return None
        random_max_sub_pattern = random.choice(list(random_max_sub_pattern_set))
        containing_patterns = self.__maximal_sub_pattern_to_patterns.get(random_max_sub_pattern)
        chosen_patterns = random.sample(containing_patterns, min(neighborhood, len(containing_patterns)))
        return random_max_sub_pattern, chosen_patterns

    def __get_maximal_common_sub_patterns(self, pattern_a: Pattern, pattern_b: Pattern) -> List[Pattern]:
        """
        Return the maximal common subpattern between pattern_a and pattern_b.
        """
        # Avoid sharing negative structures
        for pattern in [pattern_a, pattern_b]:
            if pattern.negative_structure is not None or isinstance(pattern.full_structure, NegationOperator):
                return []

        if pattern_a == pattern_b:
            return [deepcopy(pattern_a)]

        events_intersection = set(pattern_a.get_primitive_event_names()) & set(pattern_b.get_primitive_event_names())

        conditions_a = pattern_a.condition.get_condition_projection(events_intersection)
        conditions_b = pattern_b.condition.get_condition_projection(events_intersection)

        cond_intersection = conditions_a

        if conditions_a is None or conditions_b is None:
            return []

        if conditions_a != conditions_b:
            cond_intersection = conditions_a.get_conditions_intersection(conditions_b)
            if cond_intersection is None:
                return []
            events_intersection = cond_intersection.get_event_names()

        # Reducing Structure a and Structure b according to the events intersection
        structure_a = pattern_a.full_structure.get_structure_projection(events_intersection)
        structure_b = pattern_b.full_structure.get_structure_projection(events_intersection)

        if structure_a != structure_b or (None in [structure_a, structure_b]):
            # TODO: Current limitation - The algorithm does not create intersection between the patterns structures
            #  (it only checks for equality, otherwise returns that there is no common subpattern)
            return []

        window = min(pattern_a.window, pattern_b.window)
        if pattern_a.confidence is None or pattern_b.confidence is None:
            confidence = None
        else:
            confidence = min(pattern_a.confidence, pattern_b.confidence)

        maximal = Pattern(pattern_structure=structure_a, pattern_matching_condition=cond_intersection,
                          time_window=window, consumption_policy=pattern_a.consumption_policy,
                          confidence=confidence, statistics=None)
        # Creates statistics dict that contains only data of the max common subpattern
        max_pattern_statistics = pattern_a.create_modified_statistics(pattern_a.statistics, maximal)
        maximal.set_statistics(max_pattern_statistics)
        return [maximal]
