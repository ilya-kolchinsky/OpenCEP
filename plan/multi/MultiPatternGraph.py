from copy import deepcopy
from typing import List, Dict
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

        pattern_to_peers = dict()
        [pattern_to_peers.setdefault(pattern_a, []).append((self.__get_maximal_common_sub_pattern(pattern_a, pattern_b),
                                                            pattern_b.id))
         for pattern_a, pattern_b in product(self._patterns_list, self._patterns_list)]

        return pattern_to_peers
        # pattern_a
        # pattern_a -> [(maximal(pattern_a, pattern_b), pattern_b) for pattern_b in pattern]

    @mapped_cache(mapping=lambda x: frozenset(list(x)))  # TODO: Move to Utils/Pattern file
    def __get_maximal_common_sub_pattern(self, pattern_a: Pattern, pattern_b: Pattern) -> List[Pattern]:

        if pattern_b.id == pattern_a.id:
            return [deepcopy(pattern_a)]
        # TODO: ADD
        raise NotImplementedError()

