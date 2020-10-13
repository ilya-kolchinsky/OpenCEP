"""
This class is used to simplify "freeing" the matches blocked in ParallelUnaryNodes. It allows us for each pattern
matches to also keep its source, meaning the unary node it came from and needs to be "freed" from
"""

from base.PatternMatch import PatternMatch


class PatternMatchWithUnarySource:
    def __init__(self, pattern_match: PatternMatch, index: int):
        self.pattern_match = pattern_match
        # this represents the index of the unary node this match came from
        self.unary_index = index

    def get_pattern_match_timestamp(self):
        return self.pattern_match.last_timestamp

