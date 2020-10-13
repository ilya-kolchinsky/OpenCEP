"""
This class contains implementation of unary node used to connect different tree structures after split.
"""

from base.PatternMatch import PatternMatch

# TODO:
class PatternMatchWithUnarySource:
    def __init__(self, pattern_match: PatternMatch, index: int):
        self.pattern_match = pattern_match
        self.unary_index = index

    def get_pattern_match_timestamp(self):
        return self.pattern_match.last_timestamp

