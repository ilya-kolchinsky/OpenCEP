from base.PatternMatch import PatternMatch


class PatternMatchWithUnarySource:
    def __init__(self, pattern_match: PatternMatch, index: int):
        self.pattern_match = pattern_match
        self.unary_index = index

    def get_pattern_match_timestamp(self):
        return self.pattern_match.last_timestamp  # check if this is the right parameter to compare them by
