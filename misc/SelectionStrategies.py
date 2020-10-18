from enum import Enum


class SelectionStrategies(Enum):
    """
    The selection strategies supported by the framework.
    MATCH_NEXT - for each event only a single attempt to match it against another event is performed;
    MATCH_SINGLE - each event is guaranteed to only be returned as a part of a single full match;
    MATCH_ANY - each event can participate in an arbitrary number of matches.
    """
    MATCH_NEXT = 0,
    MATCH_SINGLE = 1,
    MATCH_ANY = 2