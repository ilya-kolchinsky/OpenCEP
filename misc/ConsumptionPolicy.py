from enum import Enum
from typing import List


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


DEFAULT_PRIMARY_SELECTION_STRATEGY = SelectionStrategies.MATCH_ANY
DEFAULT_SECONDARY_SELECTION_STRATEGY = SelectionStrategies.MATCH_SINGLE


class ConsumptionPolicy:
    """
    The ConsumptionPolicy object contains the policies that filter certain partial matches:
    - a SingleTypes object that contains the Types that will be unique in the result of patrial matches
    - a set of "event names" that indicate that the event will come strictly after the previous event
    - a "event name" of the specific point in the sequence from which: prohibiting any pattern matching while a single partial match is active
    """
    def __init__(self,
                 primary_selection_strategy: SelectionStrategies = None,
                 secondary_selection_strategy: SelectionStrategies = None,
                 single: str or List[str] = None,
                 contiguous: List[str] or List[List[str]] = None,
                 freeze: str or List[str] = None):
        # initialize selection strategy
        self.single_event_strategy = None
        self.single_types = None
        self.__init_selection_strategy(primary_selection_strategy, secondary_selection_strategy, single)

        # initialize contiguity constraints
        if contiguous is not None and len(contiguous) > 0 and type(contiguous[0]) == str:
            # syntactic sugar - provide a list instead of a list containing one list
            self.contiguous_names = [contiguous]
        else:
            self.contiguous_names = contiguous

        # initialize "freeze" event types
        if freeze is not None and type(freeze) == str:
            # syntactic sugar - provide a string instead of a list containing one string
            self.freeze_names = [freeze]
        else:
            self.freeze_names = freeze

    def __init_selection_strategy(self,
                                  primary_selection_strategy: SelectionStrategies,
                                  secondary_selection_strategy: SelectionStrategies,
                                  single: str or List[str]):
        """
        Initializes the selection strategy settings and performs basic sanity checks.
        """
        if primary_selection_strategy is None:
            primary_selection_strategy = DEFAULT_PRIMARY_SELECTION_STRATEGY
        if secondary_selection_strategy is None:
            secondary_selection_strategy = DEFAULT_SECONDARY_SELECTION_STRATEGY

        if primary_selection_strategy != SelectionStrategies.MATCH_ANY:
            if single is not None or secondary_selection_strategy is not None:
                raise Exception("Secondary selection strategy can only be applied when the primary is MATCH_ANY")
            self.single_event_strategy = primary_selection_strategy
            # to be initialized to hold all event types
            self.single_types = None
            return

        # primary_selection_strategy == SelectionStrategies.MATCH_ANY
        if secondary_selection_strategy == SelectionStrategies.MATCH_ANY:
            raise Exception("Illegal value for the secondary selection strategy")
        if single is None:
            # the secondary selection strategy mechanism is disabled
            self.single_event_strategy = None
            self.single_types = None
            return

        # secondary selection strategy is one of the two "single match" options, and the list of types is specified
        self.single_event_strategy = secondary_selection_strategy
        # syntactic sugar - provide a string instead of a list containing one string
        self.single_types = [single] if type(single) == str else single

    def should_register_event_type_as_single(self, is_root: bool, event_type: str = None):
        """
        Returns True if the given parameters should activate the "single match only" policy on a tree node and
        False otherwise.
        """
        if self.single_event_strategy is None:
            return False
        if self.single_event_strategy == SelectionStrategies.MATCH_ANY:
            raise Exception("Illegal value for single event strategy")
        if self.single_event_strategy == SelectionStrategies.MATCH_SINGLE:
            return is_root and (event_type is None or event_type in self.single_types)
        # self.single_event_strategy == SelectionStrategies.MATCH_NEXT
        return event_type in self.single_types
