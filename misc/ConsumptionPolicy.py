from typing import List

from misc import DefaultConfig
from misc.SelectionStrategies import SelectionStrategies


class ConsumptionPolicy:
    """
    This class provides the consumption/selection policies for event detection:
    - a selection strategy (match an event with the next event, with a single event only, or with any event);
    - if a limiting selection strategy is selected (not "match with any"), an optional list of event types
    for which this strategy will be valid - the rest will still be handled with multiple matches allowed;
    - a list of event name sequences for which strict contiguity in the input must be enforced;
    - a list of event names whose appearance must freeze creation of new partial matches until they are either matched
    or expired.
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
            primary_selection_strategy = DefaultConfig.PRIMARY_SELECTION_STRATEGY
        if secondary_selection_strategy is None:
            secondary_selection_strategy = DefaultConfig.SECONDARY_SELECTION_STRATEGY

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
