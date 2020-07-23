from enum import Enum
from typing import List

Mechanisms = Enum('Mechanisms', 'type1 type2')
class SingleTypes:
    """
    A SingleTypes object that contains the Types that will be unique in the result of patrial matches:
    - a set of "event names" that indicate that the event will come strictly after the previous event
    - a mechanism type:
        type1 - only attempt to match the event with the next corresponding event
        type2 - create multiple partial matches containing the event, but only allow a single full match to be returned by the tree root
    """
    def __init__(self, single_types: set = None, mechanism: Mechanisms = Mechanisms.type1):
        self.single_types = single_types
        self.mechanism = mechanism

class ConsumptionPolicies:
    """
    The ConsumptionPolicies object contains the policies that filter certain partial matches:
    - a SingleTypes object that contains the Types that will be unique in the result of patrial matches
    - a set of "event names" that indicate that the event will come strictly after the previous event
    - a "event name" of the specific point in the sequence from which: prohibiting any pattern matching while a single partial match is active
    """
    def __init__(self,
                 single: SingleTypes = None,
                 contiguous: List[str] or List[List[str]] = None,
                 freeze: str or List[str] = None):
        self.single = single
        if contiguous is not None and len(contiguous) > 0 and type(contiguous[0]) == str:
            # syntactic sugar - provide a list instead of a list containing one list
            self.contiguous = [contiguous]
        else:
            self.contiguous = contiguous
        if freeze is not None and type(freeze) == str:
            # syntactic sugar - provide a string instead of a list containing one string
            self.freeze = [freeze]
        else:
            self.freeze = freeze

    def should_register_event_type_as_single(self, is_root: bool, event_type: str = None):
        """
        Returns True if the given parameters should activate the "single match only" policy on a tree node and
        False otherwise.
        """
        if self.single is None:
            return False
        if self.single.mechanism == Mechanisms.type2:
            return is_root and (event_type is None or event_type in self.single.single_types)
        return event_type in self.single.single_types
