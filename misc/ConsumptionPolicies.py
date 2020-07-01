from enum import Enum

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
    def __init__(self, single: SingleTypes = None, strict: set = None, skip: str = None):
        self.single = single
        self.strict = strict
        self.skip = skip