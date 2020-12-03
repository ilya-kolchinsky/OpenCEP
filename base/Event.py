from typing import Optional
from base.DataFormatter import DataFormatter


class Event:
    """
    This class represents a single primitive event received from an input stream. It may contain arbitrary attributes
    of arbitrary types. The only requirement is that event type and timestamp of occurrence must be derivable from these
    attributes using an appropriate data formatter.
    """

    # used in order to assign a serial number to each event that enters the system
    counter = 0

    INDEX_ATTRIBUTE_NAME = "InternalIndexAttributeName"
    PROBABILITY_ATTRIBUTE_NAME = "Probability"
    HIDDEN_ATTRIBUTE_NAMES = [INDEX_ATTRIBUTE_NAME, PROBABILITY_ATTRIBUTE_NAME]

    def __init__(self, raw_data: str, data_formatter: DataFormatter):
        self.payload = data_formatter.parse_event(raw_data)
        self.type = data_formatter.get_event_type(self.payload)
        self.timestamp = data_formatter.get_event_timestamp(self.payload)
        self.payload[Event.INDEX_ATTRIBUTE_NAME] = Event.counter
        self.probability: Optional[float] = self.payload.get(Event.PROBABILITY_ATTRIBUTE_NAME, None)
        Event.counter += 1

    def __eq__(self, other):
        return self.payload[Event.INDEX_ATTRIBUTE_NAME] == other.payload[Event.INDEX_ATTRIBUTE_NAME]

    def __hash__(self):
        return hash(self.payload[Event.INDEX_ATTRIBUTE_NAME])

    def __repr__(self):
        result = ""
        for key, value in self.payload.items():
            if key in self.HIDDEN_ATTRIBUTE_NAMES:
                continue
            actual_value = "'%s'" % (value,) if isinstance(value, str) else value  # FIXME:y should use `repr(value)` especially for `str`s
            curr_str = "'%s': %s" % (key, actual_value)  # FIXME:y  `repr(key)`
            result = curr_str if result == "" else ", ".join([result, curr_str])
        result = "{%s}" % (result,)
        return result
