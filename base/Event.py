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
    HIDDEN_ATTRIBUTE_NAMES = [INDEX_ATTRIBUTE_NAME]

    def __init__(self, raw_data: str, data_formatter: DataFormatter):
        self.payload = data_formatter.parse_event(raw_data)
        self.type = data_formatter.get_event_type(self.payload)
        self.timestamp = data_formatter.get_event_timestamp(self.payload)
        self.payload[Event.INDEX_ATTRIBUTE_NAME] = Event.counter
        self.probability = data_formatter.get_probability(self.payload)
        if self.probability is not None and (self.probability < 0.0 or self.probability > 1.0):
            raise Exception("Invalid value for probability:%s" % (self.probability,))
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
            actual_value = "'%s'" % (value,) if isinstance(value, str) else value
            curr_str = "'%s': %s" % (key, actual_value)
            result = curr_str if result == "" else ", ".join([result, curr_str])
        result = "{%s}" % (result,)
        return result
