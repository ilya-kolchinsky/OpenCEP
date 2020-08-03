from base.DataFormatter import DataFormatter


class Event:
    """
    This class represents a single primitive event received from an input stream. It may contain arbitrary attributes
    of arbitrary types. The only requirement is that event type and timestamp of occurrence must be derivable from these
    attributes using an appropriate data formatter.
    """

    # used in order to assign a serial number to each event that enters the system
    counter = 0

    # this is a temporal hack to support strict contiguity
    INDEX_ATTRIBUTE_NAME = "InternalIndexAttributeName"

    def __init__(self, raw_data: str, data_formatter: DataFormatter):
        self.payload = data_formatter.parse_event(raw_data)
        self.type = data_formatter.get_event_type(self.payload)
        self.timestamp = data_formatter.get_event_timestamp(self.payload)
        self.payload[Event.INDEX_ATTRIBUTE_NAME] = Event.counter
        Event.counter += 1

    def __repr__(self):
        return str(self.payload)
