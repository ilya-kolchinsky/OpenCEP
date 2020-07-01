from base.DataFormatter import DataFormatter


class Event:
    """
    This class represents a single primitive event received from an input stream. It may contain arbitrary attributes
    of arbitrary types. The only requirement is that event type and timestamp of occurrence must be derivable from these
    attributes using an appropriate data formatter.
    """

    eventIndex = 0

    def __init__(self, raw_data: str, data_formatter: DataFormatter):
        self.payload = data_formatter.parse_event(raw_data)
        if('Index' in self.payload.keys()):
            raise Exception('The key "Index" in the event payload is reserved')
        self.payload['Index'] = Event.eventIndex
        self.event_type = data_formatter.get_event_type(self.payload)
        self.timestamp = data_formatter.get_event_timestamp(self.payload)
        Event.eventIndex += 1
