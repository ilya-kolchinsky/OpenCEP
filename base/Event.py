'''from base.DataFormatter import DataFormatter


class Event:
    """
    This class represents a single primitive event received from an input stream. It may contain arbitrary attributes
    of arbitrary types. The only requirement is that event type and timestamp of occurrence must be derivable from these
    attributes using an appropriate data formatter.
    """
    def __init__(self, raw_data: str, data_formatter: DataFormatter):
        self.payload = data_formatter.parse_event(raw_data)
        self.event_type = data_formatter.get_event_type(self.payload)
        self.timestamp = data_formatter.get_event_timestamp(self.payload)'''


class Event:
    def __init__(self, payload, event_type, time):
        self.payload = payload
        self.event_type = event_type
        self.timestamp = time

    def __repr__(self):
        return "((event_type: {}), (payload: {}), (timestamp: {}))".format(
            self.event_type, self.payload, self.timestamp
        )
