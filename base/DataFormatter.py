from abc import ABC


class DataFormatter(ABC):
    """
    An abstract class encapsulating the details regarding the input data format.
    A dedicated DataFormatter is expected to be implemented for each new type of input / dataset used by the system.
    """
    def parse_event(self, raw_data: str):
        """
        Transforms a raw data object representing a single event into a dictionary of objects, each corresponding
        to a single event attribute.
        """
        raise NotImplementedError()

    def get_event_type(self, event_payload: dict):
        """
        Deduces and returns the type of the event specified by the given payload.
        """
        raise NotImplementedError()

    def get_event_timestamp(self, event_payload: dict):
        """
        Deduces and returns the timestamp of the event specified by the given payload.
        """
        raise NotImplementedError()
