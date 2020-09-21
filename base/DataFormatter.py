from abc import ABC


class EventTypeClassifier(ABC):
    """
    An abstract class encapsulating the way event types are defined and assigned to raw data items as they are
    converted into primitive events.
    This functionality is intentionally separated from DataFormatter class to enable multiple type hierachies for a
    given data format.
    """
    def get_event_type(self, event_payload: dict):
        """
        Deduces and returns the type of the event specified by the given payload.
        """
        raise NotImplementedError()


class DataFormatter(ABC):
    """
    An abstract class encapsulating the details regarding the input data format.
    A dedicated DataFormatter is expected to be implemented for each new type of input / dataset used by the system.
    """
    def __init__(self, event_type_classifier: EventTypeClassifier):
        self.__event_type_classifier = event_type_classifier

    def parse_event(self, raw_data: str):
        """
        Transforms a raw data object representing a single event into a dictionary of objects, each corresponding
        to a single event attribute.
        """
        raise NotImplementedError()

    def get_event_timestamp(self, event_payload: dict):
        """
        Deduces and returns the timestamp of the event specified by the given payload.
        """
        raise NotImplementedError()

    def get_event_type(self, event_payload: dict):
        """
        Deduces and returns the type of the event specified by the given payload.
        """
        return self.__event_type_classifier.get_event_type(event_payload)
