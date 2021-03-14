from datetime import datetime
from typing import Any, Dict, Optional

from base.DataFormatter import DataFormatter, EventTypeClassifier
from misc.Utils import str_to_number

METASTOCK_STOCK_TICKER_KEY = "Stock Ticker"
METASTOCK_EVENT_TIMESTAMP_KEY = "Date"
PROBABILITY_KEY = "Probability"

METASTOCK_7_COLUMN_KEYS = [
    METASTOCK_STOCK_TICKER_KEY,
    METASTOCK_EVENT_TIMESTAMP_KEY,
    "Opening Price",
    "Peak Price",
    "Lowest Price",
    "Close Price",
    "Volume"]

ADDITIONAL_OPTIONAL_KEYS = [PROBABILITY_KEY]


class MetastockByTickerEventTypeClassifier(EventTypeClassifier):
    """
    This type classifier assigns a dedicated event type to each stock ticker.
    """
    def get_event_type(self, event_payload: dict):
        """
        The type of a stock event is equal to the stock ticker (company name).
        """
        return event_payload[METASTOCK_STOCK_TICKER_KEY]


class MetastockDataFormatter(DataFormatter):
    """
    A data formatter implementation for a stock event stream, where each event is given as a string in metastock 7
    format.
    """
    def __init__(self, event_type_classifier: EventTypeClassifier = MetastockByTickerEventTypeClassifier()):
        super().__init__(event_type_classifier)

    def parse_event(self, raw_data: str):
        """
        Parses a metastock 7 formatted string into an event.
        """
        event_attributes = raw_data.replace("\n", "").split(",")
        return dict(zip(
            METASTOCK_7_COLUMN_KEYS + ADDITIONAL_OPTIONAL_KEYS,
            map(str_to_number, event_attributes)
        ))

    def get_event_timestamp(self, event_payload: dict):
        """
        The event timestamp is represented in metastock 7 using a YYYYMMDDhhmm format.
        """
        timestamp_str = str(event_payload[METASTOCK_EVENT_TIMESTAMP_KEY])
        return datetime(year=int(timestamp_str[0:4]), month=int(timestamp_str[4:6]), day=int(timestamp_str[6:8]),
                        hour=int(timestamp_str[8:10]), minute=int(timestamp_str[10:12]))

    def get_probability(self, event_payload: Dict[str, Any]) -> Optional[float]:
        return event_payload.get(PROBABILITY_KEY, None)
