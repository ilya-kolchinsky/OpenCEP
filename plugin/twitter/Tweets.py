from datetime import datetime
from base.DataFormatter import DataFormatter
import json


def month_to_num(month):
    """
    Returns the numerical value of a month given as a string of 3 letters (For example: Mar -> 3)
    Returns -1 if non of the months is an input
    """
    switcher = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12}
    return switcher.get(month, -1)


METATWEET_21_COLUMN_KEYS = [
    "ID",  # id
    "Date",  # created_at
    "Text",  # text
    "Is_truncated",  # truncated
    "If Reply: Orig Status Id",  # in_reply_to_status_id
    "If_Reply: Orig User Id",  # in_reply_to_user_id
    "If_Reply: Orig User Name",  # in_reply_to_screen_name
    "User Data",  # user
    "Tweet Location",  # place["full name"]
    "Is_Quoted",  # is_quote_status  - meaning this tweet quotes another tweet
    "Quoted Status Id",  # quoted_status_id
    "Retweeted_Status_Id",  # checks if "retweeted_status" exists: if yes it has the original tweet ID, else it has None
    "Quote Count",  # quote_count - Available only with the Premium and Enterprise tier products. None otherwise
    "Reply Count",  # reply_count - Available only with the Premium and Enterprise tier products. None otherwise
    "Retweet Count",  # retweet_count
    "Favorite Count",  # favorite_count
    "Is_Self_Liked",  # favorited
    "Is_Self_Retweeted",  # retweeted
    "Filter Level",  # filter_level
    "Lang",  # lang
    "General"  # defines the tweet as an event. For now every tweet receives the value 1
]

METATWEET_EVENT_TYPE_KEY = "General"
METATWEET_EVENT_TIMESTAMP_KEY = "Date"


class MetatweetDataFormatter(DataFormatter):
    """
    A data formatter implementation for a stock event stream, where each event is given as a string in metastock 7
    format.
    """

    def parse_event(self, raw_data: str):
        """
        Parses a metastock 5 formatted string into an event.
        """
        json_version = json.loads(raw_data)  # Turning data into a json version

        event_attributes = []
        event_attributes.append(json_version["id"])

        # Handling Date - Begin
        temp_date = json_version["created_at"]
        temp_month = month_to_num(str(temp_date[4:7]))
        if temp_month < 10:
            temp_month_str = str(0) + str(temp_month)
        else:
            temp_month_str = str(temp_month)
        event_attributes.append(
            temp_date[26:30] + temp_month_str + temp_date[8:10] + temp_date[11:13] + temp_date[14:16])
        # Handling Date - End

        event_attributes.append(json_version["text"])
        event_attributes.append(json_version["truncated"])
        event_attributes.append(json_version["in_reply_to_status_id"])
        event_attributes.append(json_version["in_reply_to_user_id"])
        event_attributes.append(json_version["in_reply_to_screen_name"])
        event_attributes.append(json_version["user"])
        if json_version["place"] is not None:
            event_attributes.append(json_version["place"]["full_name"])
        else:
            event_attributes.append(None)

        event_attributes.append(json_version["is_quote_status"])

        if "quoted_status_id" in json_version:
            event_attributes.append(json_version["quoted_status_id"])
        else:
            event_attributes.append(None)

        if "retweeted_status" in json_version:
            event_attributes.append(json_version["retweeted_status"]["id"])
        else:
            event_attributes.append(None)

        if "quote_count" in json_version:
            event_attributes.append(json_version["quote_count"])
        else:
            event_attributes.append(None)

        if "reply_count" in json_version:
            event_attributes.append(json_version["reply_count"])
        else:
            event_attributes.append(None)

        event_attributes.append(json_version["retweet_count"])
        event_attributes.append(json_version["favorite_count"])
        event_attributes.append(json_version["favorited"])
        event_attributes.append(json_version["retweeted"])
        event_attributes.append(json_version["filter_level"])
        event_attributes.append(json_version["lang"])

        event_attributes.append(str(1))  # Handling General - For now always gets 1

        return dict(zip(METATWEET_21_COLUMN_KEYS, event_attributes))

    def get_event_type(self, event_payload: dict):
        """
        The type of a stock event is equal to the stock ticker (company name).
        """
        return event_payload[METATWEET_EVENT_TYPE_KEY]

    def get_event_timestamp(self, event_payload: dict):
        """
        The event timestamp is represented in metastock 7 using a YYYYMMDDhhmm format.
        """
        timestamp_str = str(event_payload[METATWEET_EVENT_TIMESTAMP_KEY])
        return datetime(year=int(timestamp_str[0:4]), month=int(timestamp_str[4:6]), day=int(timestamp_str[6:8]),
                        hour=int(timestamp_str[8:10]), minute=int(timestamp_str[10:12]))
