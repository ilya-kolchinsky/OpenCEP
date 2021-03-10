from datetime import datetime
from base.DataFormatter import DataFormatter, EventTypeClassifier
import json

TWEET_MANDATORY_FIELDS = ["id", "created_at", "text", "truncated", "in_reply_to_status_id", "in_reply_to_user_id",
                          "in_reply_to_screen_name", "user", "is_quote_status", "retweet_count", "favorite_count",
                          "favorited", "retweeted", "filter_level", "lang"]
TWEET_OPTIONAL_FIELDS = ["quoted_status_id", "quote_count", "reply_count"]
TWEET_OPTIONAL_DICT_FIELDS = {"place": "full_name", "retweeted_status": "id"}

TWEET_EVENT_TIMESTAMP_KEY = "created_at"


class DummyTwitterEventTypeClassifier(EventTypeClassifier):
    """
    Assigns a single dummy event type to all events
    """
    TWEET_TYPE = "Tweet"

    def get_event_type(self, event_payload: dict):
        return self.TWEET_TYPE


class TweetDataFormatter(DataFormatter):
    """
    A data formatter implementation for the JSON-based data arriving via Twitter API.
    """
    def __init__(self, event_type_classifier: EventTypeClassifier = DummyTwitterEventTypeClassifier()):
        super().__init__(event_type_classifier)

    def parse_event(self, raw_data: str):
        """
        Parses a tweet JSON file into an event.
        To conserve memory, only a selection of the most important fields presented in the tweet are included.
        """
        json_version = json.loads(raw_data)
        tweet_payload_dict = {key: json_version[key] for key in TWEET_MANDATORY_FIELDS}
        tweet_payload_dict.update({key: json_version[key]
                                   for key in TWEET_OPTIONAL_FIELDS if key in json_version})
        tweet_payload_dict.update({primary_key: json_version[primary_key][secondary_key]
                                   for (primary_key, secondary_key) in TWEET_OPTIONAL_DICT_FIELDS.items()
                                   if primary_key in json_version and json_version[primary_key] is not None})
        return tweet_payload_dict

    def get_event_timestamp(self, event_payload: dict):
        """
        The timestamps in Twitter are formatted as follows: Wed Oct 10 20:19:24 +0000 2018
        """
        timestamp_str = str(event_payload[TWEET_EVENT_TIMESTAMP_KEY])
        return datetime.strptime(timestamp_str, '%a %b %d %H:%M:%S +0000 %Y')
