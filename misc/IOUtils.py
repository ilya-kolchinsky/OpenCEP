from base.DataFormatter import DataFormatter
from misc.Tweets import MetatweetDataFormatter
from base.Event import Event
from queue import Queue
import tweepy
import misc.TweeterApiTokensSecrets
import json
import time


class Stream:
    """
    Represents a generic stream of objects.
    """

    def __init__(self):
        self.__stream = Queue()

    def __next__(self):
        next_item = self.__stream.get(block=True)  # Blocking get
        if next_item is None:
            raise StopIteration()
        return next_item

    def __iter__(self):
        return self

    def add_item(self, item: object):
        self.__stream.put(item)

    def close(self):
        self.__stream.put(None)

    def duplicate(self):
        ret = Stream()
        ret.__stream.queue = self.__stream.queue.copy()
        return ret

    def get_item(self):
        return self.__next__()

    def count(self):
        return self.__stream.qsize()

    def first(self):
        return self.__stream.queue[0]

    def last(self):
        x = self.__stream.queue[-1]
        if x is None:  # if stream is closed last is None. We need the one before None.
            x = self.__stream.queue[-2]
        return x


def file_input(file_path: str, data_formatter: DataFormatter) -> Stream:
    """
    Receives a file and returns a stream of events.
    "filepath": the path to the file that is to be read.
    The file will be parsed as so:
    * Each line will be a different event
    * Each line will be split on "," and the resulting array will be stored in an "Event",
      and the keys are determined from the given list "KeyMap".
    """
    with open(file_path, "r") as f:
        content = f.readlines()
    events = Stream()
    for line in content:
        events.add_item(Event(line, data_formatter))
    events.close()
    return events


def file_output(matches: list, output_file_name: str = 'matches.txt'):
    """
    Writes output matches to a file in the subfolder "Matches".
    It supports any iterable as output matches.
    """
    with open("test/Matches/" + output_file_name, 'w') as f:
        for match in matches:
            for event in match.events:
                f.write("%s\n" % event.payload)
            f.write("\n")


class MyStreamListener(tweepy.StreamListener):
    """
    A StreamListener to Tweeter API, inherited from Tweepy StreamListener class.
    """

    def __init__(self, time_limit=None):
        super().__init__()
        if time_limit is not None:
            self.__start_time = time.time()
            self.__time_limit = time_limit
        else:
            self.__time_limit = None

        self.__tweets_stream = Stream()
        self.__data_formatter = MetatweetDataFormatter()

    def on_status(self, status):
        raw_data = json.dumps(status._json)
        event = Event(raw_data, self.__data_formatter)
        self.__tweets_stream.add_item(event)
        if self.__time_limit is not None:
            if time.time() - self.__start_time > self.__time_limit:
                return False

    def get_tweets_stream(self):
        return self.__tweets_stream


class TweetsStreamSessionInput:
    """"
    The main class to create a session with Tweeter.
    The __init__ function create a new authentication with Tweeter
    """

    def __init__(self, time_limit=None):
        self.__auth = tweepy.OAuthHandler(misc.TweeterApiTokensSecrets.api_key,
                                          misc.TweeterApiTokensSecrets.api_secret_key)
        self.__auth.set_access_token(misc.TweeterApiTokensSecrets.access_token,
                                     misc.TweeterApiTokensSecrets.api_token_secret)
        self.__api = tweepy.API(self.__auth)
        try:
            self.__api.verify_credentials()
            print("Authentication success.")
        except tweepy.TweepError:
            print("Authentication error.")
            print("Your credentials are probably wrong.")
            exit(0)
        self.__my_stream_listener = MyStreamListener(time_limit)
        self.__tweet_stream = tweepy.Stream(self.__api.auth, listener=self.__my_stream_listener)

    """
    This function get a list with the words/expression you want to search by using the
    stream, and return a Stream queue 
    """
    def get_stream_queue(self, search_words_list: list):
        stream = tweepy.Stream(self.__api.auth, listener=self.__my_stream_listener)
        stream.filter(track=search_words_list, is_async=True)
        return self.__my_stream_listener.get_tweets_stream()
