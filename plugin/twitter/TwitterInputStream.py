from base.Event import Event
from misc.IOUtils import Stream
from plugin.twitter.Tweets import MetatweetDataFormatter
import tweepy
import plugin.twitter.TwitterCredentials
import json
import time

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
        self.__auth = tweepy.OAuthHandler(plugin.twitter.TwitterCredentials.api_key,
                                          plugin.twitter.TwitterCredentials.api_secret_key)
        self.__auth.set_access_token(plugin.twitter.TwitterCredentials.access_token,
                                     plugin.twitter.TwitterCredentials.api_token_secret)
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
        self.__tweet_stream.filter(track=search_words_list, is_async=True)
        return self.__my_stream_listener.get_tweets_stream()

    def disconnect(self):
        return self.__tweet_stream.disconnect()
