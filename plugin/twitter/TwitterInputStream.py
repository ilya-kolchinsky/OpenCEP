from stream.FileStream import InputStream
import tweepy
import plugin.twitter.TwitterCredentials
import json
import time


class TwitterInputStream(InputStream, tweepy.StreamListener):
    """
    Reads the objects from a Twitter session established using Twitter API.
    """
    def __init__(self, search_words_list: list, time_limit=None):
        super().__init__()

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

        if time_limit is not None:
            self.__start_time = time.time()
            self.__time_limit = time_limit
        else:
            self.__time_limit = None

        self.__tweet_stream = tweepy.Stream(self.__api.auth, listener=self)
        self.__tweet_stream.filter(track=search_words_list, is_async=True)

    def close(self):
        super().close()
        self.__tweet_stream.disconnect()

    def on_status(self, status):
        raw_data = json.dumps(status._json)
        self._stream.put(raw_data)
        if self.__time_limit is not None:
            if time.time() - self.__start_time > self.__time_limit:
                return False
