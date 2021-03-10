import os

from CEP import CEP
from stream.FileStream import FileOutputStream
from plugin.twitter.TwitterDataFormatter import DummyTwitterEventTypeClassifier, TweetDataFormatter
from plugin.twitter.TwitterInputStream import TwitterInputStream
from datetime import timedelta
from condition.Condition import Variable, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, NotEqCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern


def run_twitter_sanity_check():
    """
    This basic test invokes a simple pattern looking for two tweets that retweeted the same tweet.
    It might help finding users with common interests.
    PATTERN SEQ(Tweet a, Tweet b)
    WHERE a.Retweeted_Status_Id != None AND a.ID != b.ID AND a.Retweeted_Status_Id == b.Retweeted_Status_Id
    """
    get_retweeted_status_function = lambda x: x["retweeted_status"] if "retweeted_status" in x else None
    pattern_retweet = Pattern(
        SeqOperator(PrimitiveEventStructure(DummyTwitterEventTypeClassifier.TWEET_TYPE, "a"),
                    PrimitiveEventStructure(DummyTwitterEventTypeClassifier.TWEET_TYPE, "b")),
        AndCondition(NotEqCondition(Variable("a", lambda x: x["id"]), Variable("b", lambda x: x["id"])),
                     SimpleCondition(Variable("a", get_retweeted_status_function), relation_op=lambda x: x is not None),
                     EqCondition(Variable("a", get_retweeted_status_function),
                                 Variable("b", get_retweeted_status_function))),
        timedelta(minutes=30)
    )

    cep = CEP([pattern_retweet])
    event_stream = TwitterInputStream(['corona'])
    try:
        running_time = cep.run(event_stream, FileOutputStream(os.getcwd(), "output.txt", True), TweetDataFormatter())
        print("Test twitterSanityCheck result: Succeeded, Time Passed: %s" % (running_time,))
    finally:
        event_stream.close()


if __name__ == "__main__":
    run_twitter_sanity_check()
