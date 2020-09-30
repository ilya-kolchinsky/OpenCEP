from CEP import CEP
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes
from plugin.twitter.TwitterDataFormatter import TWEET_TYPE
from plugin.twitter.TwitterInputStream import TweetsStreamSessionInput
from datetime import timedelta
from base.Formula import EqFormula, IdentifierTerm, NotEqFormula, CompositeAnd, NaryFormula
from base.PatternStructure import SeqOperator, QItem
from base.Pattern import Pattern


def run_twitter_sanity_check():
    """
    This basic test invokes a simple pattern looking for two tweets that retweeted the same tweet.
    It might help finding users with common interests.
    PATTERN SEQ(Tweet a, Tweet b)
    WHERE a.Retweeted_Status_Id != None AND a.ID != b.ID AND a.Retweeted_Status_Id == b.Retweeted_Status_Id
    """
    print("Started")
    get_retweeted_status_function = lambda x: x["retweeted_status"] if "retweeted_status" in x else None
    pattern_retweet = Pattern(
        SeqOperator([QItem(TWEET_TYPE, "a"), QItem(TWEET_TYPE, "b")]),
        CompositeAnd([
            NotEqFormula(IdentifierTerm("a", lambda x: x["id"]), IdentifierTerm("b", lambda x: x["id"])),
            NaryFormula(IdentifierTerm("a", get_retweeted_status_function), lambda x: x != None),
            EqFormula(IdentifierTerm("a", get_retweeted_status_function),
            IdentifierTerm("b", get_retweeted_status_function))
        ]),
        timedelta(minutes=30)
    )

    streaming = TweetsStreamSessionInput()
    stream_queue = streaming.get_stream_queue(['corona'])

    cep = CEP([pattern_retweet],
              EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE, None)
    try:
        running_time = cep.run(stream_queue, is_async=True, file_path="output.txt", time_limit=5)
        print("Test twitterSanityCheck result: Succeeded, Time Passed: %s" % (running_time,))
    finally:
        streaming.disconnect()


if __name__ == "__main__":
    run_twitter_sanity_check()
