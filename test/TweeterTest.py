from CEP import CEP
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes
from plugin.twitter.TwitterInputStream import TweetsStreamSessionInput
from datetime import timedelta
from base.Formula import EqFormula, IdentifierTerm, AtomicTerm, AndFormula, NotEqFormula
from base.PatternStructure import SeqOperator, QItem
from base.Pattern import Pattern


def run_tweeter_sanity_check():
    """
    This basic test invokes a simple pattern looking for two tweets that retweeted the same tweet.
    It might help finding users with common interests.
    PATTERN SEQ(Tweet a, Tweet b)
    WHERE a.Retweeted_Status_Id != None AND a.ID != b.ID AND a.Retweeted_Status_Id == b.Retweeted_Status_Id
    """
    pattern_retweet = Pattern(
        SeqOperator([QItem('1', "a"), QItem('1', "b")]),
        AndFormula(NotEqFormula(IdentifierTerm("a", lambda x: x["ID"]), IdentifierTerm("b", lambda x: x["ID"])),
                   AndFormula(NotEqFormula(IdentifierTerm("a", lambda x: x["Retweeted_Status_Id"]), AtomicTerm(None)),
                   EqFormula(IdentifierTerm("a", lambda x: x["Retweeted_Status_Id"]),
                             IdentifierTerm("b", lambda x: x["Retweeted_Status_Id"])))),
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