from CEP import CEP
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes, \
    IterativeImprovementEvaluationMechanismParameters
from misc.IOUtils import TweetsStreamSessionInput
from misc.Tweets import MetatweetDataFormatter
from datetime import timedelta
from base.Formula import EqFormula, IdentifierTerm, AtomicTerm, AndFormula, NotEqFormula
from base.PatternStructure import AndOperator, SeqOperator, QItem
from base.Pattern import Pattern

"""
    This pattern is looking for two tweets (we are more interested in the users who published those tweets) that 
    retweeted the same tweet.
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
    timedelta.max
)

streaming = TweetsStreamSessionInput()
stream_queue = streaming.get_stream_queue(['corona'])

cep = CEP([pattern_retweet],
          EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE, None)
cep.run(stream_queue, is_async=True, file_path="output.txt")
