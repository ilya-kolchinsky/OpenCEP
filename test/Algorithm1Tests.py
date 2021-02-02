from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, SmallerThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure,KleeneClosureOperator
from base.Pattern import Pattern
from parallel.ParallelExecutionParameters import *
from condition.KCCondition import KCIndexCondition, KCValueCondition





def EqualOver2Events(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a") ,PrimitiveEventStructure("AMZN", "b")),
        EqCondition(Variable("a", lambda x: x["Peak Price"]), Variable("b", lambda x: x["Peak Price"])),
        timedelta(minutes=5)
    )
    runTest("EqualOver2Events", [pattern], createTestFile,eventStream=nasdaq_equals,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(data_parallel_mode=DataParallelExecutionModes.ALGORITHM1,num_threads=6,
                                                                 key="Opening Price")
                                                  )


def singleType1PolicyPatternSearchTest(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(single="AMZN", secondary_selection_strategy=SelectionStrategies.MATCH_NEXT)
    )
    runTest("singleType1Policy", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)


def amazonTomerTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a"),PrimitiveEventStructure("GOOG", "b"),PrimitiveEventStructure("CSCO", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Lowest Price"]), Variable("b", lambda x: x["Lowest Price"])),
            EqCondition(Variable("b", lambda x: x["Lowest Price"]), Variable("c", lambda x: x["Lowest Price"]))
        ),
        timedelta(minutes=120)
    )

    runTest("amazonTomerTest", [pattern], createTestFile)

def amazonTomerTest(createTestFile=False):

    amazonSpecificPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a"),PrimitiveEventStructure("GOOG", "b"),PrimitiveEventStructure("CSCO", "c")),
        AndCondition(
            EqCondition(Variable("a", lambda x: x["Lowest Price"]), Variable("b", lambda x: x["Lowest Price"])),
            EqCondition(Variable("b", lambda x: x["Lowest Price"]), Variable("c", lambda x: x["Lowest Price"]))
            ),
        timedelta(minutes=120)
    )
    runTest('amazonTomerTest', [amazonSpecificPattern], createTestFile)



def amazonTomerTest(createTestFile=False):
    amazonSpecificPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a"),PrimitiveEventStructure("GOOG", "b")),
        AndCondition(
            EqCondition(Variable("a", lambda x: x["Lowest Price"]), Variable("b", lambda x: x["Lowest Price"])),
            EqCondition(Variable("b", lambda x: x["Lowest Price"]), Variable("c", lambda x: x["Lowest Price"]))
            ),
        timedelta(minutes=120)
    )
    runTest('amazonTomerTest', [amazonSpecificPattern], createTestFile)
def EqualOver222Events(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator
            (AndOperator(PrimitiveEventStructure("AAPL", "a") ,PrimitiveEventStructure("AMZN", "b")), min_size=1, max_size=6)),
        EqCondition(Variable("a", lambda x: x["Peak Price"]), Variable("b", lambda x: x["Peak Price"])),
        timedelta(minutes=5)
    )
    runTest("amazonTomerTest", [pattern], createTestFile,eventStream=nasdaq_equals,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(data_parallel_mode=DataParallelExecutionModes.ALGORITHM1,num_threads=6,
                                                                 key="Opening Price"))

