from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, SmallerThanEqCondition, SmallerThanCondition,NotEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, KleeneClosureOperator, NegationOperator
from base.Pattern import Pattern
from parallel.ParallelExecutionParameters import *
from condition.KCCondition import KCIndexCondition, KCValueCondition
from plan.multi.MultiPatternEvaluationParameters import *
from misc.ConsumptionPolicy import *
from misc.StatisticsTypes import *
from plan.TreePlanBuilderFactory import IterativeImprovementTreePlanBuilderParameters
from plan.LeftDeepTreeBuilders import *
from plan.BushyTreeBuilders import *
import random



# Algorithm 1
def oneArgumentsearchTestAlgorithm1(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 135),
        timedelta(minutes=120)
    )
    runTest("one", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=6, key = "Opening Price"))

def amazonSpecificPatternSearchTestAlgoritm1(createTestFile=False):
    """
    This pattern is looking for an amazon stock in peak price of 73.
    """
    amazonSpecificPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a")),
        EqCondition(Variable("a", lambda x: x["Peak Price"]), 73),
        timedelta(minutes=120)
    )
    runTest('amazonSpecific', [amazonSpecificPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=6, key="Opening Price"))


def fbNegPatternSearchTestAlgorithm1(createTestFile=False):

    fbNegPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("FB", "a")),
        NotEqCondition(Variable("a", lambda x: x["Opening Price"]), 16),
        timedelta(minutes=120)
    )
    runTest('fbNegOpeningPrice', [fbNegPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=6, key="Opening Price"))


def fbEqualToApple1PatternSearchTestAlgorithm1(createTestFile=False):

    fbAndAaplPattern = Pattern(
        AndOperator(PrimitiveEventStructure("FB", "f"), PrimitiveEventStructure("AAPL", "a")),
        AndCondition(
            EqCondition(Variable("a", lambda x: x["Opening Price"]), Variable("f", lambda x: x["Opening Price"])),
            EqCondition(Variable("a", lambda x: x["Opening Price"]), 16),
        ),
    timedelta(minutes=9)
    )
    runTest('fbEqualToApple', [fbAndAaplPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=6, key="Opening Price"))


def fbEqualToApple2PatternSearchTestAlgorithm1(createTestFile=False):

    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("FB", "f"), PrimitiveEventStructure("AAPL", "a")),
        AndCondition(
            EqCondition(Variable("a", lambda x: x["Opening Price"]), Variable("f", lambda x: x["Opening Price"])),
            SimpleCondition(Variable("a", lambda x: x["Peak Price"]),
                            relation_op=lambda x: x <= 100)
        ),
        timedelta(minutes=9)
    )
    runTest('fbEqualToApple2', [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=6, key="Opening Price"))

def appleOpenToCloseTestAlgoritm1(createTestFile=False):

    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        AndCondition(
        EqCondition(Variable("a", lambda x: x["Opening Price"]), Variable("a", lambda y: y["Close Price"])),
        NotEqCondition(Variable("a", lambda x: x["Peak Price"]), Variable("a", lambda y: y["Lowest Price"])),
        ),
        timedelta(minutes=9)
    )
    runTest('appleOpenToClose', [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=6, key="Opening Price"))


def applePeakToOpenTestAlgoritm1(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        AndCondition(
            NotEqCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("a", lambda y: y["Lowest Price"])),
            EqCondition(Variable("a", lambda x: x["Peak Price"]),
                           Variable("a", lambda y: y["Opening Price"])),
        ),
        timedelta(minutes=9)
    )
    runTest('applePeakToOpen', [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=6, key="Opening Price"))


def KCgoogleTestAlgorithm1():
    pattern = Pattern(
        KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a")),
        SimpleCondition(Variable("a", lambda x: x["Lowest Price"]), relation_op=lambda x: x == 530),
        timedelta(minutes=3)
    )
    runTest("KCgoogle", [pattern])


def KCequalsPatternSearchTestAlgorithm1(createTestFile=False):

    pattern = Pattern(
        KleeneClosureOperator(
            SeqOperator(PrimitiveEventStructure("FB", "f"), PrimitiveEventStructure("AAPL", "a")),
        ),
        AndCondition(
            EqCondition(Variable("a", lambda x: x["Opening Price"]), Variable("f", lambda x: x["Opening Price"])),
            EqCondition(Variable("a", lambda x: x["Opening Price"]), 16)
        ),
        timedelta(minutes=9)
    )
    runTest('KCequals', [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=6, key="Opening Price"))



def multyPatternAlgorithm1(createTestFile=False):
    """
    THE test finds 2 patterns of match:
        1. AAPL stock whom Openning Price equals 135 and all the
        2. sequential Amazon and than Google stocks whom Opening Price equals

    """
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        EqCondition(Variable("a", lambda x: x["Opening Price"]), 135),
        timedelta(minutes=120)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("GOOG", "b")),
        EqCondition(Variable("a", lambda x: x["Opening Price"]),
                             Variable("b", lambda x: x["Opening Price"]))
        ,
        timedelta(minutes=3)

    )
    runMultiTest("multyPatternAlgorithm1", [pattern1, pattern2], createTestFile,eventStream=nasdaqEventStreamEquals,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM1, num_threads=7,
                                                                 key="Opening Price"))




# Algorithm 2
def oneArgumentsearchTestAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 135),
        timedelta(minutes=120)
    )
    runTest("one", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))

def simplePatternSearchTestAlgorithm2(createTestFile=False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > b.OpeningPrice
        AND b.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("b", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y)
        ),
        timedelta(minutes=5)
    )
    runTest("simple", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def googleAscendPatternSearchTestAlgorithm2(createTestFile=False):
    """
    This pattern is looking for a short ascend in the Google peak prices.
    PATTERN SEQ(GoogleStockPriceUpdate a, GoogleStockPriceUpdate b, GoogleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    WITHIN 3 minutes
    """
    googleAscendPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
                BinaryCondition(Variable("a", lambda x: x["Peak Price"]),
                                Variable("b", lambda x: x["Peak Price"]),
                                relation_op=lambda x, y: x < y),
                BinaryCondition(Variable("b", lambda x: x["Peak Price"]),
                                Variable("c", lambda x: x["Peak Price"]),
                                relation_op=lambda x, y: x < y)
        ),
        timedelta(minutes=3)
    )
    runTest('googleAscend', [googleAscendPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def amazonInstablePatternSearchTestAlgorithm2(createTestFile=False):
    """
    This pattern is looking for an in-stable day for Amazon.
    PATTERN SEQ(AmazonStockPriceUpdate x1, AmazonStockPriceUpdate x2, AmazonStockPriceUpdate x3)
    WHERE x1.LowestPrice <= 75 AND x2.PeakPrice >= 78 AND x3.LowestPrice <= x1.LowestPrice
    WITHIN 1 day
    """
    amazonInstablePattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "x1"), PrimitiveEventStructure("AMZN", "x2"), PrimitiveEventStructure("AMZN", "x3")),
        AndCondition(
                SmallerThanEqCondition(Variable("x1", lambda x: x["Lowest Price"]), 75),
                GreaterThanEqCondition(Variable("x2", lambda x: x["Peak Price"]), 78),
                BinaryCondition(Variable("x3", lambda x: x["Lowest Price"]),
                                Variable("x1", lambda x: x["Lowest Price"]),
                                relation_op=lambda x, y: x <= y)
        ),
        timedelta(days=1)
    )
    runTest('amazonInstable', [amazonInstablePattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def msftDrivRacePatternSearchTestAlgorithm2(createTestFile=False):
    """
    This pattern is looking for a race between driv and microsoft in ten minutes
    PATTERN SEQ(MicrosoftStockPriceUpdate a, DrivStockPriceUpdate b, MicrosoftStockPriceUpdate c, DrivStockPriceUpdate d, MicrosoftStockPriceUpdate e)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < d.PeakPrice AND d.PeakPrice < e.PeakPrice
    WITHIN 10 minutes
    """
    msftDrivRacePattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"),
                    PrimitiveEventStructure("MSFT", "e")),
        AndCondition(
                BinaryCondition(Variable("a", lambda x: x["Peak Price"]),
                                Variable("b", lambda x: x["Peak Price"]),
                                relation_op=lambda x, y: x < y),
                BinaryCondition(Variable("b", lambda x: x["Peak Price"]),
                                Variable("c", lambda x: x["Peak Price"]),
                                relation_op=lambda x, y: x < y),
                BinaryCondition(Variable("c", lambda x: x["Peak Price"]),
                                Variable("d", lambda x: x["Peak Price"]),
                                relation_op=lambda x, y: x < y),
                BinaryCondition(Variable("d", lambda x: x["Peak Price"]),
                                Variable("e", lambda x: x["Peak Price"]),
                                relation_op=lambda x, y: x < y)
        ),
        timedelta(minutes=10)
    )
    runTest('msftDrivRace', [msftDrivRacePattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def googleIncreasePatternSearchTestAlgorithm2(createTestFile=False):
    """
    This Pattern is looking for a 1% increase in the google stock in a half-hour.
    PATTERN SEQ(GoogleStockPriceUpdate a, GoogleStockPriceUpdate b)
    WHERE b.PeakPrice >= 1.01 * a.PeakPrice
    WITHIN 30 minutes
    """
    googleIncreasePattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b")),
        BinaryCondition(Variable("b", lambda x: x["Peak Price"]),
                        Variable("a", lambda x: x["Peak Price"]),
                        relation_op=lambda x, y: x >= y*1.01),
        timedelta(minutes=30)
    )
    runTest('googleIncrease', [googleIncreasePattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def amazonSpecificPatternSearchTestAlgorithm2(createTestFile=False):
    """
    This pattern is looking for an amazon stock in peak price of 73.
    """
    amazonSpecificPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a")),
        EqCondition(Variable("a", lambda x: x["Peak Price"]), 73),
        timedelta(minutes=120)
    )
    runTest('amazonSpecific', [amazonSpecificPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def googleAmazonLowPatternSearchTestAlgorithm2(createTestFile=False):
    """
    This pattern is looking for low prices of Amazon and Google at the same minute.
    PATTERN AND(AmazonStockPriceUpdate a, GoogleStockPriceUpdate g)
    WHERE a.PeakPrice <= 73 AND g.PeakPrice <= 525
    WITHIN 1 minute
    """
    googleAmazonLowPattern = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("GOOG", "g")),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Peak Price"]),
                            relation_op=lambda x: x <= 73),
            SimpleCondition(Variable("g", lambda x: x["Peak Price"]),
                            relation_op=lambda x: x <= 525)
        ),
        timedelta(minutes=1)
    )
    runTest('googleAmazonLow', [googleAmazonLowPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def nonsensePatternSearchTestAlgorithm2(createTestFile=False):
    """
    This pattern is looking for something that does not make sense.
    PATTERN AND(AmazonStockPriceUpdate a, AvidStockPriceUpdate b, AppleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < a.PeakPrice
    """
    nonsensePattern = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AVID", "b"), PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Peak Price"]),
                            Variable("b", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y),
            BinaryCondition(Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y),
            BinaryCondition(Variable("c", lambda x: x["Peak Price"]),
                            Variable("a", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y)
        ),
        timedelta(minutes=1)
    )
    runTest('nonsense', [nonsensePattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def hierarchyPatternSearchTestAlgorithm2(createTestFile=False):
    """
    The following pattern is looking for Amazon < Apple < Google cases in one minute windows.
    PATTERN AND(AmazonStockPriceUpdate a, AppleStockPriceUpdate b, GoogleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    WITHIN 1 minute
    """
    hierarchyPattern = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Peak Price"]),
                            Variable("b", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y),
            BinaryCondition(Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y)
        ),
        timedelta(minutes=1)
    )
    runTest('hierarchy', [hierarchyPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))

def duplicateEventTypeTestAlgorithm2(createTestFile=False):
    """
    PATTERN SEQ(AmazonStockPriceUpdate a, AmazonStockPriceUpdate b, AmazonStockPriceUpdate c)
    WHERE   TRUE
    WITHIN 10 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AMZN", "c")),
        TrueCondition(),
        timedelta(minutes=10)
    )
    runTest("duplicateEventType", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads= 6))

def structuralTest1Algorithm2():
    """
    Seq([a, KC(And([KC(d), KC(Seq([e, f]))]))])
    """
    structural_test_pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"),
                    KleeneClosureOperator(
                         AndOperator(PrimitiveEventStructure("GOOG", "b"),
                                     KleeneClosureOperator(PrimitiveEventStructure("GOOG", "c"),
                                                            min_size=1, max_size=5),
                                     KleeneClosureOperator(SeqOperator(PrimitiveEventStructure("GOOG", "d"), PrimitiveEventStructure("GOOG", "e")),
                                                            min_size=1, max_size=5)
                                     ),
                         min_size=1, max_size=5,
                     )),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
            SimpleCondition(Variable("b", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135)
        ),
        timedelta(minutes=3)
    )

    expected_result = ('Seq', 'a', ('KC', ('And', ('And', 'b', ('KC', 'c')), ('KC', ('Seq', 'd', 'e')))))
    runStructuralTest('structuralTest1', [structural_test_pattern], expected_result,
                      parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                      data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def structuralTest2Algorithm2():
    """
    KC(a)
    """
    structural_test_pattern = Pattern(
        KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
        timedelta(minutes=3)
    )
    expected_result = ('KC', 'a')
    runStructuralTest('structuralTest2', [structural_test_pattern], expected_result,
                      parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                      data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def structuralTest3Algorithm2():
    """
    Seq([a, KC(b)])
    """
    structural_test_pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("GOOG", "a"), KleeneClosureOperator(PrimitiveEventStructure("GOOG", "b"))
        ),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('KC', 'b'))
    runStructuralTest('structuralTest3', [structural_test_pattern], expected_result,
                      parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                      data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def structuralTest4Algorithm2():
    """
    And([KC(a), b])
    """
    structural_test_pattern = Pattern(
        AndOperator(
            KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a")), PrimitiveEventStructure("GOOG", "b")
        ),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
        timedelta(minutes=3)
    )
    expected_result = ('And', ('KC', 'a'), 'b')
    runStructuralTest('structuralTest4', [structural_test_pattern], expected_result,
                      parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                      data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def structuralTest5Algorithm2():
    """
    KC(Seq([KC(a), KC(b)]))
    """
    structural_test_pattern = Pattern(
        KleeneClosureOperator(
            SeqOperator(
                KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"), min_size=3, max_size=5),
                KleeneClosureOperator(PrimitiveEventStructure("GOOG", "b"))
            ), min_size=1, max_size=3
        ),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
        timedelta(minutes=3)
    )
    expected_result = ('KC', ('Seq', ('KC', 'a'), ('KC', 'b')))
    runStructuralTest('structuralTest5', [structural_test_pattern], expected_result,
                      parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                      data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def structuralTest6Algorithm2():
    """
    Seq([a, Seq([ b, And([c, d]), e])])
    """
    structural_test_pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("GOOG", "a"),
            SeqOperator(
                PrimitiveEventStructure("GOOG", "b"),
                AndOperator(
                    PrimitiveEventStructure("GOOG", "c"),
                    PrimitiveEventStructure("GOOG", "d")
                ),
                PrimitiveEventStructure("GOOG", "e")
            ),
        ),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('Seq', ('Seq', 'b', ('And', 'c', 'd')), 'e'))
    runStructuralTest('structuralTest6', [structural_test_pattern], expected_result,
                      parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                      data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def structuralTest7Algorithm2():
    """
    And([a, b, c, Seq([
                        d, KC(And([
                                e, KC(f), g
                              ]),
                        And([ KC(h), KC(Seq([ i, j])
                        ])
                   ]),
        k, l
    ])
    """
    structural_test_pattern = Pattern(
        AndOperator(
            PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"), PrimitiveEventStructure("GOOG", "c"),
            SeqOperator(
                PrimitiveEventStructure("GOOG", "d"),
                KleeneClosureOperator(
                    AndOperator(
                        PrimitiveEventStructure("GOOG", "e"), KleeneClosureOperator(PrimitiveEventStructure("GOOG", "f")), PrimitiveEventStructure("GOOG", "g")
                    )
                ), AndOperator(
                    KleeneClosureOperator(PrimitiveEventStructure("GOOG", "h")),
                    KleeneClosureOperator(
                        SeqOperator(
                            PrimitiveEventStructure("GOOG", "i"), PrimitiveEventStructure("GOOG", "j")
                        ),
                    ),
                ),
            ),
            PrimitiveEventStructure("GOOG", "k"), PrimitiveEventStructure("GOOG", "l")
        ),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
        timedelta(minutes=3)
    )
    expected_result = ('And', ('And', ('And', ('And', ('And', 'a', 'b'), 'c'),
                                       ('Seq', ('Seq', 'd', ('KC', ('And', ('And', 'e', ('KC', 'f')), 'g'))),
                                        ('And', ('KC', 'h'), ('KC', ('Seq', 'i', 'j'))))), 'k'), 'l')
    runStructuralTest('structuralTest7', [structural_test_pattern], expected_result,
                      parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                      data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def MinMax_0_TestKleeneClosureAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"), min_size=1, max_size=2)),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
        timedelta(minutes=5)
    )
    runTest("MinMax_0_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))

def MinMax_1_TestKleeneClosureAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
        timedelta(minutes=5)
    )
    runTest("MinMax_1_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))

def MinMax_2_TestKleeneClosureAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"), min_size=4, max_size=5)),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
        timedelta(minutes=5)
    )
    runTest("MinMax_2_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def KC_AND_IndexCondition_01_Algorithm2(createTestFile=False):
    """
    KC(And([a, b]))
    """
    pattern = Pattern(
        KleeneClosureOperator(
            AndOperator(
                PrimitiveEventStructure("GOOG", "a"),
                PrimitiveEventStructure("GOOG", "b")
            ), min_size=1, max_size=3
        ),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]), Variable("b", lambda x: x["Peak Price"])),
            KCIndexCondition(names={'a', 'b'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x < y,
                             first_index=0, second_index=2),
        ),
        timedelta(minutes=3)
    )
    runTest("KC_AND_IndexCondition_01_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))



def KC_AND_IndexCondition_02_Algorithm2(createTestFile=False):
    """
    KC(And([a, b]))
    """
    pattern = Pattern(
        KleeneClosureOperator(
            AndOperator(
                PrimitiveEventStructure("GOOG", "a"),
                PrimitiveEventStructure("GOOG", "b")
            ), min_size=1, max_size=3
        ),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]), Variable("b", lambda x: x["Peak Price"])),
            KCIndexCondition(names={'a', 'b'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x < y,
                             offset=2),
        ),
        timedelta(minutes=3)
    )
    runTest("KC_AND_IndexCondition_02_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def KC_AND_NegOffSet_01_Algorithm2(createTestFile=False):
    """
    KC(And([a, b, c]))
    """
    pattern = Pattern(
        KleeneClosureOperator(
            AndOperator(
                PrimitiveEventStructure("GOOG", "a"),
                PrimitiveEventStructure("GOOG", "b"),
                PrimitiveEventStructure("GOOG", "c")
            ), min_size=1, max_size=3
        ),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]), Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]), Variable("c", lambda x: x["Peak Price"])),
            KCIndexCondition(names={'a', 'b', 'c'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x < 1 + y,
                             offset=-1)
        ),
        timedelta(minutes=3)
    )
    runTest("KC_AND_NegOffSet_01_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def KC_AllValuesAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueCondition(names={'a'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x > y, value=530.5)
            ),
        timedelta(minutes=5)
    )
    runTest("KC_AllValues_01_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def KC_Specific_ValueAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueCondition(names={'a'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x > y, index=2, value=530.5)
            ),
        timedelta(minutes=5)
    )
    runTest("KC_Specific_Value_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))

def KC_MixedAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueCondition(names={'a'}, getattr_func=lambda x: x["Peak Price"],
                             relation_op=lambda x, y: x > y,
                             value=530.5),
            KCIndexCondition(names={'a'}, getattr_func=lambda x: x["Opening Price"],
                             relation_op=lambda x, y: x+0.5 < y,
                             offset=-1)
        ),
        timedelta(minutes=5)
    )
    runTest("KC_Mixed_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


currentPath = pathlib.Path(os.path.dirname(__file__))
absolutePath = str(currentPath.parent)
sys.path.append(absolutePath)

"""
Simple multi-pattern test with 2 patterns
"""
def leafIsRootAlgorithm2(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    runMultiTest("FirstMultiPattern", [pattern1, pattern2], createTestFile,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=6))

"""
multi-pattern test 2 completely distinct patterns
"""
def distinctPatternsAlgorithm2(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "x1"), PrimitiveEventStructure("AMZN", "x2"), PrimitiveEventStructure("AMZN", "x3")),
        AndCondition(
            SmallerThanEqCondition(Variable("x1", lambda x: x["Lowest Price"]), 75),
            GreaterThanEqCondition(Variable("x2", lambda x: x["Peak Price"]), 78),
            SmallerThanEqCondition(Variable("x3", lambda x: x["Lowest Price"]),
                                   Variable("x1", lambda x: x["Lowest Price"]))
        ),
        timedelta(days=1)
    )

    runMultiTest("BigMultiPattern", [pattern1, pattern2], createTestFile,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=6))

"""
multi-pattern test with 3 patterns and leaf sharing
"""
def threePatternsTestAlgorithm2(createTestFile = False):
    pattern1 = Pattern(
        AndOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                     PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=1)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"),
                    PrimitiveEventStructure("MSFT", "e")),
        AndCondition(
                SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                     Variable("b", lambda x: x["Peak Price"])),
                SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                     Variable("c", lambda x: x["Peak Price"])),
                SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                     Variable("d", lambda x: x["Peak Price"])),
                SmallerThanCondition(Variable("d", lambda x: x["Peak Price"]),
                                     Variable("e", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=10)
    )
    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    runMultiTest("ThreePatternTest", [pattern1, pattern2, pattern3], createTestFile,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=6))

"""
multi-pattern test checking case where output node is not a root
"""
def rootAndInnerAlgorithm2(createTestFile = False):
    #similar to leafIsRoot, but the time windows are different
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanEqCondition(Variable("a", lambda x: x["Peak Price"]), 135),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanEqCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )

    runMultiTest("RootAndInner", [pattern1, pattern2], createTestFile,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=6))

"""
multi-pattern test 2 identical patterns with different time stamp
"""
def samePatternDifferentTimeStampsAlgorithm2(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanEqCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanEqCondition(Variable("a", lambda x: x["Peak Price"]), 135),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=2)
    )

    runMultiTest("DifferentTimeStamp", [pattern1, pattern2], createTestFile,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=6))

"""
multi-pattern test sharing equivalent subtrees
"""
def onePatternIncludesOtherAlgorithm2(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"),
                     PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            GreaterThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b")),
        SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                             Variable("b", lambda x: x["Peak Price"]))
        ,
        timedelta(minutes=3)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                                                   TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True),
                                                                   MultiPatternEvaluationParameters(MultiPatternEvaluationApproaches.SUBTREES_UNION))
    runMultiTest("onePatternIncludesOther", [pattern1, pattern2], createTestFile, eval_mechanism_params,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=6))

"""
multi-pattern test multiple patterns share the same output node
"""
def samePatternSharingRootAlgorithm2(createTestFile = False):
    hierarchyPattern = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=1)
    )

    hierarchyPattern2 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=0.5)
    )

    hierarchyPattern3 = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=0.1)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
        TreeStorageParameters(sort_storage=False,
                              clean_up_interval=10,
                              prioritize_sorting_by_timestamp=True),
        MultiPatternEvaluationParameters(MultiPatternEvaluationApproaches.SUBTREES_UNION))

    runMultiTest('hierarchyMultiPattern', [hierarchyPattern, hierarchyPattern2, hierarchyPattern3], createTestFile, eval_mechanism_params,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=6))


"""
multi-pattern test sharing internal node between patterns
"""
def multipleParentsForInternalNodeAlgorithm2(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 500)
        ),
        timedelta(minutes=5)
    )

    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")),
            AndCondition(
                GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 530)
            ),
            timedelta(minutes=3)
    )

    pattern3 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("FB", "e")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("e", lambda x: x["Peak Price"]), 520)
        ),
        timedelta(minutes=5)
    )

    pattern4 = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"),
                    PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LI", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("c", lambda x: x["Peak Price"]), 100)
        ),
        timedelta(minutes=2)
    )

    eval_mechanism_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                  TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
        TreeStorageParameters(sort_storage=False,
                              clean_up_interval=10,
                              prioritize_sorting_by_timestamp=True),
        MultiPatternEvaluationParameters(MultiPatternEvaluationApproaches.SUBTREES_UNION))

    runMultiTest("multipleParentsForInternalNode", [pattern1, pattern2, pattern3, pattern4], createTestFile, eval_mechanism_params,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=6))

def simpleNotTestAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )

    runTest("simpleNot", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


# ON NASDAQ SHORT
def multipleNotInTheMiddleTestAlgorithm2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("LI", "d")), PrimitiveEventStructure("AMZN", "b"),
                     NegationOperator(PrimitiveEventStructure("FB", "e")), PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
                GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                     Variable("b", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"])),
                GreaterThanCondition(Variable("e", lambda x: x["Opening Price"]),
                                     Variable("a", lambda x: x["Opening Price"])),
                SmallerThanCondition(Variable("d", lambda x: x["Opening Price"]),
                                     Variable("c", lambda x: x["Opening Price"]))
            ),
        timedelta(minutes=4)
    )
    runTest("MultipleNotMiddle", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def singleType1PolicyPatternSearchTestAlgorithm2(createTestFile = False):
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
    runTest("singleType1Policy", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def singleType2PolicyPatternSearchTestAlgorithm2(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(single="AMZN", secondary_selection_strategy=SelectionStrategies.MATCH_SINGLE)
    )
    runTest("singleType2Policy", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def contiguousPolicyPatternSearchTestAlgorithm2(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(contiguous=["a", "b", "c"])
    )
    runTest("contiguousPolicySingleList", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def contiguousPolicy2PatternSearchTestAlgorithm2(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(contiguous=[["a", "b"], ["b", "c"]])
    )
    runTest("contiguousPolicyMultipleLists", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def freezePolicyPatternSearchTestAlgorithm2(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(freeze="a")
    )
    runTest("freezePolicy", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def freezePolicy2PatternSearchTestAlgorithm2(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(freeze="b")
    )
    runTest("freezePolicy2", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))

def sortedStorageTestAlgorithm2(createTestFile=False):
    pattern = Pattern(
        AndOperator(PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"), PrimitiveEventStructure("CBRL", "c")),
        AndCondition(
            GreaterThanCondition(
                Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"])
            ),
            GreaterThanCondition(
                Variable("b", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])
            ),
        ),
        timedelta(minutes=360),
    )
    storage_params = TreeStorageParameters(True, clean_up_interval=500)
    eval_params = TreeBasedEvaluationMechanismParameters(
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.tree_plan_params, storage_params
    )
    runTest("sortedStorageTest", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def sortedStorageBenchMarkTestAlgorithm2(createTestFile=False):
    pattern = Pattern(
        AndOperator(PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"),
                    PrimitiveEventStructure("CBRL", "c"), PrimitiveEventStructure("MSFT", "m")),
        AndCondition(
            GreaterThanEqCondition(
                Variable("b", lambda x: x["Lowest Price"]), Variable("a", lambda x: x["Lowest Price"])
            ),
            GreaterThanEqCondition(
                Variable("m", lambda x: x["Peak Price"]), Variable("c", lambda x: x["Peak Price"])
            ),
            GreaterThanEqCondition(
                Variable("m", lambda x: x["Lowest Price"]), Variable("b", lambda x: x["Lowest Price"])
            ),
        ),
        timedelta(minutes=360),
    )
    runBenchMark("sortedStorageBenchMark - unsorted storage", [pattern])
    storage_params = TreeStorageParameters(sort_storage=True, attributes_priorities={"a": 122, "b": 200, "c": 104, "m": 139})
    eval_params = TreeBasedEvaluationMechanismParameters(
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.tree_plan_params, storage_params
    )
    runBenchMark("sortedStorageBenchMark - sorted storage", [pattern], eval_mechanism_params=eval_params,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(num_threads=4))


