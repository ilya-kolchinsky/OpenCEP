from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, \
    SmallerThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern


def oneArgumentsearchTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                          test_name = "one"):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 135),
        timedelta(minutes=120)
    )
    runTest(test_name, [pattern], createTestFile, eval_mechanism_params)


def simplePatternSearchTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                            test_name = "simple"):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > b.OpeningPrice
        AND b.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AVID", "c")),
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
    runTest(test_name, [pattern], createTestFile, eval_mechanism_params)


def googleAscendPatternSearchTest(createTestFile=False,
                                  eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = "googleAscend"):
    """
    This pattern is looking for a short ascend in the Google peak prices.
    PATTERN SEQ(GoogleStockPriceUpdate a, GoogleStockPriceUpdate b, GoogleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    WITHIN 3 minutes
    """
    googleAscendPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
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
    runTest(test_name, [googleAscendPattern], createTestFile, eval_mechanism_params)


def amazonInstablePatternSearchTest(createTestFile=False,
                                    eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = "amazonInstable"):
    """
    This pattern is looking for an in-stable day for Amazon.
    PATTERN SEQ(AmazonStockPriceUpdate x1, AmazonStockPriceUpdate x2, AmazonStockPriceUpdate x3)
    WHERE x1.LowestPrice <= 75 AND x2.PeakPrice >= 78 AND x3.LowestPrice <= x1.LowestPrice
    WITHIN 1 day
    """
    amazonInstablePattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "x1"), PrimitiveEventStructure("AMZN", "x2"),
                    PrimitiveEventStructure("AMZN", "x3")),
        AndCondition(
            SmallerThanEqCondition(Variable("x1", lambda x: x["Lowest Price"]), 75),
            GreaterThanEqCondition(Variable("x2", lambda x: x["Peak Price"]), 78),
            BinaryCondition(Variable("x3", lambda x: x["Lowest Price"]),
                            Variable("x1", lambda x: x["Lowest Price"]),
                            relation_op=lambda x, y: x <= y)
        ),
        timedelta(days=1)
    )
    runTest(test_name, [amazonInstablePattern], createTestFile, eval_mechanism_params)


def msftDrivRacePatternSearchTest(createTestFile=False,
                                  eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = "msftDrivRace"):
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
    runTest(test_name, [msftDrivRacePattern], createTestFile, eval_mechanism_params)


def googleIncreasePatternSearchTest(createTestFile=False,
                                    eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = "googleIncrease"):
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
                        relation_op=lambda x, y: x >= y * 1.01),
        timedelta(minutes=30)
    )
    runTest(test_name, [googleIncreasePattern], createTestFile, eval_mechanism_params)


def amazonSpecificPatternSearchTest(createTestFile=False,
                                    eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = "amazonSpecific"):
    """
    This pattern is looking for an amazon stock in peak price of 73.
    """
    amazonSpecificPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a")),
        EqCondition(Variable("a", lambda x: x["Peak Price"]), 73),
        timedelta(minutes=120)
    )
    runTest(test_name, [amazonSpecificPattern], createTestFile, eval_mechanism_params)


def googleAmazonLowPatternSearchTest(createTestFile=False,
                                     eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                                     test_name = "googleAmazonLow"):
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
    runTest(test_name, [googleAmazonLowPattern], createTestFile, eval_mechanism_params)


def nonsensePatternSearchTest(createTestFile=False,
                              eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                              test_name = "nonsense"):
    """
    This pattern is looking for something that does not make sense.
    PATTERN AND(AmazonStockPriceUpdate a, AvidStockPriceUpdate b, AppleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < a.PeakPrice
    """
    nonsensePattern = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AVID", "b"),
                    PrimitiveEventStructure("AAPL", "c")),
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
    runTest(test_name, [nonsensePattern], createTestFile, eval_mechanism_params)


def hierarchyPatternSearchTest(createTestFile=False,
                               eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                               test_name = "hierarchy"):
    """
    The following pattern is looking for Amazon < Apple < Google cases in one minute windows.
    PATTERN AND(AmazonStockPriceUpdate a, AppleStockPriceUpdate b, GoogleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    WITHIN 1 minute
    """
    hierarchyPattern = Pattern(
        AndOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
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
    runTest(test_name, [hierarchyPattern], createTestFile, eval_mechanism_params)


def duplicateEventTypeTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                           test_name = "duplicateEventType"):
    """
    PATTERN SEQ(AmazonStockPriceUpdate a, AmazonStockPriceUpdate b, AmazonStockPriceUpdate c)
    WHERE   TRUE
    WITHIN 10 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AMZN", "c")),
        TrueCondition(),
        timedelta(minutes=10)
    )
    runTest(test_name, [pattern], createTestFile, eventStream=nasdaqEventStreamTiny,
            eval_mechanism_params=eval_mechanism_params)
