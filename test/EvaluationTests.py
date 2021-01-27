from datetime import timedelta

from base.Pattern import Pattern
from base.PatternStructure import PrimitiveEventStructure, SeqOperator, AndOperator
from condition.BaseRelationCondition import SmallerThanEqCondition, GreaterThanEqCondition, EqCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import BinaryCondition, Variable, SimpleCondition
from test.NewTestsUtils import DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS, \
    DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS, \
    DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER, \
    DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER, \
    DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER, \
    DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER, \
    DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER, \
    DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER
from test.testUtils import runTest


def simple(eval_mechanism_params, createTestFile=False):
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

    runTest("simple", [pattern], createTestFile, eval_mechanism_params)


def simple_1():
    simple(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def simple_2():
    simple(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def simple_3():
    simple(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def simple_4():
    simple(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def simple_5():
    simple(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def simple_6():
    simple(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def simple_7():
    simple(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def simple_8():
    simple(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def googleAscendPatternSearchTest(eval_mechanism_params, createTestFile=False):
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

    runTest('googleAscend', [googleAscendPattern], createTestFile, eval_mechanism_params)


def googleAscendPatternSearchTest_1():
    googleAscendPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleAscendPatternSearchTest_2():
    googleAscendPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleAscendPatternSearchTest_3():
    googleAscendPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleAscendPatternSearchTest_4():
    googleAscendPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def googleAscendPatternSearchTest_5():
    googleAscendPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def googleAscendPatternSearchTest_6():
    googleAscendPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def googleAscendPatternSearchTest_7():
    googleAscendPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleAscendPatternSearchTest_8():
    googleAscendPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def amazonInstablePatternSearchTest(eval_mechanism_params, createTestFile=False):
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

    runTest('amazonInstable', [amazonInstablePattern], createTestFile, eval_mechanism_params)


def amazonInstablePatternSearchTest_1():
    amazonInstablePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def amazonInstablePatternSearchTest_2():
    amazonInstablePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def amazonInstablePatternSearchTest_3():
    amazonInstablePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def amazonInstablePatternSearchTest_4():
    amazonInstablePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def amazonInstablePatternSearchTest_5():
    amazonInstablePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def amazonInstablePatternSearchTest_6():
    amazonInstablePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def amazonInstablePatternSearchTest_7():
    amazonInstablePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def amazonInstablePatternSearchTest_8():
    amazonInstablePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def msftDrivRacePatternSearchTest(eval_mechanism_params, createTestFile=False):
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

    runTest('msftDrivRace', [msftDrivRacePattern], createTestFile, eval_mechanism_params)


def msftDrivRacePatternSearchTest_1():
    msftDrivRacePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def msftDrivRacePatternSearchTest_2():
    msftDrivRacePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def msftDrivRacePatternSearchTest_3():
    msftDrivRacePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def msftDrivRacePatternSearchTest_4():
    msftDrivRacePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def msftDrivRacePatternSearchTest_5():
    msftDrivRacePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def msftDrivRacePatternSearchTest_6():
    msftDrivRacePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def msftDrivRacePatternSearchTest_7():
    msftDrivRacePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def msftDrivRacePatternSearchTest_8():
    msftDrivRacePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def googleIncreasePatternSearchTest(eval_mechanism_params, createTestFile=False):
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

    runTest('googleIncrease', [googleIncreasePattern], createTestFile, eval_mechanism_params)


def googleIncreasePatternSearchTest_1():
    googleIncreasePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleIncreasePatternSearchTest_2():
    googleIncreasePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleIncreasePatternSearchTest_3():
    googleIncreasePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleIncreasePatternSearchTest_4():
    googleIncreasePatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def googleIncreasePatternSearchTest_5():
    googleIncreasePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def googleIncreasePatternSearchTest_6():
    googleIncreasePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def googleIncreasePatternSearchTest_7():
    googleIncreasePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleIncreasePatternSearchTest_8():
    googleIncreasePatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def amazonSpecificPatternSearchTest(eval_mechanism_params, createTestFile=False):
    """
    This pattern is looking for an amazon stock in peak price of 73.
    """
    amazonSpecificPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a")),
        EqCondition(Variable("a", lambda x: x["Peak Price"]), 73),
        timedelta(minutes=120)
    )

    runTest('amazonSpecific', [amazonSpecificPattern], createTestFile, eval_mechanism_params)


def amazonSpecificPatternSearchTest_1():
    amazonSpecificPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def amazonSpecificPatternSearchTest_2():
    amazonSpecificPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def amazonSpecificPatternSearchTest_3():
    amazonSpecificPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def amazonSpecificPatternSearchTest_4():
    amazonSpecificPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def amazonSpecificPatternSearchTest_5():
    amazonSpecificPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def amazonSpecificPatternSearchTest_6():
    amazonSpecificPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def amazonSpecificPatternSearchTest_7():
    amazonSpecificPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def amazonSpecificPatternSearchTest_8():
    amazonSpecificPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def googleAmazonLowPatternSearchTest(eval_mechanism_params, createTestFile=False):
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

    runTest('googleAmazonLow', [googleAmazonLowPattern], createTestFile, eval_mechanism_params)

def googleAmazonLowPatternSearchTest_1():
    googleAmazonLowPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleAmazonLowPatternSearchTest_2():
    googleAmazonLowPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleAmazonLowPatternSearchTest_3():
    googleAmazonLowPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleAmazonLowPatternSearchTest_4():
    googleAmazonLowPatternSearchTest(DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def googleAmazonLowPatternSearchTest_5():
    googleAmazonLowPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def googleAmazonLowPatternSearchTest_6():
    googleAmazonLowPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def googleAmazonLowPatternSearchTest_7():
    googleAmazonLowPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleAmazonLowPatternSearchTest_8():
    googleAmazonLowPatternSearchTest(DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)

