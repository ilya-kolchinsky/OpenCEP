from test.testUtils import *
# from test.KC_tests import *

from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes, \
    IterativeImprovementEvaluationMechanismParameters, TreeBasedEvaluationMechanismParameters
from misc.ConsumptionPolicy import *
from evaluation.LeftDeepTreeBuilders import *
from evaluation.BushyTreeBuilders import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, MulTerm, \
    EqFormula, IdentifierTerm, MinusTerm, AtomicTerm, AndFormula, TrueFormula, NaryFormula, CompositeAnd, NaryOperationTerm
from base.PatternStructure import AndOperator, SeqOperator, QItem, NegationOperator
from base.Pattern import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters

try:
    from UnitTests.test_storage import run_storage_tests
except ImportError:
    from test.UnitTests.test_storage import run_storage_tests


def naryOneArgumentsearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a")]),
        # GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135), relation_op=lambda x, y: x > y),
        timedelta(minutes=120)
    )
    runTest("one", [pattern], createTestFile)


def narySimplePatternSearchTest(createTestFile=False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > b.OpeningPrice
        AND b.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]),
        CompositeAnd(
            [
            NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                        IdentifierTerm("b", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y: x > y),
            NaryFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                        IdentifierTerm("c", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y: x > y)
            ]
        ),
        timedelta(minutes=5)
    )
    runTest("simple", [pattern], createTestFile)


def naryGoogleAscendPatternSearchTest(createTestFile=False):
    """
    This pattern is looking for a short ascend in the Google peak prices.
    PATTERN SEQ(GoogleStockPriceUpdate a, GoogleStockPriceUpdate b, GoogleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    WITHIN 3 minutes
    """
    googleAscendPattern = Pattern(
        SeqOperator([QItem("GOOG", "a"), QItem("GOOG", "b"), QItem("GOOG", "c")]),
        CompositeAnd(
            [
                NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                            IdentifierTerm("b", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y),
                NaryFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                            IdentifierTerm("c", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y)
            ]
        ),
        timedelta(minutes=3)
    )
    runTest('googleAscend', [googleAscendPattern], createTestFile)


def naryAmazonInstablePatternSearchTest(createTestFile=False):
    """
    This pattern is looking for an in-stable day for Amazon.
    PATTERN SEQ(AmazonStockPriceUpdate x1, AmazonStockPriceUpdate x2, AmazonStockPriceUpdate x3)
    WHERE x1.LowestPrice <= 75 AND x2.PeakPrice >= 78 AND x3.LowestPrice <= x1.LowestPrice
    WITHIN 1 day
    """
    amazonInstablePattern = Pattern(
        SeqOperator([QItem("AMZN", "x1"), QItem("AMZN", "x2"), QItem("AMZN", "x3")]),
        CompositeAnd(
            [
                NaryFormula(IdentifierTerm("x1", lambda x: x["Lowest Price"]),
                            AtomicTerm(75),
                            relation_op=lambda x, y: x <= y),
                NaryFormula(IdentifierTerm("x2", lambda x: x["Peak Price"]),
                            AtomicTerm(78),
                            relation_op=lambda x, y: x >= y),
                NaryFormula(IdentifierTerm("x3", lambda x: x["Lowest Price"]),
                            IdentifierTerm("x1", lambda x: x["Lowest Price"]),
                            relation_op=lambda x, y: x <= y)
            ]
        ),
        timedelta(days=1)
    )
    runTest('amazonInstable', [amazonInstablePattern], createTestFile)


def naryMsftDrivRacePatternSearchTest(createTestFile=False):
    """
    This pattern is looking for a race between driv and microsoft in ten minutes
    PATTERN SEQ(MicrosoftStockPriceUpdate a, DrivStockPriceUpdate b, MicrosoftStockPriceUpdate c, DrivStockPriceUpdate d, MicrosoftStockPriceUpdate e)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < d.PeakPrice AND d.PeakPrice < e.PeakPrice
    WITHIN 10 minutes
    """
    msftDrivRacePattern = Pattern(
        SeqOperator(
            [QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("MSFT", "c"), QItem("DRIV", "d"), QItem("MSFT", "e")]),
        CompositeAnd(
            [
                NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                            IdentifierTerm("b", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y),
                NaryFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                            IdentifierTerm("c", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y),
                NaryFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                            IdentifierTerm("d", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y),
                NaryFormula(IdentifierTerm("d", lambda x: x["Peak Price"]),
                            IdentifierTerm("e", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y)
            ]
        ),
        timedelta(minutes=10)
    )
    runTest('msftDrivRace', [msftDrivRacePattern], createTestFile)


def naryGoogleIncreasePatternSearchTest(createTestFile=False):
    """
    This Pattern is looking for a 1% increase in the google stock in a half-hour.
    PATTERN SEQ(GoogleStockPriceUpdate a, GoogleStockPriceUpdate b)
    WHERE b.PeakPrice >= 1.01 * a.PeakPrice
    WITHIN 30 minutes
    """
    googleIncreasePattern = Pattern(
        SeqOperator([QItem("GOOG", "a"), QItem("GOOG", "b")]),
        NaryFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                    NaryOperationTerm(AtomicTerm(1.01),
                                       IdentifierTerm("a", lambda x: x["Peak Price"]),
                                       operator=lambda x, y: x * y),
                    relation_op=lambda x, y: x >= y),
        timedelta(minutes=30)
    )
    runTest('googleIncrease', [googleIncreasePattern], createTestFile)


def naryAmazonSpecificPatternSearchTest(createTestFile=False):
    """
    This pattern is looking for an amazon stock in peak price of 73.
    """
    amazonSpecificPattern = Pattern(
        SeqOperator([QItem("AMZN", "a")]),
        NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(73), relation_op=lambda x, y: x == y),
        timedelta(minutes=120)
    )
    runTest('amazonSpecific', [amazonSpecificPattern], createTestFile)


def naryGoogleAmazonLowPatternSearchTest(createTestFile=False):
    """
    This pattern is looking for low prices of Amazon and Google at the same minute.
    PATTERN AND(AmazonStockPriceUpdate a, GoogleStockPriceUpdate g)
    WHERE a.PeakPrice <= 73 AND g.PeakPrice <= 525
    WITHIN 1 minute
    """
    googleAmazonLowPattern = Pattern(
        AndOperator([QItem("AMZN", "a"), QItem("GOOG", "g")]),
        CompositeAnd([
            NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                        AtomicTerm(73),
                        relation_op=lambda x,y: x <= y),
            NaryFormula(IdentifierTerm("g", lambda x: x["Peak Price"]),
                        AtomicTerm(525),
                        relation_op=lambda x, y: x <= y)
        ]),
        timedelta(minutes=1)
    )
    runTest('googleAmazonLow', [googleAmazonLowPattern], createTestFile)


def naryNonsensePatternSearchTest(createTestFile=False):
    """
    This pattern is looking for something that does not make sense.
    PATTERN AND(AmazonStockPriceUpdate a, AvidStockPriceUpdate b, AppleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < a.PeakPrice
    """
    nonsensePattern = Pattern(
        AndOperator([QItem("AMZN", "a"), QItem("AVID", "b"), QItem("AAPL", "c")]),
        CompositeAnd([
            NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                        IdentifierTerm("b", lambda x: x["Peak Price"]),
                        relation_op=lambda x, y: x < y),
            NaryFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                        IdentifierTerm("c", lambda x: x["Peak Price"]),
                        relation_op=lambda x, y: x < y),
            NaryFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                        IdentifierTerm("a", lambda x: x["Peak Price"]),
                        relation_op=lambda x, y: x < y)
        ]),
        timedelta(minutes=1)
    )
    runTest('nonsense', [nonsensePattern], createTestFile)


def naryHierarchyPatternSearchTest(createTestFile=False):
    """
    The following pattern is looking for Amazon < Apple < Google cases in one minute windows.
    PATTERN AND(AmazonStockPriceUpdate a, AppleStockPriceUpdate b, GoogleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    WITHIN 1 minute
    """
    hierarchyPattern = Pattern(
        AndOperator([QItem("AMZN", "a"), QItem("AAPL", "b"), QItem("GOOG", "c")]),
        CompositeAnd([
            NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                        IdentifierTerm("b", lambda x: x["Peak Price"]),
                        relation_op=lambda x, y: x < y),
            NaryFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                        IdentifierTerm("c", lambda x: x["Peak Price"]),
                        relation_op=lambda x, y: x < y)
        ]),
        timedelta(minutes=1)
    )
    runTest('hierarchy', [hierarchyPattern], createTestFile)


def naryNonFrequencyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("LOCM", "c")]),
        NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                    IdentifierTerm("b", lambda x: x["Opening Price"]),
                    IdentifierTerm("c", lambda x: x["Opening Price"]),
                    relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    runTest("nonFrequency", [pattern], createTestFile)


def naryFrequencyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("LOCM", "c")]),
        NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                    IdentifierTerm("b", lambda x: x["Opening Price"]),
                    IdentifierTerm("c", lambda x: x["Opening Price"]),
                    relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 460, "AMZN": 442, "LOCM": 219})
    runTest("frequency", [pattern], createTestFile, EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)


def naryArrivalRatesPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("LOCM", "c")]),
        NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                    IdentifierTerm("b", lambda x: x["Opening Price"]),
                    IdentifierTerm("c", lambda x: x["Opening Price"]),
                    relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, [0.0159, 0.0153, 0.0076])
    runTest("arrivalRates", [pattern], createTestFile, EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)


def compositeNonFrequencyPatternSearch2Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("LOCM", "a"), QItem("AMZN", "b"), QItem("AAPL", "c")]),
        CompositeAnd([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("nonFrequency2", [pattern], createTestFile)


def compositeFrequencyPatternSearch2Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("LOCM", "a"), QItem("AMZN", "b"), QItem("AAPL", "c")]),
        CompositeAnd([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 2, "AMZN": 3, "LOCM": 1})
    runTest("frequency2", [pattern], createTestFile, EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)


def compositeGreedyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        CompositeAnd([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ]),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('greedy1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.GREEDY_LEFT_DEEP_TREE, events=nasdaqEventStream)


def naryiiRandomPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        CompositeAnd([
            NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                        IdentifierTerm("b", lambda x: x["Peak Price"]),
                        IdentifierTerm("c", lambda x: x["Peak Price"]),
                        relation_op=lambda x,y,z: x < y < z),
                SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                                   IdentifierTerm("d", lambda x: x["Peak Price"]))
        ]),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('iiRandom1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE,
            eval_mechanism_params=IterativeImprovementEvaluationMechanismParameters(
                20, IterativeImprovementType.SWAP_BASED, IterativeImprovementInitType.RANDOM),
            events=nasdaqEventStream)


def naryiiRandom2PatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                    IdentifierTerm("b", lambda x: x["Peak Price"]),
                    IdentifierTerm("c", lambda x: x["Peak Price"]),
                    IdentifierTerm("d", lambda x: x["Peak Price"]),
                    relation_op=lambda x, y, z, w: x < y < z < w),

        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('iiRandom2', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE,
            eval_mechanism_params=IterativeImprovementEvaluationMechanismParameters(
                20, IterativeImprovementType.CIRCLE_BASED, IterativeImprovementInitType.RANDOM),
            events=nasdaqEventStream)


def compositeiiGreedyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        CompositeAnd([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ]),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('iiGreedy1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE,
            eval_mechanism_params=IterativeImprovementEvaluationMechanismParameters(
                20, IterativeImprovementType.SWAP_BASED, IterativeImprovementInitType.GREEDY),
            events=nasdaqEventStream)


def compositeiiGreedy2PatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        CompositeAnd([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ]),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('iiGreedy2', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.LOCAL_SEARCH_LEFT_DEEP_TREE,
            eval_mechanism_params=IterativeImprovementEvaluationMechanismParameters(
                20, IterativeImprovementType.CIRCLE_BASED, IterativeImprovementInitType.GREEDY),
            events=nasdaqEventStream)


def naryDpLdPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        CompositeAnd([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            NaryFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                        IdentifierTerm("c", lambda x: x["Peak Price"]),
                        IdentifierTerm("d", lambda x: x["Peak Price"]),
                        relation_op=lambda x,y,z: x < y < z)
        ]),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('dpLd1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE, events=nasdaqEventStream)


def naryDpBPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        CompositeAnd([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            NaryFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                        IdentifierTerm("c", lambda x: x["Peak Price"]),
                        relation_op=lambda x,y: x < y),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ]),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('dpB1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE, events=nasdaqEventStream)


def compositeZStreamOrdPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        CompositeAnd([
            CompositeAnd([
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ]),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
            ]
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('zstream-ord1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.ORDERED_ZSTREAM_BUSHY_TREE, events=nasdaqEventStream)


def compositeZStreamPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        CompositeAnd([
            CompositeAnd([
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                CompositeAnd([
                    SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                       IdentifierTerm("c", lambda x: x["Peak Price"]))
                ])
            ]),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ]),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    runTest('zstream1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.ZSTREAM_BUSHY_TREE, events=nasdaqEventStream)


def compositeFrequencyTailoredPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("DRIV", "a"), QItem("MSFT", "b"), QItem("CBRL", "c")]),
        CompositeAnd([
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=360)
    )
    frequencyDict = {"MSFT": 256, "DRIV": 257, "CBRL": 1}
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, frequencyDict)
    runTest('frequencyTailored1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE, events=nasdaqEventStream)


def naryNonFrequencyTailoredPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("DRIV", "a"), QItem("MSFT", "b"), QItem("CBRL", "c")]),
        NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                    IdentifierTerm("b", lambda x: x["Opening Price"]),
                    IdentifierTerm("c", lambda x: x["Opening Price"]),
                    relation_op= lambda x,y,z: x > y > z),
        timedelta(minutes=360)
    )
    runTest('nonFrequencyTailored1', [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE, events=nasdaqEventStream)


def nonFrequencyPatternSearch3Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AAPL", "b"), QItem("AAPL", "c"), QItem("LOCM", "d")]),
        TrueFormula(),
        timedelta(minutes=5)
    )
    runTest("nonFrequency3", [pattern], createTestFile)


def frequencyPatternSearch3Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AAPL", "b"), QItem("AAPL", "c"), QItem("LOCM", "d")]),
        TrueFormula(),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 460, "LOCM": 219})
    runTest("frequency3", [pattern], createTestFile, EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)


def nonFrequencyPatternSearch4Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c"), QItem("LOCM", "d")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    runTest("nonFrequency4", [pattern], createTestFile)


def frequencyPatternSearch4Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c"), QItem("LOCM", "d")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AVID": 1, "LOCM": 2, "AAPL": 3, "AMZN": 4})
    runTest("frequency4", [pattern], createTestFile, EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)


def nonFrequencyPatternSearch5Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            [QItem("AAPL", "a1"), QItem("LOCM", "b1"), QItem("AAPL", "a2"), QItem("LOCM", "b2"), QItem("AAPL", "a3"),
             QItem("LOCM", "b3")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    runTest("nonFrequency5", [pattern], createTestFile)


def frequencyPatternSearch5Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            [QItem("AAPL", "a1"), QItem("LOCM", "b1"), QItem("AAPL", "a2"), QItem("LOCM", "b2"), QItem("AAPL", "a3"),
             QItem("LOCM", "b3")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"LOCM": 1, "AAPL": 2})  # {"AAPL": 460, "LOCM": 219}
    runTest("frequency5", [pattern], createTestFile, EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)


def frequencyPatternSearch6Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            [QItem("AAPL", "a1"), QItem("LOCM", "b1"), QItem("AAPL", "a2"), QItem("LOCM", "b2"), QItem("AAPL", "a3"),
             QItem("LOCM", "b3")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 1, "LOCM": 2})  # {"AAPL": 460, "LOCM": 219}
    runTest("frequency6", [pattern], createTestFile, EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)


runTest.over_all_time = 0

# basic functionality tests
naryOneArgumentsearchTest()
narySimplePatternSearchTest()
naryGoogleAscendPatternSearchTest()
naryAmazonInstablePatternSearchTest()
naryMsftDrivRacePatternSearchTest()
naryGoogleIncreasePatternSearchTest()
naryAmazonSpecificPatternSearchTest()
naryGoogleAmazonLowPatternSearchTest()
naryNonsensePatternSearchTest()
naryHierarchyPatternSearchTest()
naryNonFrequencyPatternSearchTest()
naryArrivalRatesPatternSearchTest()
naryFrequencyPatternSearchTest()
compositeNonFrequencyPatternSearch2Test()
compositeFrequencyPatternSearch2Test()
compositeGreedyPatternSearchTest()
naryiiRandomPatternSearchTest()
naryiiRandom2PatternSearchTest()
compositeiiGreedyPatternSearchTest()
compositeiiGreedy2PatternSearchTest()
compositeZStreamOrdPatternSearchTest()
compositeZStreamPatternSearchTest()
naryDpBPatternSearchTest()
naryDpLdPatternSearchTest()
naryNonFrequencyTailoredPatternSearchTest()
compositeFrequencyTailoredPatternSearchTest()
nonFrequencyPatternSearch3Test()
frequencyPatternSearch3Test()
nonFrequencyPatternSearch4Test()
frequencyPatternSearch4Test()
nonFrequencyPatternSearch5Test()
frequencyPatternSearch5Test()
frequencyPatternSearch6Test()

print("Finished running all tests, overall time: %s" % runTest.over_all_time)