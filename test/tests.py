from test.testUtils import *
from test.KC_tests import *

from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes, \
    IterativeImprovementEvaluationMechanismParameters, TreeBasedEvaluationMechanismParameters
from misc.ConsumptionPolicy import *
from evaluation.LeftDeepTreeBuilders import *
from evaluation.BushyTreeBuilders import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, \
    EqFormula, IdentifierTerm, TrueFormula, NaryFormula, AndFormula
from base.PatternStructure import AndOperator, SeqOperator, QItem, NegationOperator
from base.Pattern import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters

try:
    from UnitTests.test_storage import run_storage_tests
except ImportError:
    from test.UnitTests.test_storage import run_storage_tests


def oneArgumentsearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a")]),
        NaryFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 135),
        timedelta(minutes=120)
    )
    runTest("one", [pattern], createTestFile)


def simplePatternSearchTest(createTestFile=False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > b.OpeningPrice
        AND b.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]),
        AndFormula(
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


def googleAscendPatternSearchTest(createTestFile=False):
    """
    This pattern is looking for a short ascend in the Google peak prices.
    PATTERN SEQ(GoogleStockPriceUpdate a, GoogleStockPriceUpdate b, GoogleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    WITHIN 3 minutes
    """
    googleAscendPattern = Pattern(
        SeqOperator([QItem("GOOG", "a"), QItem("GOOG", "b"), QItem("GOOG", "c")]),
        AndFormula(
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


def amazonInstablePatternSearchTest(createTestFile=False):
    """
    This pattern is looking for an in-stable day for Amazon.
    PATTERN SEQ(AmazonStockPriceUpdate x1, AmazonStockPriceUpdate x2, AmazonStockPriceUpdate x3)
    WHERE x1.LowestPrice <= 75 AND x2.PeakPrice >= 78 AND x3.LowestPrice <= x1.LowestPrice
    WITHIN 1 day
    """
    amazonInstablePattern = Pattern(
        SeqOperator([QItem("AMZN", "x1"), QItem("AMZN", "x2"), QItem("AMZN", "x3")]),
        AndFormula(
            [
                NaryFormula(IdentifierTerm("x1", lambda x: x["Lowest Price"]),
                            relation_op=lambda x: x <= 75),
                NaryFormula(IdentifierTerm("x2", lambda x: x["Peak Price"]),
                            relation_op=lambda x: x >= 78),
                NaryFormula(IdentifierTerm("x3", lambda x: x["Lowest Price"]),
                            IdentifierTerm("x1", lambda x: x["Lowest Price"]),
                            relation_op=lambda x, y: x <= y)
            ]
        ),
        timedelta(days=1)
    )
    runTest('amazonInstable', [amazonInstablePattern], createTestFile)


def msftDrivRacePatternSearchTest(createTestFile=False):
    """
    This pattern is looking for a race between driv and microsoft in ten minutes
    PATTERN SEQ(MicrosoftStockPriceUpdate a, DrivStockPriceUpdate b, MicrosoftStockPriceUpdate c, DrivStockPriceUpdate d, MicrosoftStockPriceUpdate e)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < d.PeakPrice AND d.PeakPrice < e.PeakPrice
    WITHIN 10 minutes
    """
    msftDrivRacePattern = Pattern(
        SeqOperator(
            [QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("MSFT", "c"), QItem("DRIV", "d"), QItem("MSFT", "e")]),
        AndFormula(
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


def googleIncreasePatternSearchTest(createTestFile=False):
    """
    This Pattern is looking for a 1% increase in the google stock in a half-hour.
    PATTERN SEQ(GoogleStockPriceUpdate a, GoogleStockPriceUpdate b)
    WHERE b.PeakPrice >= 1.01 * a.PeakPrice
    WITHIN 30 minutes
    """
    googleIncreasePattern = Pattern(
        SeqOperator([QItem("GOOG", "a"), QItem("GOOG", "b")]),
        NaryFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                    IdentifierTerm("a", lambda x: x["Peak Price"]),
                    relation_op=lambda x, y: x >= y*1.01),
        timedelta(minutes=30)
    )
    runTest('googleIncrease', [googleIncreasePattern], createTestFile)


def amazonSpecificPatternSearchTest(createTestFile=False):
    """
    This pattern is looking for an amazon stock in peak price of 73.
    """
    amazonSpecificPattern = Pattern(
        SeqOperator([QItem("AMZN", "a")]),
        NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), relation_op=lambda x: x == 73),
        timedelta(minutes=120)
    )
    runTest('amazonSpecific', [amazonSpecificPattern], createTestFile)


def googleAmazonLowPatternSearchTest(createTestFile=False):
    """
    This pattern is looking for low prices of Amazon and Google at the same minute.
    PATTERN AND(AmazonStockPriceUpdate a, GoogleStockPriceUpdate g)
    WHERE a.PeakPrice <= 73 AND g.PeakPrice <= 525
    WITHIN 1 minute
    """
    googleAmazonLowPattern = Pattern(
        AndOperator([QItem("AMZN", "a"), QItem("GOOG", "g")]),
        AndFormula([
            NaryFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                        relation_op=lambda x: x <= 73),
            NaryFormula(IdentifierTerm("g", lambda x: x["Peak Price"]),
                        relation_op=lambda x: x <= 525)
        ]),
        timedelta(minutes=1)
    )
    runTest('googleAmazonLow', [googleAmazonLowPattern], createTestFile)


def nonsensePatternSearchTest(createTestFile=False):
    """
    This pattern is looking for something that does not make sense.
    PATTERN AND(AmazonStockPriceUpdate a, AvidStockPriceUpdate b, AppleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice AND c.PeakPrice < a.PeakPrice
    """
    nonsensePattern = Pattern(
        AndOperator([QItem("AMZN", "a"), QItem("AVID", "b"), QItem("AAPL", "c")]),
        AndFormula([
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


def hierarchyPatternSearchTest(createTestFile=False):
    """
    The following pattern is looking for Amazon < Apple < Google cases in one minute windows.
    PATTERN AND(AmazonStockPriceUpdate a, AppleStockPriceUpdate b, GoogleStockPriceUpdate c)
    WHERE a.PeakPrice < b.PeakPrice AND b.PeakPrice < c.PeakPrice
    WITHIN 1 minute
    """
    hierarchyPattern = Pattern(
        AndOperator([QItem("AMZN", "a"), QItem("AAPL", "b"), QItem("GOOG", "c")]),
        AndFormula([
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


def nonFrequencyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("LOCM", "c")]),
        AndFormula([
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("nonFrequency", [pattern], createTestFile)


def frequencyPatternSearchTest(createTestFile=False):
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


def arrivalRatesPatternSearchTest(createTestFile=False):
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


def nonFrequencyPatternSearch2Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("LOCM", "a"), QItem("AMZN", "b"), QItem("AAPL", "c")]),
        AndFormula([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("nonFrequency2", [pattern], createTestFile)


def frequencyPatternSearch2Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("LOCM", "a"), QItem("AMZN", "b"), QItem("AAPL", "c")]),
        AndFormula([
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 2, "AMZN": 3, "LOCM": 1})
    runTest("frequency2", [pattern], createTestFile, EvaluationMechanismTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)


def greedyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula([
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


def iiRandomPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula([
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


def iiRandom2PatternSearchTest(createTestFile=False):
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


def iiGreedyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula([
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


def iiGreedy2PatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula([
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


def dpLdPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula([
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


def dpBPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula([
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


def zStreamOrdPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula([
            AndFormula([
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ]),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"])),
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


def zStreamPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("MSFT", "a"), QItem("DRIV", "b"), QItem("ORLY", "c"), QItem("CBRL", "d")]),
        AndFormula([
            AndFormula([
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                AndFormula([
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


def frequencyTailoredPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("DRIV", "a"), QItem("MSFT", "b"), QItem("CBRL", "c")]),
        AndFormula([
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


def nonFrequencyTailoredPatternSearchTest(createTestFile=False):
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


# ON CUSTOM
def multipleNotBeginAndEndTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(QItem("TYP1", "x")),
                     NegationOperator(QItem("TYP4", "t")),
                     QItem("AAPL", "a"), QItem("AMZN", "b"),
                     QItem("GOOG", "c"),
                     NegationOperator(QItem("TYP2", "y")),
                     NegationOperator(QItem("TYP3", "z"))]),
        AndFormula([
            GreaterThanFormula(IdentifierTerm("x", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("y", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("t", lambda x: x["Opening Price"]),
                               IdentifierTerm("a", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("MultipleNotBeginAndEnd", [pattern], createTestFile)

# ON custom2
def simpleNotTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), NegationOperator(QItem("AMZN", "b")), QItem("GOOG", "c")]),
        AndFormula([
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )

    runTest("simpleNot", [pattern], createTestFile)


# ON NASDAQ SHORT
def multipleNotInTheMiddleTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), NegationOperator(QItem("LI", "d")), QItem("AMZN", "b"),
                     NegationOperator(QItem("FB", "e")), QItem("GOOG", "c")]),
        AndFormula([
                GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                                   IdentifierTerm("b", lambda x: x["Opening Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                                   IdentifierTerm("c", lambda x: x["Opening Price"])),
                GreaterThanFormula(IdentifierTerm("e", lambda x: x["Opening Price"]),
                                   IdentifierTerm("a", lambda x: x["Opening Price"])),
                SmallerThanFormula(IdentifierTerm("d", lambda x: x["Opening Price"]),
                                   IdentifierTerm("c", lambda x: x["Opening Price"]))
            ]),
        timedelta(minutes=4)
    )
    runTest("MultipleNotMiddle", [pattern], createTestFile)


# ON NASDAQ SHORT
def oneNotAtTheBeginningTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(QItem("TYP1", "x")), QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("GOOG", "c")]),
        AndFormula([
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("OneNotBegin", [pattern], createTestFile)

# ON NASDAQ SHORT
def multipleNotAtTheBeginningTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(QItem("TYP1", "x")), NegationOperator(QItem("TYP2", "y")),
                     NegationOperator(QItem("TYP3", "z")), QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("GOOG", "c")]),
        AndFormula([
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("MultipleNotBegin", [pattern], createTestFile)


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("GOOG", "c"), NegationOperator(QItem("TYP1", "x"))]),
        AndFormula([
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("OneNotEnd", [pattern], createTestFile)


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("GOOG", "c"), NegationOperator(QItem("TYP1", "x")),
                     NegationOperator(QItem("TYP2", "y")), NegationOperator(QItem("TYP3", "z"))]),
        AndFormula([
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("MultipleNotEnd", [pattern], createTestFile)

# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEnd(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(QItem("AAPL", "a")), QItem("AMAZON", "b"), NegationOperator(QItem("GOOG", "c")),
                     QItem("FB", "d"), NegationOperator(QItem("TYP1", "x"))]),
        AndFormula([
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ]),
        timedelta(minutes=5)
    )
    runTest("NotEverywhere", [pattern], createTestFile)

def singleType1PolicyPatternSearchTest(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(single="AMZN", secondary_selection_strategy=SelectionStrategies.MATCH_NEXT)
    )
    runTest("singleType1Policy", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)

def singleType2PolicyPatternSearchTest(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(single="AMZN", secondary_selection_strategy=SelectionStrategies.MATCH_SINGLE)
    )
    runTest("singleType2Policy", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)


def contiguousPolicyPatternSearchTest(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(contiguous=["a", "b", "c"])
    )
    runTest("contiguousPolicySingleList", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)

def contiguousPolicy2PatternSearchTest(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(contiguous=[["a", "b"], ["b", "c"]])
    )
    runTest("contiguousPolicyMultipleLists", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)

def freezePolicyPatternSearchTest(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(freeze="a")
    )
    runTest("freezePolicy", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)

def freezePolicy2PatternSearchTest(createTestFile = False):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator([QItem("AAPL", "a"), QItem("AMZN", "b"), QItem("AVID", "c")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(freeze="b")
    )
    runTest("freezePolicy2", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)


def sortedStorageTest(createTestFile=False):
    pattern = Pattern(
        AndOperator([QItem("DRIV", "a"), QItem("MSFT", "b"), QItem("CBRL", "c")]),
        AndFormula([
            GreaterThanFormula(
                IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])
            ),
            GreaterThanFormula(
                IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])
            ),
        ]),
        timedelta(minutes=360),
    )
    storage_params = TreeStorageParameters(True, clean_up_interval=500)
    runTest("sortedStorageTest", [pattern], createTestFile,
            eval_mechanism_type=EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
            eval_mechanism_params=TreeBasedEvaluationMechanismParameters(storage_params=storage_params),
            events=nasdaqEventStream)


def sortedStorageBenchMarkTest(createTestFile=False):
    pattern = Pattern(
        AndOperator([QItem("DRIV", "a"), QItem("MSFT", "b"), QItem("CBRL", "c"), QItem("MSFT", "m")]),
        AndFormula([
            GreaterThanEqFormula(
                IdentifierTerm("b", lambda x: x["Lowest Price"]), IdentifierTerm("a", lambda x: x["Lowest Price"])
            ),
            GreaterThanEqFormula(
                IdentifierTerm("m", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"])
            ),
            GreaterThanEqFormula(
                IdentifierTerm("m", lambda x: x["Lowest Price"]), IdentifierTerm("b", lambda x: x["Lowest Price"])
            ),
        ]),
        timedelta(minutes=360),
    )
    runBenchMark("sortedStorageBenchMark - unsorted storage", [pattern])

    storage_params = TreeStorageParameters(sort_storage=True, attributes_priorities={"a": 122, "b": 200, "c": 104, "m": 139})
    runBenchMark("sortedStorageBenchMark - sorted storage", [pattern], eval_mechanism_params=TreeBasedEvaluationMechanismParameters(storage_params=storage_params))


runTest.over_all_time = 0

# basic functionality tests
# oneArgumentsearchTest()
# simplePatternSearchTest()
# googleAscendPatternSearchTest()
# amazonInstablePatternSearchTest()
# msftDrivRacePatternSearchTest()
# googleIncreasePatternSearchTest()
# amazonSpecificPatternSearchTest()
# googleAmazonLowPatternSearchTest()
# nonsensePatternSearchTest()
# hierarchyPatternSearchTest()
# nonFrequencyPatternSearchTest()
# arrivalRatesPatternSearchTest()
# frequencyPatternSearchTest()
# nonFrequencyPatternSearch2Test()
# frequencyPatternSearch2Test()
# nonFrequencyPatternSearch3Test()
# frequencyPatternSearch3Test()
# nonFrequencyPatternSearch4Test()
# frequencyPatternSearch4Test()
# nonFrequencyPatternSearch5Test()
# frequencyPatternSearch5Test()
# frequencyPatternSearch6Test()
# greedyPatternSearchTest()
# iiRandomPatternSearchTest()
# iiRandom2PatternSearchTest()
# iiGreedyPatternSearchTest()
# iiGreedy2PatternSearchTest()
# zStreamOrdPatternSearchTest()
# zStreamPatternSearchTest()
# dpBPatternSearchTest()
# dpLdPatternSearchTest()
# nonFrequencyTailoredPatternSearchTest()
# frequencyTailoredPatternSearchTest()
#
# # tree structure tests - CEP object only created not used
# structuralTest1()
# structuralTest2()
# structuralTest3()
# structuralTest4()
# structuralTest5()
# structuralTest6()
# structuralTest7()

# Kleene closure tests
# oneArgumentsearchTestKleeneClosure()
# MinMax_0_TestKleeneClosure()
# MinMax_1_TestKleeneClosure()
# MinMax_2_TestKleeneClosure()
KC_AND()

# negation tests
# simpleNotTest()
# multipleNotInTheMiddleTest()
# oneNotAtTheBeginningTest()
# multipleNotAtTheBeginningTest()
# oneNotAtTheEndTest()
# multipleNotAtTheEndTest()
# multipleNotBeginAndEndTest()
# testWithMultipleNotAtBeginningMiddleEnd()
#
# # consumption policies tests
# singleType1PolicyPatternSearchTest()
# singleType2PolicyPatternSearchTest()
# contiguousPolicyPatternSearchTest()
# contiguousPolicy2PatternSearchTest()
# freezePolicyPatternSearchTest()
# freezePolicy2PatternSearchTest()
#
# # storage tests
# sortedStorageTest()
# run_storage_tests()
#
# # benchmarks
# if INCLUDE_BENCHMARKS:
#     sortedStorageBenchMarkTest()
#
# # Twitter tests
# try:
#     from TwitterTest import run_twitter_sanity_check
#     run_twitter_sanity_check()
# except ImportError:  # tweepy might not be installed
#     pass
# finally:
#     print("Finished running all tests, overall time: %s" % runTest.over_all_time)
