from plan.TreePlanBuilderFactory import IterativeImprovementTreePlanBuilderParameters
from test.KC_tests import *
from test.MultiPattern_tests import *
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from misc.ConsumptionPolicy import *
from plan.LeftDeepTreeBuilders import *
from plan.BushyTreeBuilders import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, MulTerm, EqFormula, IdentifierTerm, \
    AtomicTerm, AndFormula, TrueFormula
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern
from tree.PatternMatchStorage import TreeStorageParameters
try:
    from UnitTests.test_storage import run_storage_tests
except ImportError:
    from test.UnitTests.test_storage import run_storage_tests
    

def oneArgumentsearchTest(createTestFile = False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
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
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
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
        SeqOperator([PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"]))
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
        SeqOperator([PrimitiveEventStructure("AMZN", "x1"), PrimitiveEventStructure("AMZN", "x2"), PrimitiveEventStructure("AMZN", "x3")]),
        AndFormula(
            SmallerThanEqFormula(IdentifierTerm("x1", lambda x: x["Lowest Price"]), AtomicTerm(75)),
            AndFormula(
                GreaterThanEqFormula(IdentifierTerm("x2", lambda x: x["Peak Price"]), AtomicTerm(78)),
                SmallerThanEqFormula(IdentifierTerm("x3", lambda x: x["Lowest Price"]),
                                     IdentifierTerm("x1", lambda x: x["Lowest Price"]))
            )
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
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("MSFT", "c"), PrimitiveEventStructure("DRIV", "d"), PrimitiveEventStructure("MSFT", "e")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            AndFormula(
                SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                                   IdentifierTerm("d", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("d", lambda x: x["Peak Price"]),
                                   IdentifierTerm("e", lambda x: x["Peak Price"]))
            )
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
        SeqOperator([PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b")]),
        GreaterThanEqFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                             MulTerm(AtomicTerm(1.01), IdentifierTerm("a", lambda x: x["Peak Price"]))),
        timedelta(minutes=30)
    )
    runTest('googleIncrease', [googleIncreasePattern], createTestFile)


def amazonSpecificPatternSearchTest(createTestFile=False):
    """
    This pattern is looking for an amazon stock in peak price of 73.
    """
    amazonSpecificPattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AMZN", "a")]),
        EqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(73)),
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
        AndOperator([PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("GOOG", "g")]),
        AndFormula(
            SmallerThanEqFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(73)),
            SmallerThanEqFormula(IdentifierTerm("g", lambda x: x["Peak Price"]), AtomicTerm(525))
        ),
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
        AndOperator([PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AVID", "b"), PrimitiveEventStructure("AAPL", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            AndFormula(
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                                   IdentifierTerm("a", lambda x: x["Peak Price"]))
            )
        ),
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
        AndOperator([PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                               IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                               IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=1)
    )
    runTest('hierarchy', [hierarchyPattern], createTestFile)


def duplicateEventTypeTest(createTestFile=False):
    """
    PATTERN SEQ(AmazonStockPriceUpdate a, AmazonStockPriceUpdate b, AmazonStockPriceUpdate c)
    WHERE   TRUE
    WITHIN 10 minutes
    """
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AMZN", "c")]),
        TrueFormula(),
        timedelta(minutes=10)
    )
    runTest("duplicateEventType", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)


def nonFrequencyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LOCM", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    runTest("nonFrequency", [pattern], createTestFile)


def frequencyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LOCM", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 460, "AMZN": 442, "LOCM": 219})
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("frequency", [pattern], createTestFile, eval_mechanism_params=eval_params)


def arrivalRatesPatternSearchTest(createTestFile=False):
    pattern = Pattern(
    SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LOCM", "c")]),
    AndFormula(
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])),
        GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
    timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, [0.0159, 0.0153, 0.0076])
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("arrivalRates", [pattern], createTestFile, eval_mechanism_params=eval_params)


def nonFrequencyPatternSearch2Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("LOCM", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AAPL", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    runTest("nonFrequency2", [pattern], createTestFile)


def frequencyPatternSearch2Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("LOCM", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AAPL", "c")]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 2, "AMZN": 3, "LOCM": 1})
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("frequency2", [pattern], createTestFile, eval_mechanism_params=eval_params)


def nonFrequencyPatternSearch3Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AAPL", "b"), PrimitiveEventStructure("AAPL", "c"), PrimitiveEventStructure("LOCM", "d")]),
        TrueFormula(),
        timedelta(minutes=5)
    )
    runTest("nonFrequency3", [pattern], createTestFile)


def frequencyPatternSearch3Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AAPL", "b"), PrimitiveEventStructure("AAPL", "c"), PrimitiveEventStructure("LOCM", "d")]),
        TrueFormula(),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 460, "LOCM": 219})
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("frequency3", [pattern], createTestFile, eval_mechanism_params=eval_params)


def nonFrequencyPatternSearch4Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("LOCM", "d")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    runTest("nonFrequency4", [pattern], createTestFile)


def frequencyPatternSearch4Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("LOCM", "d")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AVID": 1, "LOCM": 2, "AAPL": 3, "AMZN": 4})
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("frequency4", [pattern], createTestFile, eval_mechanism_params=eval_params)


def nonFrequencyPatternSearch5Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"), PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"), PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    runTest("nonFrequency5", [pattern], createTestFile)


def frequencyPatternSearch5Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"), PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"), PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"LOCM": 1, "AAPL": 2})  # {"AAPL": 460, "LOCM": 219}
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("frequency5", [pattern], createTestFile, eval_mechanism_params=eval_params)

    
def frequencyPatternSearch6Test(createTestFile = False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"), PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"), PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")]),
        TrueFormula(),
        timedelta(minutes=7)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 1, "LOCM": 2})  # {"AAPL": 460, "LOCM": 219}
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("frequency6", [pattern], createTestFile, eval_mechanism_params=eval_params)


def greedyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('greedy1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def iiRandomPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        IterativeImprovementTreePlanBuilderParameters(DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.tree_plan_params.cost_model_type,
                                                      20,
                                                      IterativeImprovementType.SWAP_BASED,
                                                      IterativeImprovementInitType.RANDOM),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('iiRandom1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def iiRandom2PatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        IterativeImprovementTreePlanBuilderParameters(DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.tree_plan_params.cost_model_type,
                                                      20,
                                                      IterativeImprovementType.CIRCLE_BASED,
                                                      IterativeImprovementInitType.RANDOM),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('iiRandom2', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def iiGreedyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        IterativeImprovementTreePlanBuilderParameters(DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.tree_plan_params.cost_model_type,
                                                      20,
                                                      IterativeImprovementType.SWAP_BASED,
                                                      IterativeImprovementInitType.GREEDY),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('iiGreedy1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def iiGreedy2PatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        IterativeImprovementTreePlanBuilderParameters(DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.tree_plan_params.cost_model_type,
                                                      20,
                                                      IterativeImprovementType.CIRCLE_BASED,
                                                      IterativeImprovementInitType.GREEDY),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('iiGreedy2', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def dpLdPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('dpLd1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def dpBPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('dpB1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def zStreamOrdPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('zstream-ord1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def zStreamPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"), PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")]),
        AndFormula(
            AndFormula(
                SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]),
                                   IdentifierTerm("b", lambda x: x["Peak Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]),
                                   IdentifierTerm("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanFormula(IdentifierTerm("c", lambda x: x["Peak Price"]),
                               IdentifierTerm("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('zstream1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def frequencyTailoredPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"), PrimitiveEventStructure("CBRL", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=360)
    )
    frequencyDict = {"MSFT": 256, "DRIV": 257, "CBRL": 1}
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, frequencyDict)
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('frequencyTailored1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def nonFrequencyTailoredPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"), PrimitiveEventStructure("CBRL", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            GreaterThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=360)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('nonFrequencyTailored1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


# ON CUSTOM
def multipleNotBeginAndEndTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                     NegationOperator(PrimitiveEventStructure("TYP4", "t")),
                     PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                     PrimitiveEventStructure("GOOG", "c"),
                     NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                     NegationOperator(PrimitiveEventStructure("TYP3", "z"))]),
        AndFormula(
            AndFormula(
                GreaterThanFormula(IdentifierTerm("x", lambda x: x["Opening Price"]),
                                   IdentifierTerm("b", lambda x: x["Opening Price"])),
                SmallerThanFormula(IdentifierTerm("y", lambda x: x["Opening Price"]),
                                   IdentifierTerm("c", lambda x: x["Opening Price"]))),
            GreaterThanFormula(IdentifierTerm("t", lambda x: x["Opening Price"]),
                               IdentifierTerm("a", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    runTest("MultipleNotBeginAndEnd", [pattern], createTestFile)

# ON custom2
def simpleNotTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    runTest("simpleNot", [pattern], createTestFile)


# ON NASDAQ SHORT
def multipleNotInTheMiddleTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("LI", "d")), PrimitiveEventStructure("AMZN", "b"),
                     NegationOperator(PrimitiveEventStructure("FB", "e")), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            AndFormula(
                GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                                   IdentifierTerm("b", lambda x: x["Opening Price"])),
                SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                                   IdentifierTerm("c", lambda x: x["Opening Price"]))),
            AndFormula(
                GreaterThanFormula(IdentifierTerm("e", lambda x: x["Opening Price"]),
                                   IdentifierTerm("a", lambda x: x["Opening Price"])),
                SmallerThanFormula(IdentifierTerm("d", lambda x: x["Opening Price"]),
                                   IdentifierTerm("c", lambda x: x["Opening Price"])))
            ),
        timedelta(minutes=4)
    )
    runTest("MultipleNotMiddle", [pattern], createTestFile)


# ON NASDAQ SHORT
def oneNotAtTheBeginningTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(PrimitiveEventStructure("TYP1", "x")), PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    runTest("OneNotBegin", [pattern], createTestFile)

# ON NASDAQ SHORT
def multipleNotAtTheBeginningTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(PrimitiveEventStructure("TYP1", "x")), NegationOperator(PrimitiveEventStructure("TYP2", "y")),
                     NegationOperator(PrimitiveEventStructure("TYP3", "z")), PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    runTest("MultipleNotBegin", [pattern], createTestFile)


# ON NASDAQ *HALF* SHORT
def oneNotAtTheEndTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    runTest("OneNotEnd", [pattern], createTestFile)


# ON NASDAQ *HALF* SHORT
def multipleNotAtTheEndTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("GOOG", "c"), NegationOperator(PrimitiveEventStructure("TYP1", "x")),
                     NegationOperator(PrimitiveEventStructure("TYP2", "y")), NegationOperator(PrimitiveEventStructure("TYP3", "z"))]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )
    runTest("MultipleNotEnd", [pattern], createTestFile)

# ON CUSTOM3
def testWithMultipleNotAtBeginningMiddleEnd(createTestFile=False):
    pattern = Pattern(
        SeqOperator([NegationOperator(PrimitiveEventStructure("AAPL", "a")), PrimitiveEventStructure("AMAZON", "b"), NegationOperator(PrimitiveEventStructure("GOOG", "c")),
                     PrimitiveEventStructure("FB", "d"), NegationOperator(PrimitiveEventStructure("TYP1", "x"))]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
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
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")]),
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
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")]),
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
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")]),
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
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")]),
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
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")]),
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
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AVID", "c")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
        ConsumptionPolicy(freeze="b")
    )
    runTest("freezePolicy2", [pattern], createTestFile, eventStream=nasdaqEventStreamTiny)


def sortedStorageTest(createTestFile=False):
    pattern = Pattern(
        AndOperator([PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"), PrimitiveEventStructure("CBRL", "c")]),
        AndFormula(
            GreaterThanFormula(
                IdentifierTerm("a", lambda x: x["Opening Price"]), IdentifierTerm("b", lambda x: x["Opening Price"])
            ),
            GreaterThanFormula(
                IdentifierTerm("b", lambda x: x["Opening Price"]), IdentifierTerm("c", lambda x: x["Opening Price"])
            ),
        ),
        timedelta(minutes=360),
    )
    storage_params = TreeStorageParameters(True, clean_up_interval=500)
    eval_params = TreeBasedEvaluationMechanismParameters(
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.tree_plan_params, storage_params
    )
    runTest("sortedStorageTest", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def sortedStorageBenchMarkTest(createTestFile=False):
    pattern = Pattern(
        AndOperator([PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"), PrimitiveEventStructure("CBRL", "c"), PrimitiveEventStructure("MSFT", "m")]),
        AndFormula(
            GreaterThanEqFormula(
                IdentifierTerm("b", lambda x: x["Lowest Price"]), IdentifierTerm("a", lambda x: x["Lowest Price"])
            ),
            AndFormula(
                GreaterThanEqFormula(
                    IdentifierTerm("m", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"])
                ),
                GreaterThanEqFormula(
                    IdentifierTerm("m", lambda x: x["Lowest Price"]), IdentifierTerm("b", lambda x: x["Lowest Price"])
                ),
            ),
        ),
        timedelta(minutes=360),
    )
    runBenchMark("sortedStorageBenchMark - unsorted storage", [pattern])

    storage_params = TreeStorageParameters(sort_storage=True, attributes_priorities={"a": 122, "b": 200, "c": 104, "m": 139})
    eval_params = TreeBasedEvaluationMechanismParameters(
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.tree_plan_params, storage_params
    )
    runBenchMark("sortedStorageBenchMark - sorted storage", [pattern], eval_mechanism_params=eval_params)


runTest.over_all_time = 0

# basic functionality tests
oneArgumentsearchTest()
simplePatternSearchTest()
googleAscendPatternSearchTest()
amazonInstablePatternSearchTest()
msftDrivRacePatternSearchTest()
googleIncreasePatternSearchTest()
amazonSpecificPatternSearchTest()
googleAmazonLowPatternSearchTest()
nonsensePatternSearchTest()
hierarchyPatternSearchTest()
nonFrequencyPatternSearchTest()
arrivalRatesPatternSearchTest()
duplicateEventTypeTest()

# tree plan generation algorithms
frequencyPatternSearchTest()
nonFrequencyPatternSearch2Test()
frequencyPatternSearch2Test()
nonFrequencyPatternSearch3Test()
frequencyPatternSearch3Test()
nonFrequencyPatternSearch4Test()
frequencyPatternSearch4Test()
nonFrequencyPatternSearch5Test()
frequencyPatternSearch5Test()
frequencyPatternSearch6Test()
greedyPatternSearchTest()
iiRandomPatternSearchTest()
iiRandom2PatternSearchTest()
iiGreedyPatternSearchTest()
iiGreedy2PatternSearchTest()
zStreamOrdPatternSearchTest()
zStreamPatternSearchTest()
dpBPatternSearchTest()
dpLdPatternSearchTest()
nonFrequencyTailoredPatternSearchTest()
frequencyTailoredPatternSearchTest()

# tree structure tests - CEP object only created not used
structuralTest1()
structuralTest2()
structuralTest3()
structuralTest4()
structuralTest5()
structuralTest6()
structuralTest7()

# Kleene closure tests
oneArgumentsearchTestKleeneClosure()
MinMax_0_TestKleeneClosure()
MinMax_1_TestKleeneClosure()
MinMax_2_TestKleeneClosure()
KC_AND()

# negation tests
simpleNotTest()
multipleNotInTheMiddleTest()
oneNotAtTheBeginningTest()
multipleNotAtTheBeginningTest()
oneNotAtTheEndTest()
multipleNotAtTheEndTest()
multipleNotBeginAndEndTest()
testWithMultipleNotAtBeginningMiddleEnd()

# consumption policies tests
singleType1PolicyPatternSearchTest()
singleType2PolicyPatternSearchTest()
contiguousPolicyPatternSearchTest()
contiguousPolicy2PatternSearchTest()
freezePolicyPatternSearchTest()
freezePolicy2PatternSearchTest()

# storage tests
sortedStorageTest()
run_storage_tests()

# multi-pattern tests
# first approach: sharing leaves
leafIsRoot()
distinctPatterns()
threePatternsTest()
samePatternDifferentTimeStamps()
rootAndInner()

# second approach: sharing equivalent subtrees
onePatternIncludesOther()
samePatternSharingRoot()
severalPatternShareSubtree()
notInTheBeginningShare()
multipleParentsForInternalNode()

# benchmarks
if INCLUDE_BENCHMARKS:
    sortedStorageBenchMarkTest()


# Twitter tests
try:
    from TwitterTest import run_twitter_sanity_check
    run_twitter_sanity_check()
except ImportError:  # tweepy might not be installed
    pass
finally:
    print("Finished running all tests, overall time: %s" % runTest.over_all_time)
