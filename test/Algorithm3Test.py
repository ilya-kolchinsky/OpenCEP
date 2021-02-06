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

###########################################Algorithm3###################################################################
########################################################################################################################
"""
Basic functionality tests for Algorithm3
"""

def oneArgumentsearchTestAlgorithm3(createTestFile=False):
    """
    The test finds all the AAPL stock thier opening price greater than 135
    "by splitting tha given data into 8 threads.
    Each stock type divided into semi groups according to randomly chosen attribute,
    and each semi-group goes to another thread while covering all the possible combinations.
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 135),
        timedelta(minutes=120)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("one", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=5,
                                                                 attributes_dict=attribute_dict)
            )




def simplePatternSearchTestAlgorithm3(createTestFile=False):
    """
    Using 64 threads , Algorithm 3 calculates in parallel the pattern below by dividing each stock type into semi-groups
    according to randomly chosen attribute and than calculates all the possible semi-groups combinations over the threads.
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("simple", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=65,
                                                                 attributes_dict=attribute_dict)
            )

def googleAmazonLowPatternSearchTestAlgorithm3(createTestFile=False):
    """
    Using 16 threads , Algorithm 3 calculates in parallel the pattern below by dividing each stock type into semi-groups
    according to randomly chosen attribute and than calculates all the possible semi-groups combinations over the threads.
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = googleAmazonLowPattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("googleAmazonLow", [googleAmazonLowPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict)
            )


def nonsensePatternSearchTestAlgorithm3(createTestFile=False):
    """
    Using 27 threads , Algorithm 3 calculates in parallel the pattern below by dividing each stock type into semi-groups
    according to randomly chosen attribute and than calculates all the possible semi-groups combinations over the threads.
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = nonsensePattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("nonsense", [nonsensePattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=28,
                                                                 attributes_dict=attribute_dict)
            )



def duplicateEventTypeTestAlgorithm3(createTestFile=False):
    """
    Using 8 threads , Algorithm 3 calculates in parallel the pattern below by dividing each stock type into semi-groups
    according to randomly chosen attribute and than calculates all the possible semi-groups combinations over the threads.
    PATTERN SEQ(AmazonStockPriceUpdate a, AmazonStockPriceUpdate b, AmazonStockPriceUpdate c)
    WHERE   TRUE
    WITHIN 10 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AMZN", "c")),
        TrueCondition(),
        timedelta(minutes=10)
    )

    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("duplicateEventType", [pattern], createTestFile,eventStream=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict)
            )



def amazonSpecificPatternSearchTestAlgorithm3(createTestFile=False):
    """
    Using 27 threads , Algorithm 3 calculates in parallel the pattern below by dividing each stock type into semi-groups
    according to randomly chosen attribute and than calculates all the possible semi-groups combinations over the threads.
    This pattern is looking for an amazon stock in peak price of 73,
    by splitting tha given data into 27 threads.
    """
    amazonSpecificPattern = Pattern(

        SeqOperator(PrimitiveEventStructure("AMZN", "a"), PrimitiveEventStructure("AAPL", "b"),PrimitiveEventStructure("GOOG", "c")),
        EqCondition(Variable("a", lambda x: x["Peak Price"]), 73),
        timedelta(minutes=120)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = amazonSpecificPattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('amazonSpecific', [amazonSpecificPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=28,attributes_dict=attribute_dict))


def googleAscendPatternSearchTestAlgorithm3(createTestFile=False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = googleAscendPattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('googleAscend', [googleAscendPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM, ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,attributes_dict=attribute_dict))


def amazonInstablePatternSearchTestAlgorithm3(createTestFile=False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = amazonInstablePattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('amazonInstable', [amazonInstablePattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=65,
                                                                 attributes_dict=attribute_dict))


def msftDrivRacePatternSearchTestAlgorithm3(createTestFile=False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = msftDrivRacePattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('msftDrivRace', [msftDrivRacePattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=33,
                                                                 attributes_dict=attribute_dict))



def googleIncreasePatternSearchTestAlgorithm3(createTestFile=False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = googleIncreasePattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('googleIncrease', [googleIncreasePattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=10,
                                                                 attributes_dict=attribute_dict))



def hierarchyPatternSearchTestAlgorithm3(createTestFile=False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = hierarchyPattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('hierarchy', [hierarchyPattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))




"""
 tree plan generation algorithms for Algorithm3
"""


def arrivalRatesPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LOCM", "c")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("b", lambda x: x["Opening Price"]),
                        Variable("c", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, [0.0159, 0.0153, 0.0076])
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('arrivalRates', [pattern], createTestFile,eval_params,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))


def nonFrequencyPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LOCM", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('nonFrequency', [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=28,
                                                                 attributes_dict=attribute_dict))
def frequencyPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("LOCM", "c")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("b", lambda x: x["Opening Price"]),
                        Variable("c", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("nonFrequency", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=28,
                                                                 attributes_dict=attribute_dict))





def nonFrequencyPatternSearch3TestAlgorithm3(createTestFile=False):

    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("AAPL", "c"), PrimitiveEventStructure("LOCM", "d")),
        TrueCondition(),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('nonFrequency3', [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict))




def frequencyPatternSearch3TestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("AAPL", "c"), PrimitiveEventStructure("LOCM", "d")),
        TrueCondition(),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 460, "LOCM": 219})
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("frequency3", [pattern], createTestFile, eval_mechanism_params=eval_params,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict))




def nonFrequencyPatternSearch2TestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("LOCM", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("nonFrequency2", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))


def frequencyPatternSearch2TestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("LOCM", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 2, "AMZN": 3, "LOCM": 1})
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("frequency2", [pattern], createTestFile,eval_mechanism_params=eval_params,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))


def nonFrequencyPatternSearch4TestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("LOCM", "d")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("nonFrequency4", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict))


def frequencyPatternSearch4TestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("LOCM", "d")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AVID": 1, "LOCM": 2, "AAPL": 3, "AMZN": 4})
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("frequency4", [pattern], createTestFile,eval_mechanism_params=eval_params,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=82,
                                                                 attributes_dict=attribute_dict))


def nonFrequencyPatternSearch5TestAlgorithm3(createTestFile=False):
    """
    might takes more than 2 minutes
    """
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"),
            PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"),
            PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("nonFrequency5", [pattern], createTestFile,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=65,
                                                                 attributes_dict=attribute_dict))


def frequencyPatternSearch5TestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"),
            PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"),
            PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"LOCM": 1, "AAPL": 2})  # {"AAPL": 460, "LOCM": 219}
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("frequency5", [pattern], createTestFile, eval_mechanism_params=eval_params,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=65,
                                                                 attributes_dict=attribute_dict))



def frequencyPatternSearch6TestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"),
            PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"),
            PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, {"AAPL": 1, "LOCM": 2})  # {"AAPL": 460, "LOCM": 219}
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("frequency6",[pattern], createTestFile, eval_mechanism_params=eval_params,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=65,
                                                                 attributes_dict=attribute_dict))


def greedyPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"]))
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("greedy1", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict))


def iiRandomPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Peak Price"]),
                            Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            relation_op=lambda x,y,z: x < y < z),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"]))
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("iiRandom1", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict))


def iiRandom2PatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Peak Price"]),
                            Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            Variable("d", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y, z, w: x < y < z < w)
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("iiRandom2", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=82,
                                                                 attributes_dict=attribute_dict))


def iiGreedyPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"]))
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("iiGreedy1", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=82,
                                                                 attributes_dict=attribute_dict))


def iiGreedy2PatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"]))
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("iiGreedy2", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict))



def dpLdPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SimpleCondition(Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            Variable("d", lambda x: x["Peak Price"]),
                            relation_op=lambda x,y,z: x < y < z)
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("dpLd1", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict))


def dpBPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            BinaryCondition(Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            relation_op=lambda x,y: x < y),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"]))
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("dpB1", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=82,
                                                                 attributes_dict=attribute_dict))


def zStreamOrdPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            AndCondition(
                SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                     Variable("b", lambda x: x["Peak Price"])),
                SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                     Variable("c", lambda x: x["Peak Price"]))
            ),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"])),
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest('zstream-ord1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=82,
                                                                 attributes_dict=attribute_dict))


def zStreamPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            AndCondition(
                SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                     Variable("b", lambda x: x["Peak Price"])),
                AndCondition(
                    SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                         Variable("c", lambda x: x["Peak Price"]))
                )
            ),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"]))
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("zstream1", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=17,
                                                                 attributes_dict=attribute_dict))


def frequencyTailoredPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"), PrimitiveEventStructure("CBRL", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=360)
    )
    frequencyDict = {"MSFT": 256, "DRIV": 257, "CBRL": 1}
    pattern.set_statistics(StatisticsTypes.FREQUENCY_DICT, frequencyDict)
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("frequencyTailored1", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=28,
                                                                 attributes_dict=attribute_dict))


def nonFrequencyTailoredPatternSearchTestAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"), PrimitiveEventStructure("CBRL", "c")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("b", lambda x: x["Opening Price"]),
                        Variable("c", lambda x: x["Opening Price"]),
                        relation_op= lambda x,y,z: x > y > z),
        timedelta(minutes=360)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("nonFrequencyTailored1", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))


"""
Kleene closure tests
"""

def MinMax_0_TestKleeneClosureAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"), min_size=1, max_size=2)),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("MinMax_0_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=5,
                                                                 attributes_dict=attribute_dict))

def MinMax_2_TestKleeneClosureAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"), min_size=4, max_size=5)),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("MinMax_2_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=33,
                                                                 attributes_dict=attribute_dict))


def KC_AND_IndexCondition_01Algorithm3(createTestFile=False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    print(attribute_dict)
    runTest("KC_AND_IndexCondition_01_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=65,
                                                                 attributes_dict=attribute_dict))


def KC_AND_IndexCondition_02Algorithm3(createTestFile=False):
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
    runTest("KC_AND_IndexCondition_02_", [pattern], createTestFile, events=nasdaqEventStreamKC)
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("KC_AND_IndexCondition_01_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=65,
                                                                 attributes_dict=attribute_dict))

def KC_AND_NegOffSet_01Algorithm3(createTestFile=False):
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
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"])),
            KCIndexCondition(names={'a', 'b', 'c'}, getattr_func=lambda x: x["Peak Price"],
                             relation_op=lambda x, y: x < 1 + y,
                             offset=-1)
        ),
        timedelta(minutes=3)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("KC_AND_NegOffSet_01_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=82,
                                                                 attributes_dict=attribute_dict))

def KC_AllValuesAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueCondition(names={'a'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x > y,
                             value=530.5)
        ),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("KC_AllValues_01_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))

def KC_Specific_ValueAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueCondition(names={'a'}, getattr_func=lambda x: x["Peak Price"], relation_op=lambda x, y: x > y,
                             index=2, value=530.5)
        ),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("KC_Specific_Value_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=10,
                                                                 attributes_dict=attribute_dict))

def KC_MixedAlgorithm3(createTestFile=False):
    pattern = Pattern(
        SeqOperator(KleeneClosureOperator(PrimitiveEventStructure("GOOG", "a"))),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Opening Price"]), relation_op=lambda x: x > 0),
            KCValueCondition(names={'a'}, getattr_func=lambda x: x["Peak Price"],
                             relation_op=lambda x, y: x > y,
                             value=530.5),
            KCIndexCondition(names={'a'}, getattr_func=lambda x: x["Opening Price"],
                             relation_op=lambda x, y: x + 0.5 < y,
                             offset=-1)
        ),
        timedelta(minutes=5)
    )
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("KC_Mixed_", [pattern], createTestFile, events=nasdaqEventStreamKC,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=10,
                                                                 attributes_dict=attribute_dict))

"""
consumption policies tests
"""

def singleType1PolicyPatternSearchTestAlgorithm3(createTestFile = False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("singleType1Policy", [pattern], createTestFile, events=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))


def singleType2PolicyPatternSearchTestAlgorithm3(createTestFile = False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("singleType2Policy", [pattern], createTestFile, events=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))


def contiguousPolicyPatternSearchTestAlgorithm3(createTestFile = False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("contiguousPolicySingleList", [pattern], createTestFile, events=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=28,
                                                                 attributes_dict=attribute_dict))


def contiguousPolicy2PatternSearchTestAlgorithm3(createTestFile = False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("contiguousPolicyMultipleLists", [pattern], createTestFile, events=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=65,
                                                                 attributes_dict=attribute_dict))




def freezePolicy2PatternSearchTestAlgorithm3(createTestFile = False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("freezePolicy2", [pattern], createTestFile, events=nasdaqEventStreamTiny,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=28,
                                                                 attributes_dict=attribute_dict))



"""
storage tests
"""

def sortedStorageTestAlgorithm3(createTestFile=False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern.get_all_event_types_with_duplicates()
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runTest("sortedStorageTest", [pattern], createTestFile,  eval_mechanism_params=eval_params, events=nasdaqEventStream,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                  ParallelExecutionPlatforms.THREADING),
            data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3, num_threads=9,
                                                                 attributes_dict=attribute_dict))


"""
multi-pattern tests
"""
def distinctPatternsAlgorithm3(createTestFile = False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern1.get_all_event_types_with_duplicates()
    types.extend(pattern2.get_all_event_types_with_duplicates())
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runMultiTest("BigMultiPattern", [pattern1, pattern2], createTestFile,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                       ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3,
                                                                      num_threads=65,
                                                                      attributes_dict=attribute_dict))

"""
multi-pattern test checking case where output node is not a root
"""
def rootAndInnerAlgorithm3(createTestFile = False):
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

    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern1.get_all_event_types_with_duplicates()
    types.extend(pattern2.get_all_event_types_with_duplicates())
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runMultiTest("RootAndInner", [pattern1, pattern2], createTestFile,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                       ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3,
                                                                      num_threads=17,
                                                                      attributes_dict=attribute_dict))


"""
multi-pattern test 2 identical patterns with different time stamp
"""
def samePatternDifferentTimeStampsAlgorithm3(createTestFile = False):
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

    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern1.get_all_event_types_with_duplicates()
    types.extend(pattern2.get_all_event_types_with_duplicates())
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runMultiTest("DifferentTimeStamp", [pattern1, pattern2], createTestFile,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                       ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3,
                                                                      num_threads=65,
                                                                      attributes_dict=attribute_dict))


"""
multi-pattern test sharing equivalent subtrees
"""
def onePatternIncludesOtherAlgorithm3(createTestFile = False):
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
    attributes = ["Opening Price", "Peak Price", "Lowest Price", "Close Price", "Volume"]
    attribute_dict = {}
    types = pattern1.get_all_event_types_with_duplicates()
    types.extend(pattern2.get_all_event_types_with_duplicates())
    for type in types:
        if type in attribute_dict.keys():
            attribute_dict[type].append(random.choice(attributes))
        else:
            attribute_dict[type] = [random.choice(attributes)]
    runMultiTest("onePatternIncludesOther", [pattern1, pattern2], createTestFile,eval_mechanism_params,
                 parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM,
                                                                       ParallelExecutionPlatforms.THREADING),
                 data_parallel_params=DataParallelExecutionParameters(DataParallelExecutionModes.ALGORITHM3,
                                                                      num_threads=33,
                                                                      attributes_dict=attribute_dict))


