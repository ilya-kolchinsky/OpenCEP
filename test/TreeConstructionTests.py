from adaptive.statistics.StatisticsCollectorFactory import StatisticsCollectorParameters
from misc.DefaultConfig import DEFAULT_TREE_COST_MODEL
from plan.TreePlanBuilderFactory import IterativeImprovementTreePlanBuilderParameters
from test.EvalTestsDefaults import DEFAULT_TESTING_STATISTICS_COLLECTOR_SELECTIVITY_AND_ARRIVAL_RATES_STATISTICS
from test.testUtils import *
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from adaptive.optimizer.OptimizerFactory import StatisticsDeviationAwareOptimizerParameters
from plan.LeftDeepTreeBuilders import *
from plan.BushyTreeBuilders import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import GreaterThanCondition, SmallerThanCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern


def nonFrequencyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("LOCM", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    runTest("nonFrequency", [pattern], createTestFile)


def frequencyPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("LOCM", "c")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("b", lambda x: x["Opening Price"]),
                        Variable("c", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    runTest("nonFrequency", [pattern], createTestFile)


def arrivalRatesPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("LOCM", "c")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("b", lambda x: x["Opening Price"]),
                        Variable("c", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.0159, 0.0153, 0.0076]})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters(
            TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest("arrivalRates", [pattern], createTestFile, eval_params)


def nonFrequencyPatternSearch2Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("LOCM", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    runTest("nonFrequency2", [pattern], createTestFile)


def frequencyPatternSearch2Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("LOCM", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AAPL", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=5)
    )
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.0076, 0.0153, 0.0159]})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters(
            TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest("frequency2", [pattern], createTestFile, eval_mechanism_params=eval_params)

def nonFrequencyPatternSearch3Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("AAPL", "c"), PrimitiveEventStructure("LOCM", "d")),
        TrueCondition(),
        timedelta(minutes=5)
    )
    runTest("nonFrequency3", [pattern], createTestFile)


def frequencyPatternSearch3Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AAPL", "b"),
                    PrimitiveEventStructure("AAPL", "c"), PrimitiveEventStructure("LOCM", "d")),
        TrueCondition(),
        timedelta(minutes=5)
    )
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.0159, 0.0159, 0.0159, 0.0076]})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters(
            TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest("frequency3", [pattern], createTestFile, eval_mechanism_params=eval_params)


def nonFrequencyPatternSearch4Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("LOCM", "d")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    runTest("nonFrequency4", [pattern], createTestFile)


def frequencyPatternSearch4Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("LOCM", "d")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters(
            TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.0159, 0.0153, 0.0146, 0.0076]})
    runTest("frequency4", [pattern], createTestFile, eval_mechanism_params=eval_params)


def nonFrequencyPatternSearch5Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"),
            PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"),
            PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    runTest("nonFrequency5", [pattern], createTestFile)


def frequencyPatternSearch5Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"),
            PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"),
            PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters(
            TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.0159, 0.0076, 0.0159, 0.0076, 0.0159, 0.0076]})  # {"AAPL": 460, "LOCM": 219}
    runTest("frequency5", [pattern], createTestFile, eval_mechanism_params=eval_params)


def frequencyPatternSearch6Test(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a1"), PrimitiveEventStructure("LOCM", "b1"),
            PrimitiveEventStructure("AAPL", "a2"), PrimitiveEventStructure("LOCM", "b2"),
            PrimitiveEventStructure("AAPL", "a3"), PrimitiveEventStructure("LOCM", "b3")),
        TrueCondition(),
        timedelta(minutes=7)
    )
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.0159, 0.0076, 0.0159, 0.0076, 0.0159, 0.0076]})  # {"AAPL": 460, "LOCM": 219}
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters(
            TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest("frequency6", [pattern], createTestFile, eval_mechanism_params=eval_params)

def greedyPatternSearchTest(createTestFile=False):
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
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=TreePlanBuilderParameters(TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)
    runTest('greedy1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def iiRandomPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SimpleCondition(Variable("a", lambda x: x["Peak Price"]),
                            Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y, z: x < y < z),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=IterativeImprovementTreePlanBuilderParameters(
            DEFAULT_TREE_COST_MODEL,
            20,
            IterativeImprovementType.SWAP_BASED,
            IterativeImprovementInitType.RANDOM),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest('iiRandom1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def iiRandom2PatternSearchTest(createTestFile=False):
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
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=IterativeImprovementTreePlanBuilderParameters(
            DEFAULT_TREE_COST_MODEL,
            20,
            IterativeImprovementType.CIRCLE_BASED,
            IterativeImprovementInitType.RANDOM),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest('iiRandom2', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def iiGreedyPatternSearchTest(createTestFile=False):
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
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=IterativeImprovementTreePlanBuilderParameters(
            DEFAULT_TREE_COST_MODEL,
            20,
            IterativeImprovementType.SWAP_BASED,
            IterativeImprovementInitType.GREEDY),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)


def iiGreedy2PatternSearchTest(createTestFile=False):
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
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=IterativeImprovementTreePlanBuilderParameters(
            DEFAULT_TREE_COST_MODEL,
            20,
            IterativeImprovementType.CIRCLE_BASED,
            IterativeImprovementInitType.GREEDY),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest('iiGreedy2', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def dpLdPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SimpleCondition(Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            Variable("d", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y, z: x < y < z)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=TreePlanBuilderParameters(TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)
    runTest('dpLd1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def dpBPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b"),
                    PrimitiveEventStructure("ORLY", "c"), PrimitiveEventStructure("CBRL", "d")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            BinaryCondition(Variable("b", lambda x: x["Peak Price"]),
                            Variable("c", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y),
            SmallerThanCondition(Variable("c", lambda x: x["Peak Price"]),
                                 Variable("d", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=TreePlanBuilderParameters(TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)
    runTest('dpB1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def zStreamOrdPatternSearchTest(createTestFile=False):
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
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=TreePlanBuilderParameters(TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.SELECTIVITY_MATRIX, StatisticsTypes.ARRIVAL_RATES])
        ),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)
    runTest('zstream-ord1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def zStreamPatternSearchTest(createTestFile=False):
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
    pattern.set_statistics({StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix,
                            StatisticsTypes.ARRIVAL_RATES: arrivalRates})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=TreePlanBuilderParameters(TreePlanBuilderTypes.ZSTREAM_BUSHY_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES, StatisticsTypes.SELECTIVITY_MATRIX])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)
    runTest('zstream1', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def frequencyTailoredPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"),
                    PrimitiveEventStructure("CBRL", "c")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"])),
            GreaterThanCondition(Variable("b", lambda x: x["Opening Price"]),
                                 Variable("c", lambda x: x["Opening Price"]))
        ),
        timedelta(minutes=360)
    )
    # frequencyDict = {"MSFT": 256, "DRIV": 257, "CBRL": 1}
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.01454418928322895, 0.016597077244258872, 0.012421711899791231]})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(
            tree_plan_params=TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
            statistics_collector_params=StatisticsCollectorParameters(statistics_types=[StatisticsTypes.ARRIVAL_RATES])),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest('frequencyTailored1', [pattern], createTestFile, eval_mechanism_params=eval_params,
            events=nasdaqEventStream)


def nonFrequencyTailoredPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("DRIV", "a"), PrimitiveEventStructure("MSFT", "b"),
                    PrimitiveEventStructure("CBRL", "c")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("b", lambda x: x["Opening Price"]),
                        Variable("c", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=360)
    )
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params)

    runTest('nonFrequencyTailored1', [pattern], createTestFile, eval_mechanism_params=eval_params,
            events=nasdaqEventStream)
