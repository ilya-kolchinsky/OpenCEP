from adaptive.optimizer.OptimizerFactory import OptimizerParameters
from adaptive.optimizer.OptimizerTypes import OptimizerTypes
from test.testUtils import *
from plan.LeftDeepTreeBuilders import *
from plan.BushyTreeBuilders import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, KleeneClosureOperator, \
    NegationOperator
from base.Pattern import Pattern
from plan.TreePlanBuilderFactory import IterativeImprovementTreePlanBuilderParameters


def basicNestedTest(createTestFile=False):
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=5)
    )
    expected_result = ('And', ('Seq', 'a', 'b'), ('Seq', 'c', 'd'))
    runStructuralTest('basicNestedStructuralTest', [pattern], expected_result)
    runTest("basicNested", [pattern], createTestFile)


def nestedAscendingTest(createTestFile=False):
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d")),
                    AndOperator(PrimitiveEventStructure("GOOG", "e"), PrimitiveEventStructure("AAPL", "f")),
                    PrimitiveEventStructure("GOOG", "g"),
                    SeqOperator(PrimitiveEventStructure("AMZN", "h"), PrimitiveEventStructure("BIDU", "i"))),
        TrueCondition(),
        timedelta(minutes=1)
    )
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.11, 0.2, 0.3, 0.4, 0.5, 0.11, 0.5, 0.2, 0.4]})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("nestedAscending", [pattern], createTestFile, eval_params)


def nestedAscendingStructuralTest():
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d")),
                    AndOperator(PrimitiveEventStructure("GOOG", "e"), PrimitiveEventStructure("AAPL", "f")),
                    PrimitiveEventStructure("GOOG", "g"),
                    SeqOperator(PrimitiveEventStructure("AMZN", "h"), PrimitiveEventStructure("BIDU", "i"))),
        TrueCondition(),
        timedelta(minutes=1)
    )
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: [0.11, 0.2, 0.3, 0.4, 0.5, 0.11, 0.5, 0.2, 0.4]})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('And', ('And', ('And', ('And', 'g', ('Seq', 'a', 'b')), ('Seq', 'c', 'd')), ('And', 'e', 'f')), ('Seq', 'h', 'i'))
    runStructuralTest('nestedAscendingStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)


def greedyNestedTest(createTestFile=False):
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('greedyNested', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def greedyNestedStructuralTest():
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('And', ('Seq', 'b', 'a'), ('Seq', 'd', 'c'))
    runStructuralTest('greedyNestedStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)


def iiGreedyNestedPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=IterativeImprovementTreePlanBuilderParameters(DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.optimizer_params.tree_plan_params.cost_model_type,
                                                                                                            20,
                                                                                                            IterativeImprovementType.SWAP_BASED,
                                                                                                            IterativeImprovementInitType.GREEDY)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('iiGreedyNested', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def iiGreedyNestedStructuralTest():
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=IterativeImprovementTreePlanBuilderParameters(
                                                 DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.optimizer_params.tree_plan_params.cost_model_type,
                                                 20,
                                                 IterativeImprovementType.SWAP_BASED,
                                                 IterativeImprovementInitType.GREEDY)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('And', ('Seq', 'b', 'a'), ('Seq', 'd', 'c'))
    runStructuralTest('iiGreedyNestedStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)


def greedyNestedComplexStructuralTest():
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"), PrimitiveEventStructure("DRIV", "c")),
                    SeqOperator(PrimitiveEventStructure("LOCM", "d"), PrimitiveEventStructure("GOOG", "e")),
                    SeqOperator(PrimitiveEventStructure("AVID", "f"), SeqOperator(PrimitiveEventStructure("ORLY", "g"), PrimitiveEventStructure("CBRL", "h")), PrimitiveEventStructure("BIDU", "i")),
                    PrimitiveEventStructure("MSFT", "j")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
                         [0.1, 1.0, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19],
                         [0.2, 0.12, 1.0, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29],
                         [0.3, 0.13, 0.23, 1.0, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39],
                         [0.4, 0.14, 0.24, 0.34, 1.0, 0.45, 0.46, 0.47, 0.48, 0.49],
                         [0.5, 0.15, 0.25, 0.35, 0.45, 1.0, 0.56, 0.57, 0.58, 0.59],
                         [0.6, 0.16, 0.26, 0.36, 0.46, 0.56, 1.0, 0.67, 0.68, 0.69],
                         [0.7, 0.17, 0.27, 0.37, 0.47, 0.57, 0.67, 1.0, 0.78, 0.79],
                         [0.8, 0.18, 0.28, 0.38, 0.48, 0.58, 0.68, 0.78, 1.0, 0.89],
                         [0.9, 0.19, 0.29, 0.39, 0.49, 0.59, 0.69, 0.79, 0.89, 1.0]]
    arrivalRates = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('And', ('And', ('And', ('Seq', ('Seq', 'a', 'b'), 'c'), ('Seq', ('Seq', 'f', 'i'), ('Seq', 'g', 'h'))), ('Seq', 'd', 'e')), 'j')
    runStructuralTest('greedyNestedComplexStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)


def dpLdNestedPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('dpLdNested', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def dpLdNestedStructuralTest():
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('And', ('Seq', 'b', 'a'), ('Seq', 'd', 'c'))
    runStructuralTest('dpLdNestedStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)


def dpBNestedPatternSearchTest(createTestFile=False):
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('dpBNested', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def dpBNestedStructuralTest():
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d"))),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                         [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    arrivalRates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_BUSHY_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('And', ('Seq', 'a', 'b'), ('Seq', 'c', 'd'))
    runStructuralTest('dpBNestedStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)


def dpLdNestedComplexStructuralTest():
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                                PrimitiveEventStructure("DRIV", "c")),
                    SeqOperator(PrimitiveEventStructure("LOCM", "d"), PrimitiveEventStructure("GOOG", "e")),
                    SeqOperator(PrimitiveEventStructure("AVID", "f"), PrimitiveEventStructure("BIDU", "g"),
                                SeqOperator(PrimitiveEventStructure("ORLY", "h"),
                                            PrimitiveEventStructure("CBRL", "i"))),
                    PrimitiveEventStructure("MSFT", "j")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
                         [0.1, 1.0, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19],
                         [0.2, 0.12, 1.0, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29],
                         [0.3, 0.13, 0.23, 1.0, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39],
                         [0.4, 0.14, 0.24, 0.34, 1.0, 0.45, 0.46, 0.47, 0.48, 0.49],
                         [0.5, 0.15, 0.25, 0.35, 0.45, 1.0, 0.56, 0.57, 0.58, 0.59],
                         [0.6, 0.16, 0.26, 0.36, 0.46, 0.56, 1.0, 0.67, 0.68, 0.69],
                         [0.7, 0.17, 0.27, 0.37, 0.47, 0.57, 0.67, 1.0, 0.78, 0.79],
                         [0.8, 0.18, 0.28, 0.38, 0.48, 0.58, 0.68, 0.78, 1.0, 0.89],
                         [0.9, 0.19, 0.29, 0.39, 0.49, 0.59, 0.69, 0.79, 0.89, 1.0]]
    arrivalRates = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('And', ('And', ('And', ('Seq', ('Seq', 'a', 'b'), 'c'), 'j'), ('Seq', 'd', 'e')), ('Seq', ('Seq', 'f', 'g'), ('Seq', 'h', 'i')))
    runStructuralTest('dpLdNestedComplexStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)


def zstreamOrdNestedComplexStructuralTest():
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                                PrimitiveEventStructure("DRIV", "c")),
                    SeqOperator(PrimitiveEventStructure("LOCM", "d"), PrimitiveEventStructure("GOOG", "e")),
                    SeqOperator(PrimitiveEventStructure("AVID", "f"), PrimitiveEventStructure("BIDU", "g"),
                                SeqOperator(PrimitiveEventStructure("ORLY", "h"),
                                            PrimitiveEventStructure("CBRL", "i"))),
                    PrimitiveEventStructure("MSFT", "j")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("c", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            EqCondition(Variable("a", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("b", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("c", lambda x: x["Date"]), 200802010900),
            EqCondition(Variable("d", lambda x: x["Date"]), 200802010900)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
                         [0.1, 1.0, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19],
                         [0.2, 0.12, 1.0, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29],
                         [0.3, 0.13, 0.23, 1.0, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39],
                         [0.4, 0.14, 0.24, 0.34, 1.0, 0.45, 0.46, 0.47, 0.48, 0.49],
                         [0.5, 0.15, 0.25, 0.35, 0.45, 1.0, 0.56, 0.57, 0.58, 0.59],
                         [0.6, 0.16, 0.26, 0.36, 0.46, 0.56, 1.0, 0.67, 0.68, 0.69],
                         [0.7, 0.17, 0.27, 0.37, 0.47, 0.57, 0.67, 1.0, 0.78, 0.79],
                         [0.8, 0.18, 0.28, 0.38, 0.48, 0.58, 0.68, 0.78, 1.0, 0.89],
                         [0.9, 0.19, 0.29, 0.39, 0.49, 0.59, 0.69, 0.79, 0.89, 1.0]]
    arrivalRates = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('And', ('And', ('And', ('Seq', ('Seq', 'a', 'b'), 'c'), ('Seq', ('Seq', 'f', 'g'), ('Seq', 'h', 'i'))), ('Seq', 'd', 'e')), 'j')
    runStructuralTest('zstreamOrdNestedComplexStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)


def KCNestedStructuralTest():
    pattern = Pattern(
        KleeneClosureOperator(
            AndOperator(
                PrimitiveEventStructure("GOOG", "a"),
                SeqOperator(PrimitiveEventStructure("GOOG", "b"),
                            KleeneClosureOperator(PrimitiveEventStructure("GOOG", "c"),
                                                  min_size=1, max_size=5))
            ), min_size=1, max_size=3
        ),
        TrueCondition(),
        timedelta(minutes=3)
    )
    expected_result = ('KC', ('And', 'a', ('Seq', 'b', ('KC', 'c'))))
    runStructuralTest('KCNestedStructuralTest', [pattern], expected_result)


def NegationInNestedPatternTest_1(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            PrimitiveEventStructure("AMZN", "b"),
            AndOperator(
                NegationOperator(PrimitiveEventStructure("FB", "c")),
                PrimitiveEventStructure("GOOG", "d")
            )
        ),
        TrueCondition(),
        timedelta(minutes=1)
    )
    expected_result = ('Seq', ('Seq', 'a', 'b'), ('NAnd', 'd', 'c'))
    runStructuralTest('NegationInNestedPatternTest', [pattern], expected_result)
    runTest("NegationInNestedShort", [pattern], createTestFile, eventStream=nasdaqEventStreamShort)


def NegationInNestedPatternTest_2(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(PrimitiveEventStructure("FB", "c")),
                PrimitiveEventStructure("GOOG", "d"),
                NegationOperator(PrimitiveEventStructure("DRIV", "e"))
            )
        ),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.1, 0.2, 0.3, 0.4],
                         [0.1, 1.0, 0.12, 0.13, 0.14],
                         [0.2, 0.12, 1.0, 0.23, 0.24],
                         [0.3, 0.13, 0.23, 1.0, 0.34],
                         [0.4, 0.14, 0.24, 0.34, 1.0]]
    arrivalRates = [0.05, 0.1, 0.2, 0.3, 0.4]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.DYNAMIC_PROGRAMMING_LEFT_DEEP_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('Seq', ('NAnd', ('NAnd', ('And', 'b', 'd'), 'c'), 'e'), 'a')
    runStructuralTest('NegationInNestedPatternStructuralTest_1', [pattern], expected_result, eval_mechanism_params=eval_params)
    runTest("NegationInNested", [pattern], createTestFile, eval_params)


def NegationInNestedPatternZstreamTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(PrimitiveEventStructure("FB", "c")),
                PrimitiveEventStructure("GOOG", "d"),
                NegationOperator(PrimitiveEventStructure("DRIV", "e"))
            )
        ),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("d", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y)
        ),
        timedelta(minutes=3)
    )
    selectivityMatrix = [[1.0, 0.1, 0.2, 0.3, 0.4],
                         [0.1, 1.0, 0.12, 0.13, 0.14],
                         [0.2, 0.12, 1.0, 0.23, 0.24],
                         [0.3, 0.13, 0.23, 1.0, 0.34],
                         [0.4, 0.14, 0.24, 0.34, 1.0]]
    arrivalRates = [0.05, 0.1, 0.2, 0.3, 0.4]
    pattern.set_statistics({StatisticsTypes.ARRIVAL_RATES: arrivalRates,
                            StatisticsTypes.SELECTIVITY_MATRIX: selectivityMatrix})
    eval_params = TreeBasedEvaluationMechanismParameters(
        optimizer_params=OptimizerParameters(opt_type=OptimizerTypes.TRIVIAL_OPTIMIZER,
                                             tree_plan_params=TreePlanBuilderParameters(
                                                 TreePlanBuilderTypes.ORDERED_ZSTREAM_BUSHY_TREE)),
        storage_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    expected_result = ('Seq', ('NAnd', ('NAnd', ('And', 'b', 'd'), 'c'), 'e'), 'a')
    runStructuralTest('NegationInNestedPatternStructuralTest', [pattern], expected_result, eval_mechanism_params=eval_params)
    runTest("NegationInNested", [pattern], createTestFile, eval_params)


def NestedNegationSeqTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(
                    SeqOperator(
                        PrimitiveEventStructure("BIDU", "c"),
                        PrimitiveEventStructure("ORLY", "d")
                    )
                ),
                PrimitiveEventStructure("CERN", "e"),
                NegationOperator(PrimitiveEventStructure("DRIV", "f"))
            )
        ),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("b", lambda x: x["Opening Price"]),
                            Variable("e", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y)
        ),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('NAnd', ('NAnd', ('And', 'b', 'e'), ('Seq', 'c', 'd')), 'f'))
    runStructuralTest('NestedNegationSeqStructuralTest', [pattern], expected_result)
    runTest("NestedNegationSeq", [pattern], createTestFile)


def NestedNegationSeqAndTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(
                    SeqOperator(
                        PrimitiveEventStructure("FB", "c"),
                        PrimitiveEventStructure("ORLY", "d")
                    )
                ),
                PrimitiveEventStructure("GOOG", "e"),
                NegationOperator(PrimitiveEventStructure("DRIV", "f")),
                NegationOperator(
                    AndOperator(
                        PrimitiveEventStructure("ALTR", "g"),
                        PrimitiveEventStructure("AFFX", "h")
                    )
                )
            )
        ),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x > y),
            BinaryCondition(Variable("b", lambda x: x["Opening Price"]),
                            Variable("e", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x < y)
        ),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('NAnd', ('NAnd', ('NAnd', ('And', 'b', 'e'), ('Seq', 'c', 'd')), 'f'), ('And', 'g', 'h')))
    runStructuralTest('NestedNegationSeqAndStructuralTest_1', [pattern], expected_result)
    runTest("NestedNegationSeqAnd", [pattern], createTestFile)


def NestedNegationSeqAndStructuralTest():
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(
                    SeqOperator(
                        PrimitiveEventStructure("GOOG", "c"),
                        PrimitiveEventStructure("ORLY", "d"),
                        PrimitiveEventStructure("DRIV", "e")
                    )
                )
            ),
            PrimitiveEventStructure("FB", "f"),
            NegationOperator(
                AndOperator(
                    PrimitiveEventStructure("LOCM", "g"),
                    PrimitiveEventStructure("BIDU", "h")
                )
            )
        ),
        TrueCondition(),
        timedelta(minutes=3)
    )
    expected_result = ('NSeq', ('Seq', ('Seq', 'a', ('NAnd', 'b', ('Seq', ('Seq', 'c', 'd'), 'e'))), 'f'), ('And', 'g', 'h'))
    runStructuralTest('NestedNegationSeqAndStructuralTest_2', [pattern], expected_result)


def NestedNegationInNegationStructuralTest_1():
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(
                    SeqOperator(
                        PrimitiveEventStructure("GOOG", "c"),
                        PrimitiveEventStructure("ORLY", "d"),
                        NegationOperator(PrimitiveEventStructure("DRIV", "e"))
                    )
                ),
                PrimitiveEventStructure("FB", "f"),
                NegationOperator(
                    AndOperator(
                        PrimitiveEventStructure("LOCM", "g"),
                        PrimitiveEventStructure("BIDU", "h")
                    )
                )
            )
        ),
        TrueCondition(),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('NAnd', ('NAnd', ('And', 'b', 'f'), ('NSeq', ('Seq', 'c', 'd'), 'e')), ('And', 'g', 'h')))
    runStructuralTest('NestedNegationInNegationStructuralTest_1', [pattern], expected_result)


def NestedNegationInNegationStructuralTest_2():
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(
                    SeqOperator(
                        PrimitiveEventStructure("GOOG", "c"),
                        PrimitiveEventStructure("ORLY", "d"),
                        NegationOperator(
                            AndOperator(
                                PrimitiveEventStructure("DRIV", "e"),
                                PrimitiveEventStructure("AVID", "f")
                            )
                        )
                    )
                ),
                PrimitiveEventStructure("FB", "g"),
                NegationOperator(
                    AndOperator(
                        PrimitiveEventStructure("LOCM", "h"),
                        PrimitiveEventStructure("BIDU", "i")
                    )
                )
            )
        ),
        TrueCondition(),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('NAnd', ('NAnd', ('And', 'b', 'g'), ('NSeq', ('Seq', 'c', 'd'), ('And', 'e', 'f'))), ('And', 'h', 'i')))
    runStructuralTest('NestedNegationInNegationStructuralTest_2', [pattern], expected_result)


def NestedNegationWithKCTest(createTestFile=False):
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(
                    SeqOperator(
                        PrimitiveEventStructure("FB", "c"),
                        PrimitiveEventStructure("ORLY", "d"),
                        NegationOperator(
                            AndOperator(
                                PrimitiveEventStructure("DRIV", "e"),
                                PrimitiveEventStructure("AVID", "f")
                            )
                        )
                    )
                ),
                PrimitiveEventStructure("GOOG", "g"),
                NegationOperator(
                    KleeneClosureOperator(
                        PrimitiveEventStructure("LOCM", "h")
                    )
                )
            )
        ),
        TrueCondition(),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('NAnd', ('NAnd', ('And', 'b', 'g'), ('NSeq', ('Seq', 'c', 'd'), ('And', 'e', 'f'))), ('KC', 'h')))
    runStructuralTest('NestedNegationWithKCStructuralTest', [pattern], expected_result)
    runTest('NestedNegationWithKC', [pattern], createTestFile)


def NestedNegationWithNestedKCStructuralTest():
    pattern = Pattern(
        SeqOperator(
            PrimitiveEventStructure("AAPL", "a"),
            AndOperator(
                PrimitiveEventStructure("AMZN", "b"),
                NegationOperator(
                    SeqOperator(
                        PrimitiveEventStructure("GOOG", "c"),
                        PrimitiveEventStructure("ORLY", "d"),
                        NegationOperator(
                            AndOperator(
                                PrimitiveEventStructure("DRIV", "e"),
                                PrimitiveEventStructure("AVID", "f")
                            )
                        )
                    )
                ),
                PrimitiveEventStructure("FB", "g"),
                NegationOperator(
                    KleeneClosureOperator(
                        AndOperator(
                            PrimitiveEventStructure("LOCM", "h"),
                            PrimitiveEventStructure("BIDU", "i")
                        )
                    )
                )
            )
        ),
        TrueCondition(),
        timedelta(minutes=3)
    )
    expected_result = ('Seq', 'a', ('NAnd', ('NAnd', ('And', 'b', 'g'), ('NSeq', ('Seq', 'c', 'd'), ('And', 'e', 'f'))), ('KC', ('And', 'h', 'i'))))
    runStructuralTest('NestedNegationWithNestedKCStructuralTest', [pattern], expected_result)
