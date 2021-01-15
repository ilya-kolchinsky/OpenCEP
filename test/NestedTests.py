from misc.StatisticsTypes import StatisticsTypes
from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, SmallerThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern

def nestedTest(createTestFile=False):
    """
    TODO: comment
    """
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
    runTest("nested", [pattern], createTestFile)

def nestedAscendingTest(createTestFile=False):
    """
    TODO: comment
    """
    pattern = Pattern(
        AndOperator(SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
                    SeqOperator(PrimitiveEventStructure("AVID", "c"), PrimitiveEventStructure("BIDU", "d")),
                    AndOperator(PrimitiveEventStructure("GOOG", "e"), PrimitiveEventStructure("AAPL", "f")),
                    PrimitiveEventStructure("GOOG", "g"),
                    SeqOperator(PrimitiveEventStructure("AMZN", "h"), PrimitiveEventStructure("BIDU", "i"))),
        TrueCondition(),
        timedelta(minutes=1)
    )
    pattern.set_statistics(StatisticsTypes.ARRIVAL_RATES, [0.11, 0.2, 0.3, 0.4, 0.5, 0.11, 0.5, 0.2, 0.4])
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.SORT_BY_FREQUENCY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest("nestedAscending", [pattern], createTestFile, eval_params)

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
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivityMatrix, arrivalRates))
    eval_params = TreeBasedEvaluationMechanismParameters(
        TreePlanBuilderParameters(TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE),
        DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS.storage_params
    )
    runTest('greedyNested', [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)