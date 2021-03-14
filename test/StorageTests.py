from datetime import timedelta

from condition.BaseRelationCondition import GreaterThanCondition, GreaterThanEqCondition
from test.testUtils import *
from condition.Condition import Variable
from condition.CompositeCondition import AndCondition
from base.PatternStructure import AndOperator, PrimitiveEventStructure
from base.Pattern import Pattern

def sortedStorageTest(createTestFile=False):
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
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters()),
        storage_params=storage_params)
    runTest("sortedStorageTest", [pattern], createTestFile, eval_mechanism_params=eval_params, events=nasdaqEventStream)


def sortedStorageBenchMarkTest(createTestFile=False):
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
        optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params=TreePlanBuilderParameters()),
        storage_params=storage_params)

    runBenchMark("sortedStorageBenchMark - sorted storage", [pattern], eval_mechanism_params=eval_params)
