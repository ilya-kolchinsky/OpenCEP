from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, \
    SmallerThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern
from parallel.ParallelExecutionParameters import DataParallelExecutionParametersHirzelAlgorithm, DataParallelExecutionParametersRIPAlgorithm
from datetime import datetime, timedelta


def simpleGroupByKeyTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                              test_name="parallel_1_"):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b)
    WHERE   a.OpeningPrice == b.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x == y)
        ),
        timedelta(minutes=5)
    )
    units = 8
    parallel_execution_params = DataParallelExecutionParametersHirzelAlgorithm(units_number=units, key="Opening Price")
    runTest(test_name, [pattern], createTestFile, eval_mechanism_params, parallel_execution_params, eventStream=custom4)
    expected_result = tuple([('Seq', 'a', 'b')] * units)
    runStructuralTest('structuralTest1', [pattern], expected_result, parallel_execution_params=parallel_execution_params)


def simpleRIPTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                         test_name="parallel_2_"):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b)
    WHERE   a.OpeningPrice == b.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Opening Price"]),
                            Variable("b", lambda x: x["Opening Price"]),
                            relation_op=lambda x, y: x == y)
        ),
        timedelta(minutes=5)
    )
    units = 8
    parallel_execution_params = DataParallelExecutionParametersRIPAlgorithm(units_number=units, interval=timedelta(minutes=60))

    runTest(test_name, [pattern], createTestFile, eval_mechanism_params, parallel_execution_params,
            eventStream=custom4)
    expected_result = tuple([('Seq', 'a', 'b')] * units)
    runStructuralTest('structuralTest1', [pattern], expected_result,
                      parallel_execution_params=parallel_execution_params)


if __name__ == "__main__":
    runTest.over_all_time = 0
    simpleRIPTest()
