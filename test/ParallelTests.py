from test.testUtils import *
from datetime import timedelta
from plugin.sensors.Sensors import SensorsDataFormatter
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, \
    SmallerThanEqCondition, SmallerThanCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern
from parallel.ParallelExecutionParameters import *
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
    parallel_execution_params = DataParallelExecutionParametersHirzelAlgorithm(units_number=units, key="Opening Price",
                                                                               debug=True)
    runTest(test_name, [pattern], createTestFile, eval_mechanism_params, parallel_execution_params, eventStream=custom4)
    expected_result = tuple([('Seq', 'a', 'b')] * units)
    runStructuralTest('structuralTest1', [pattern], expected_result,
                      parallel_execution_params=parallel_execution_params)


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
    parallel_execution_params = DataParallelExecutionParametersRIPAlgorithm(units_number=units,
                                                                            interval=timedelta(minutes=60))

    runTest(test_name, [pattern], createTestFile, eval_mechanism_params, parallel_execution_params,
            eventStream=custom4)
    expected_result = tuple([('Seq', 'a', 'b')] * units)
    # runStructuralTest('structuralTest1', [pattern], expected_result,
    #                  parallel_execution_params=parallel_execution_params)


def SensorsDataRIPTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                       test_name="Sensors_"):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b)
    WHERE   a.OpeningPrice == b.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("Magnetometer", "a"),
                    PrimitiveEventStructure("Accelerometer", "b")),
        AndCondition(
            GreaterThanCondition(Variable("a", lambda x: x["MagX"]),
                                 Variable("b", lambda x: x["AccX"])),
            SmallerThanCondition(Variable("a", lambda x: x["MagY"]),
                                 Variable("b", lambda x: x["AccY"])),
            BinaryCondition(Variable("a", lambda x: x["Amplitude"]),
                            Variable("b", lambda x: x["Amplitude"]),
                            relation_op=lambda x, y: x == y),
        ),
        timedelta(minutes=5)
    )
    units = 8
    parallel_execution_params = DataParallelExecutionParametersRIPAlgorithm(units_number=units,
                                                                            interval=timedelta(minutes=60),
                                                                            debug=True)
    runTest(test_name, [pattern], createTestFile, eventStream=Sensors_data, eval_mechanism_params=eval_mechanism_params,
            data_formatter=SensorsDataFormatter())
    runTest(test_name, [pattern], createTestFile, eventStream=Sensors_data, eval_mechanism_params=eval_mechanism_params,
            parallel_execution_params=parallel_execution_params, data_formatter=SensorsDataFormatter())
    # expected_result = tuple([('Seq', 'a', 'b')] * units)
    # runStructuralTest('structuralTest1', [pattern], expected_result,
    #                   parallel_execution_params=parallel_execution_params)


def simpleHyperCubeTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
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
    units = 30
    attributes_dict = {"AAPL": ["Opening Price", "Peak Price"], "AMZN": "Peak Price"}
    parallel_execution_params = DataParallelExecutionParametersHyperCubeAlgorithm(units_number=units,
                                                                                  attributes_dict=attributes_dict,
                                                                                  debug=True)
    runTest(test_name, [pattern], createTestFile, eval_mechanism_params, parallel_execution_params, eventStream=custom4)
    # expected_result = tuple([('Seq', 'a', 'b')] * units)
    # runStructuralTest('structuralTest1', [pattern], expected_result, parallel_execution_params=parallel_execution_params)


if __name__ == "__main__":
    runTest.over_all_time = 0
    # simpleGroupByKeyTest()
    # simpleRIPTest()
    SensorsDataRIPTest()
    # simpleHyperCubeTest()
