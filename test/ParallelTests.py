from test.testUtils import *
from plugin.sensors.Sensors import SensorsDataFormatter
from condition.Condition import Variable, BinaryCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import GreaterThanCondition, GreaterThanEqCondition, \
    SmallerThanEqCondition, SmallerThanCondition
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern
from parallel.ParallelExecutionParameters import *
from datetime import timedelta


# GroupByKey - HIRZEL Tests

def simpleGroupByKeyTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                         test_name="parallel_GroupByKey_1_"):
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
    units = 7
    parallel_execution_params = DataParallelExecutionParametersHirzelAlgorithm(units_number=units, key="Opening Price")
    runTest(test_name, [pattern], createTestFile, eval_mechanism_params, parallel_execution_params, eventStream=custom4)


def SensorsDataHIRZELTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                          test_name="Sensors_GroupBYKey_1_"):
    """
    PATTERN SEQ(a.MagX > b.AccX && a.MagY < b.AccY)
    WHERE   a.Amplitude == b.Amplitude
    WITHIN 3 minutes
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
        timedelta(minutes=3)
    )
    units = 8

    parallel_execution_params = DataParallelExecutionParametersHirzelAlgorithm(units_number=units, key="Amplitude")

    runTest(test_name, [pattern], createTestFile, eventStream=Sensors_data_short,
            eval_mechanism_params=eval_mechanism_params,
            data_formatter=SensorsDataFormatter(),
            parallel_execution_params=parallel_execution_params)


def GroupByKeyMultiPatternTest(createTestFile=False,
                               eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                               test_name="GroupByKey_MultiPatternTest_1_"):
    """Brings the matches either from the first pattern:
        (YHOO.Opening_price <  EBAY.Opening_price
        OR
        Matches from the second pattern:
        int(AMZ.peak_price) == int(74)
        in the defined time-deltas
    """
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("YHOO", "a"), PrimitiveEventStructure("EBAY", "b")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Peak Price"]),
                            Variable("b", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: int(x) == int(y)),
            SmallerThanCondition(Variable("a", lambda x: x["Opening Price"]),
                                 Variable("b", lambda x: x["Opening Price"]))),
        timedelta(minutes=3)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "x1")),
        BinaryCondition(left_term=Variable("x1", lambda x: int(x["Peak Price"])), right_term=74,
                        relation_op=lambda x: int(x) == 74),
        timedelta(days=1)
    )

    units = 8

    parallel_execution_params = DataParallelExecutionParametersHirzelAlgorithm(units_number=units, key="Peak Price")

    runTest(test_name, [pattern1, pattern2], createTestFile, eventStream=nasdaqEventStream,
            eval_mechanism_params=eval_mechanism_params,
            parallel_execution_params=parallel_execution_params)


# End GroupByKey tests

# RIP Tests
def simpleRIPTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                  test_name="parallel_2_"):
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
                                                                            multiple=12)
    runTest(test_name, [pattern], createTestFile, eval_mechanism_params, parallel_execution_params,
            eventStream=custom4)


def StocksDataRIPTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                      test_name="Stocks_Data_RIP_Test_"):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("GOOG", "b"),
                    PrimitiveEventStructure("GOOG", "c")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"])),
            SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                                 Variable("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("AMZN", "x1"), PrimitiveEventStructure("AMZN", "x2"),
                    PrimitiveEventStructure("AMZN", "x3")),
        AndCondition(
            SmallerThanEqCondition(Variable("x1", lambda x: x["Lowest Price"]), 75),
            GreaterThanEqCondition(Variable("x2", lambda x: x["Peak Price"]), 78),
            SmallerThanEqCondition(Variable("x3", lambda x: x["Lowest Price"]),
                                   Variable("x1", lambda x: x["Lowest Price"]))
        ),
        timedelta(days=1)
    )

    units = 8
    parallel_execution_params = DataParallelExecutionParametersRIPAlgorithm(units_number=units,
                                                                            multiple=26/24)
    runTest(test_name, [pattern1, pattern2], createTestFile, eventStream=nasdaqEventStream,
            eval_mechanism_params=eval_mechanism_params,
            parallel_execution_params=parallel_execution_params)
    runParallelTest(test_name, [pattern1, pattern2], createTestFile, eventStream=nasdaqEventStream,
                    eval_mechanism_params=eval_mechanism_params,
                    parallel_execution_params=parallel_execution_params)


def SensorsDataRIPTestShort(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                            test_name="Sensors_short_"):
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
                                                                            multiple=1.2)
    runTest(test_name, [pattern], createTestFile, eventStream=Sensors_data_short,
            eval_mechanism_params=eval_mechanism_params, data_formatter=SensorsDataFormatter(),
            parallel_execution_params=parallel_execution_params)
    runParallelTest(test_name, [pattern], createTestFile, eventStream=Sensors_data_short,
                    eval_mechanism_params=eval_mechanism_params, data_formatter=SensorsDataFormatter(),
                    parallel_execution_params=parallel_execution_params)


def SensorsDataRIPTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                       test_name="Sensors_"):
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
        timedelta(minutes=3)
    )
    units = 8
    parallel_execution_params = DataParallelExecutionParametersRIPAlgorithm(units_number=units,
                                                                            multiple=2)
    runTest(test_name, [pattern], createTestFile, eventStream=Sensors_data,
            eval_mechanism_params=eval_mechanism_params,
            parallel_execution_params=parallel_execution_params, data_formatter=SensorsDataFormatter())
    runParallelTest(test_name, [pattern], createTestFile, eventStream=Sensors_data,
                    eval_mechanism_params=eval_mechanism_params,
                    parallel_execution_params=parallel_execution_params, data_formatter=SensorsDataFormatter())


def SensorsDataRIPLongTime(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                           test_name="Sensors_long_time_"):
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
        timedelta(minutes=2)
    )
    units = 8
    parallel_execution_params = DataParallelExecutionParametersRIPAlgorithm(units_number=units,
                                                                            multiple=5.5)
    runTest(test_name, [pattern], createTestFile, eventStream=Sensors_data_longtime,
            eval_mechanism_params=eval_mechanism_params, data_formatter=SensorsDataFormatter(),
            parallel_execution_params=parallel_execution_params)
    runParallelTest(test_name, [pattern], createTestFile, eventStream=Sensors_data_longtime,
                    eval_mechanism_params=eval_mechanism_params,
                    parallel_execution_params=parallel_execution_params, data_formatter=SensorsDataFormatter())


# HyperCube Tests

def HyperCubeMultiPatternTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                              test_name="HyperCubeMultiPatternTest_"):
    pattern1 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "a"), PrimitiveEventStructure("AMZN", "b")),
        AndCondition(
            SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                                 Variable("b", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    pattern2 = Pattern(
        SeqOperator(PrimitiveEventStructure("GOOG", "x1"), PrimitiveEventStructure("AMZN", "x2")),
        AndCondition(
            GreaterThanEqCondition(Variable("x2", lambda x: x["Peak Price"]), 78)
        ),
        timedelta(days=1)
    )

    units = 8
    attributes_dict = {"AMZN": "Opening Price", "GOOG": "Peak Price"}

    parallel_execution_params = DataParallelExecutionParametersHyperCubeAlgorithm(units_number=units,
                                                                                  attributes_dict=attributes_dict)
    runTest(test_name, [pattern1, pattern2], createTestFile, eventStream=nasdaqEventStream,
            eval_mechanism_params=eval_mechanism_params,
            parallel_execution_params=parallel_execution_params)


def simpleHyperCubeTest(createTestFile=False, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                        test_name="simpleHyperCubeTest_"):
    """
    PATTERN SEQ(AppleStockPriceUpdate a, AmazonStockPriceUpdate b, AvidStockPriceUpdate c)
    WHERE   a.OpeningPrice > c.OpeningPrice
    WITHIN 5 minutes
    """
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("AVID", "c")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), Variable("c", lambda x: x["Opening Price"])),
        timedelta(minutes=5),
    )
    units = 8
    attributes_dict = {"AMZN": "Opening Price", "AAPL": "Peak Price"}
    parallel_execution_params = DataParallelExecutionParametersHyperCubeAlgorithm(units_number=units,
                                                                                  attributes_dict=attributes_dict)
    runTest(test_name, [pattern], createTestFile, parallel_execution_params=parallel_execution_params,
            eventStream=nasdaqEventStreamTiny)
    runParallelTest(test_name, [pattern], createTestFile, parallel_execution_params=parallel_execution_params,
                    eventStream=nasdaqEventStreamTiny)


def HyperCubeMultiAttrbutesTest(createTestFile=False,
                                eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                                test_name="HyperCubeMultyAttrbutes"):
    HyperCubeMultyAttrbutesPattern = Pattern(
        SeqOperator(PrimitiveEventStructure("MSFT", "a"), PrimitiveEventStructure("DRIV", "b")),
        AndCondition(
            BinaryCondition(Variable("a", lambda x: x["Peak Price"]),
                            Variable("b", lambda x: x["Peak Price"]),
                            relation_op=lambda x, y: x < y)
        ),
        timedelta(minutes=10)
    )
    units = 9
    attributes_dict = {"MSFT": ["Peak Price", "Opening Price"], "DRIV": "Peak Price"}
    parallel_execution_params = DataParallelExecutionParametersHyperCubeAlgorithm(units_number=units,
                                                                                  attributes_dict=attributes_dict)
    runTest(test_name, [HyperCubeMultyAttrbutesPattern], createTestFile, eval_mechanism_params,
            parallel_execution_params=parallel_execution_params)


def HyperCubeMultiEventTypesTest(createTestFile=False,
                                 eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                                 test_name="HyperCubeMultyEventTypes_"):
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
    units = 16
    attributes_dict = {"Magnetometer": "MagX", "Accelerometer": "AccX"}

    parallel_execution_params = DataParallelExecutionParametersHyperCubeAlgorithm(units_number=units,
                                                                                  attributes_dict=attributes_dict)
    runTest(test_name, [pattern], createTestFile, eval_mechanism_params,
            parallel_execution_params=parallel_execution_params,
            eventStream=Sensors_data_longtime,
            data_formatter=SensorsDataFormatter())
    runParallelTest(test_name, [pattern], createTestFile, eval_mechanism_params,
                    parallel_execution_params=parallel_execution_params,
                    eventStream=Sensors_data_longtime,
                    data_formatter=SensorsDataFormatter())


if __name__ == "__main__":
    runTest.over_all_time = 0
    # GroupByKey
    simpleGroupByKeyTest()
    SensorsDataHIRZELTest()
    GroupByKeyMultiPatternTest()
    # RIP
    simpleRIPTest()
    StocksDataRIPTest()
    SensorsDataRIPTestShort()
    SensorsDataRIPTest()
    SensorsDataRIPLongTime()
    # HyperCube
    simpleHyperCubeTest()
    HyperCubeMultiPatternTest()
    HyperCubeMultiAttrbutesTest()
    HyperCubeMultiEventTypesTest()
