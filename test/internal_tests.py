from test.testUtils import *
from datetime import timedelta
from condition.Condition import Variable, TrueCondition, BinaryCondition, SimpleCondition
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition, GreaterThanCondition, GreaterThanEqCondition, SmallerThanEqCondition
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from base.Pattern import Pattern
from parallel.ParallelExecutionParameters import *

def stream_test():
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a")),
        GreaterThanCondition(Variable("a", lambda x: x["Opening Price"]), 135),
        timedelta(minutes=0.5)
    )
    runTest(testName="stream", patterns=[pattern], createTestFile=False,
            parallel_execution_params=ParallelExecutionParameters(ParallelExecutionModes.DATA_PARALLELISM),
            data_parallel_params=DataParallelExecutionParameters(num_threads=6))


def runTest(testName, patterns, createTestFile = False,
            eval_mechanism_params = DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
            events = None, eventStream = nasdaqEventStream,
            parallel_execution_params: ParallelExecutionParameters = None,
            data_parallel_params: DataParallelExecutionParameters = None
            ):
    if createTestFile:
        createTest(testName, patterns, events, eventStream=eventStream)
    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()

    listShort = ["OneNotBegin", "MultipleNotBegin", "MultipleNotMiddle", "distinctPatterns"]
    listHalfShort = ["OneNotEnd", "MultipleNotEnd"]
    listCustom = ["MultipleNotBeginAndEnd"]
    listCustom2 = ["simpleNot"]
    if testName in listShort:
        events = nasdaqEventStreamShort.duplicate()
    elif testName in listHalfShort:
        events = nasdaqEventStreamHalfShort.duplicate()
    elif testName in listCustom:
        events = custom.duplicate()
    elif testName in listCustom2:
        events = custom2.duplicate()
    elif testName == "NotEverywhere":
        events = custom3.duplicate()

    cep = CEP(patterns, eval_mechanism_params, parallel_execution_params, data_parallel_params)

    base_matches_directory = os.path.join(absolutePath, 'test', 'Matches')
    output_file_name = "%sMatches.txt" % testName
    matches_stream = FileOutputStream(base_matches_directory, output_file_name)
    running_time = cep.run(events, matches_stream, DEFAULT_TESTING_DATA_FORMATTER)
    expected_matches_path = os.path.join(absolutePath, 'test', 'TestsExpected', output_file_name)
    actual_matches_path = os.path.join(base_matches_directory, output_file_name)
    is_test_successful = fileCompare(actual_matches_path, expected_matches_path)
    print("Test %s result: %s, Time Passed: %s" % (testName,
                                                   "Succeeded" if is_test_successful else "Failed", running_time))
    runTest.over_all_time += running_time
    if is_test_successful:
       os.remove(actual_matches_path)