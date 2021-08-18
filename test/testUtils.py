import os
import pathlib
import sys

from CEP import CEP
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from adaptive.optimizer.OptimizerFactory import StatisticsDeviationAwareOptimizerParameters
from stream.Stream import OutputStream
from stream.FileStream import FileInputStream, FileOutputStream
from misc.Utils import generate_matches
from plan.TreeCostModels import TreeCostModels
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from plugin.stocks.Stocks import MetastockDataFormatter
from stream.FileStream import FileInputStream, FileOutputStream
from stream.Stream import OutputStream
from tree.PatternMatchStorage import TreeStorageParameters
from parallel.ParallelExecutionParameters import ParallelExecutionParameters
from parallel.ParallelExecutionModes import ParallelExecutionModes

currentPath = pathlib.Path(os.path.dirname(__file__))
absolutePath = str(currentPath.parent)
sys.path.append(absolutePath)

INCLUDE_BENCHMARKS = False
INCLUDE_TWITTER = False

nasdaqEventStreamTiny = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_TINY.txt"))
nasdaqEventStreamShort = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_SHORT.txt"))
nasdaqEventStreamMedium = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_MEDIUM.txt"))
nasdaqEventStreamFrequencyTailored = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_FREQUENCY_TAILORED.txt"))
nasdaqEventStream_AAPL_AMZN_GOOG = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_AAPL_AMZN_GOOG.txt"))
nasdaqEventStream = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_LONG.txt"))

nasdaqEventStreamHalfShort = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_HALF_SHORT.txt"))
custom = FileInputStream(os.path.join(absolutePath, "test/EventFiles/custom.txt"))
custom2 = FileInputStream(os.path.join(absolutePath, "test/EventFiles/custom2.txt"))
custom3 = FileInputStream(os.path.join(absolutePath, "test/EventFiles/custom3.txt"))
custom4 = FileInputStream(os.path.join(absolutePath, "test/EventFiles/custom4.txt"))

Sensors_data = FileInputStream(os.path.join(absolutePath, "test/EventFiles/Sensors.dat"))
Sensors_data_short = FileInputStream(os.path.join(absolutePath, "test/EventFiles/Sensors_short.dat"))
Sensors_data_longtime = FileInputStream(os.path.join(absolutePath, "test/EventFiles/Sensors_long_time.dat"))

nasdaqEventStreamKC = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_KC.txt"))

DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(storage_params=TreeStorageParameters(sort_storage=False,
                                                                                clean_up_interval=10,
                                                                                prioritize_sorting_by_timestamp=True),
                                           optimizer_params=StatisticsDeviationAwareOptimizerParameters(tree_plan_params
                                           =TreePlanBuilderParameters(
                                               TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                               TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL)))
"""
Default testing Data formatters
"""
DEFAULT_TESTING_DATA_FORMATTER = MetastockDataFormatter()


class FailedCounter:
    """
    This class helps tracking failed tests (if there are any).
    """
    counter = 0
    failed_tests = set()
    missing_combination = []

    def increase_counter(self):
        self.counter = self.counter + 1

    def print_counter(self):
        print(self.counter, "tests Failed")


num_failed_tests = FailedCounter()

def closeFiles(file1, file2):
    file1.close()
    file2.close()


def fileCompare(pathA, pathB):
    """
    Compare expected output and actual ouput
    :param path1: path to first file
    :param path2: path to second file
    :return: bool, True if the two files are equivalent
    """
    file1 = open(pathA)
    file2 = open(pathB)

    set1 = set()
    set2 = set()
    fillSet(file1, set1)
    fillSet(file2, set2)

    if len(set1) != len(set2):
        closeFiles(file1, file2)
        return False

    if set1 != set2:
        closeFiles(file1, file2)
        return False

    file1.seek(0)
    file2.seek(0)

    counterA = len([line.strip("\n") for line in file1 if line != "\n"])
    counterB = len([line.strip("\n") for line in file2 if line != "\n"])

    if counterA != counterB:
        closeFiles(file1, file2)
        return False

    closeFiles(file1, file2)
    return True


def fillSet(file, set: set):
    """
    fill a set, each element of the set is x consecutive lines of the file, with x = counter
    :param file:
    :param set:
    :param counter:
    :return:
    """
    list = []
    for line in file:
        if line == '\n':
            continue
        # solve a problem when no blank lines at end of file
        line = line.strip()
        list.append(line)
        # if we read 'counter' lines, we want to add it to the set, and continue with the next 'counter' lines
        if line == '\n':
            set.add(tuple(list))
            list = []


def outputTestFile(base_path: str, matches: list, output_file_name: str = 'matches.txt'):
    base_matches_directory = os.path.join(os.path.join(base_path), 'test', 'Matches')
    if not os.path.exists(base_matches_directory):
        os.makedirs(base_matches_directory, exist_ok=True)
    with open(os.path.join(base_matches_directory, output_file_name), 'w') as f:
        for match in matches:
            for event in match.events:
                f.write("%s\n" % event)
            f.write("\n")


def createTest(testName, patterns, events=None, eventStream=nasdaqEventStream):
    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()
    pattern = patterns[0]
    matches = generate_matches(pattern, events)
    outputTestFile(absolutePath, matches, '../TestsExpected/%sMatches.txt' % testName)
    print("Finished creating test %s" % testName)


def runTest(testName, patterns, createTestFile=False,
            eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
            parallel_execution_params: ParallelExecutionParameters = None,
            events=None, eventStream=nasdaqEventStream, expected_file_name=None,
            data_formatter=DEFAULT_TESTING_DATA_FORMATTER):
    if expected_file_name is None:
        expected_file_name = testName

    if createTestFile:
        createTest(testName, patterns, events, eventStream=eventStream)
    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()

    listShort = ["OneNotBegin", "MultipleNotBegin", "MultipleNotMiddle", "distinctPatterns"]
    listHalfShort = ["OneNotEnd", "MultipleNotEnd"]
    listCustom = ["MultipleNotBeginAndEnd", "NotEverywhere2"]
    listCustom2 = ["simpleNot"]
    if expected_file_name in listShort:
        events = nasdaqEventStreamShort.duplicate()
    elif expected_file_name in listHalfShort:
        events = nasdaqEventStreamHalfShort.duplicate()
    elif expected_file_name in listCustom:
        events = custom.duplicate()
    elif expected_file_name in listCustom2:
        events = custom2.duplicate()
    elif expected_file_name == "NotEverywhere":
        events = custom3.duplicate()

    cep = CEP(patterns, eval_mechanism_params, parallel_execution_params)

    base_matches_directory = os.path.join(absolutePath, 'test', 'Matches')
    output_file_name = "%sMatches.txt" % testName.split('|')[0]
    expected_output_file_name = "%sMatches.txt" % expected_file_name.split('|')[0]
    is_async = parallel_execution_params is not None and parallel_execution_params.execution_mode == ParallelExecutionModes.DATA_PARALLELISM
    matches_stream = FileOutputStream(base_matches_directory, output_file_name, is_async)
    running_time = cep.run(events, matches_stream, data_formatter)

    expected_matches_path = os.path.join(absolutePath, 'test', 'TestsExpected', expected_output_file_name)
    actual_matches_path = os.path.join(base_matches_directory, output_file_name)
    is_test_successful = fileCompare(actual_matches_path, expected_matches_path)
    print("Test %s result: %s, Time Passed: %s" % (testName.replace('|', ''),
                                                   "Succeeded" if is_test_successful else "Failed", running_time))
    runTest.over_all_time += running_time
    if is_test_successful:
        os.remove(actual_matches_path)
    else:
        num_failed_tests.increase_counter()
        num_failed_tests.failed_tests.add(testName)


"""
Input:
testName- name of the test
patterns- list of patterns
Output:
expected output file for the test.
"""


def createExpectedOutput(testName, patterns, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                         events=None, eventStream=nasdaqEventStream):
    curr_events = events
    expected_directory = os.path.join(absolutePath, 'test', 'TestsExpected')
    filenames = []
    for i in range(len(patterns)):
        next_events = curr_events.duplicate()
        cep = CEP(patterns[i], eval_mechanism_params)
        output_file_name = "%sMatches.txt" % (testName + str(i))
        filenames.append(output_file_name)
        matches_stream = FileOutputStream(expected_directory, output_file_name)
        cep.run(curr_events, matches_stream, DEFAULT_TESTING_DATA_FORMATTER)
        curr_events = next_events
    uniteFiles(testName, len(patterns))

    for filename in filenames:
        single_pattern_path = os.path.join(expected_directory, filename)
        os.remove(single_pattern_path)


def uniteFiles(testName, numOfPatterns):
    base_matches_directory = os.path.join(absolutePath, 'test', 'TestsExpected')
    output_file_name = "%sMatches.txt" % testName
    output_file = os.path.join(base_matches_directory, output_file_name)
    with open(output_file, 'w') as f:
        for i in range(numOfPatterns):
            prefix = "%d: " % (i + 1)
            prefix += '% s'
            input_file_name = "%sMatches.txt" % (testName + str(i))
            file = os.path.join(base_matches_directory, input_file_name)
            with open(file) as expFile:
                text = expFile.read()
            setexp = set(text.split('\n\n'))
            setexp.remove('')
            setexp = {prefix % j for j in setexp}
            for line in setexp:
                f.write(line)
                f.write('\n\n')


"""
This function runs multi-pattern CEP on the given list of patterns and prints
success or fail output.
"""


def runMultiTest(test_name, patterns, createTestFile=False,
                 eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                 events=None, eventStream=nasdaqEventStream, expected_file_name=None):
    if expected_file_name is None:
        expected_file_name = test_name

    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()

    listShort = ["multiplePatterns", "distinctPatterns", "MultipleNotBeginningShare", "multipleParentsForInternalNode"]
    listHalfShort = ["onePatternIncludesOther", "threeSharingSubtrees"]
    listCustom = []
    listCustom2 = ["FirstMultiPattern", "RootAndInner"]
    if expected_file_name in listShort:
        events = nasdaqEventStreamShort.duplicate()
    elif expected_file_name in listHalfShort:
        events = nasdaqEventStreamHalfShort.duplicate()
    elif expected_file_name in listCustom:
        events = custom.duplicate()
    elif expected_file_name in listCustom2:
        events = custom2.duplicate()
    elif expected_file_name == "NotEverywhere":
        events = custom3.duplicate()

    if createTestFile:
        createExpectedOutput(test_name, patterns, eval_mechanism_params, events.duplicate(), eventStream)

    cep = CEP(patterns, eval_mechanism_params)

    base_matches_directory = os.path.join(absolutePath, 'test', 'Matches')
    output_file_name = "%sMatches.txt" % test_name
    expected_output_file_name = "%sMatches.txt" % expected_file_name
    actual_matches_path = os.path.join(base_matches_directory, output_file_name)
    expected_matches_path = os.path.join(absolutePath, 'test', 'TestsExpected', expected_output_file_name)
    matches_stream = FileOutputStream(base_matches_directory, output_file_name)
    running_time = cep.run(events, matches_stream, DEFAULT_TESTING_DATA_FORMATTER)

    match_set = [set() for i in range(len(patterns))]
    with open(actual_matches_path) as matchFile:
        all_matches = matchFile.read()
    match_list = all_matches.split('\n\n')
    for match in match_list:
        if match:
            match_set[int(match.partition(':')[0]) - 1].add(match.strip()[match.index(' ') + 1:])

    exp_set = [set() for i in range(len(patterns))]
    with open(expected_matches_path) as expFile:
        all_exp_matches = expFile.read()
    exp_match_list = all_exp_matches.split('\n\n')
    for match in exp_match_list:
        if match:
            exp_set[int(match.partition(':')[0]) - 1].add(match.strip()[match.index(' ') + 1:])

    res = (exp_set == match_set)
    print("Test %s result: %s, Time Passed: %s" % (test_name,
          "Succeeded" if res else "Failed", running_time))
    runTest.over_all_time += running_time
    if res:
        os.remove(actual_matches_path)
    else:
        num_failed_tests.increase_counter()
        num_failed_tests.failed_tests.add(test_name)


class DummyOutputStream(OutputStream):
    def add_item(self, item: object):
        pass


def runBenchMark(testName, patterns, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS, events=None):
    """
    this runs a bench mark ,since some outputs for benchmarks are very large,
    we assume correct functionality and only check runtimes. (not a test)
    """
    if events is None:
        events = nasdaqEventStream.duplicate()
    else:
        events = events.duplicate()
    cep = CEP(patterns, eval_mechanism_params)
    running_time = cep.run(events, DummyOutputStream(), DEFAULT_TESTING_DATA_FORMATTER)
    print("Bench Mark %s completed, Time Passed: %s" % (testName, running_time))
    runTest.over_all_time += running_time


def runStructuralTest(testName, patterns, expected_result,
                      eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                      parallel_execution_params: ParallelExecutionParameters = None, ):
    # print('{} is a test to check the tree structure, without actually running a test'.format(testName))
    # print('place a breakpoint after creating the CEP object to debug it.\n')
    cep = CEP(patterns, eval_mechanism_params, parallel_execution_params)
    structure_summary = cep.get_evaluation_mechanism_structure_summary()
    success = structure_summary == expected_result
    print("Test %s result: %s" % (testName, "Succeeded" if success else "Failed"))
    if not success:
        num_failed_tests.increase_counter()
        num_failed_tests.failed_tests.add(testName)


from unittest.mock import patch
from parallel.data_parallel.DataParallelExecutionAlgorithm import *
from base.PatternMatch import PatternMatch
from dataclasses import dataclass, field


class runParallelTest:
    @dataclass
    class Unit:
        events_list: List[str] = field(default_factory=list)
        filtered_matches_list: List[PatternMatch] = field(default_factory=list)
        unfiltered_matches_list: List[PatternMatch] = field(default_factory=list)

        def format(self, data_formatter):
            events = [Event(e, data_formatter) for e in self.events_list]
            events.sort(key=lambda e: e.timestamp)
            self.filtered_matches_list.sort(key=lambda mat: (mat.first_timestamp, mat.last_timestamp))
            self.unfiltered_matches_list.sort(key=lambda mat: (mat.first_timestamp, mat.last_timestamp))
            result = "filtered size = " + str(len(self.filtered_matches_list)) + "\n"
            result += "unfiltered size = " + str(len(self.unfiltered_matches_list)) + "\n"
            result += '\tevents:\n'
            for event in events:
                result += f'\t\t{event}\n'
            result += '\tfiltered_matches:\n'
            for filtered_matche in self.filtered_matches_list:
                for ev in filtered_matche.events:
                    result += f'\t\t{ev}\n'
                result += '\n'
            result += '\tunfiltered_matches:\n'
            for unfiltered_matche in self.unfiltered_matches_list:
                for ev in unfiltered_matche.events:
                    result += f'\t\t{ev}\n'
                result += '\n'
            result += '\n'
            return result

    def new_add_item(self, item: PatternMatch):
        if self.skip_item(item):
            runParallelTest.units[self.unit_id].filtered_matches_list.append(item)
        else:
            runParallelTest.units[self.unit_id].unfiltered_matches_list.append(item)
            self.matches.add_item(item)

    def new_add_event(self, raw_event):
        runParallelTest.units[self.unit_id].events_list.append(raw_event)
        self.events.add_item(raw_event)

    @staticmethod
    @patch.object(DataParallelExecutionAlgorithm.ExecutionUnit, 'add_event', new=new_add_event)
    @patch.object(DataParallelExecutionAlgorithm.FilterStream, 'add_item', new=new_add_item)
    def __init__(testName, patterns, createTestFile=False,
                 eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                 parallel_execution_params: ParallelExecutionParameters = None,
                 events=None, eventStream=nasdaqEventStream, expected_file_name=None,
                 data_formatter=DEFAULT_TESTING_DATA_FORMATTER):
        testName += "ParallelTest"
        if not parallel_execution_params or parallel_execution_params.execution_mode != ParallelExecutionModes.DATA_PARALLELISM:
            raise Exception(f"parallel_execution_params.execution_mode must be DATA_PARALLELISM")
        runParallelTest.units = [runParallelTest.Unit() for _ in range(parallel_execution_params.units_number)]

        if expected_file_name is None:
            expected_file_name = testName

        if createTestFile:
            createTest(testName, patterns, events, eventStream=eventStream)
        if events is None:
            events = eventStream.duplicate()
        else:
            events = events.duplicate()

        listShort = ["OneNotBegin", "MultipleNotBegin", "MultipleNotMiddle", "distinctPatterns"]
        listHalfShort = ["OneNotEnd", "MultipleNotEnd"]
        listCustom = ["MultipleNotBeginAndEnd", "NotEverywhere2"]
        listCustom2 = ["simpleNot"]
        if expected_file_name in listShort:
            events = nasdaqEventStreamShort.duplicate()
        elif expected_file_name in listHalfShort:
            events = nasdaqEventStreamHalfShort.duplicate()
        elif expected_file_name in listCustom:
            events = custom.duplicate()
        elif expected_file_name in listCustom2:
            events = custom2.duplicate()
        elif expected_file_name == "NotEverywhere":
            events = custom3.duplicate()

        cep = CEP(patterns, eval_mechanism_params, parallel_execution_params)

        base_matches_directory = os.path.join(absolutePath, 'test', 'Matches')
        output_file_name = f"{testName.split('|')[0]}_Matches.txt"
        expected_output_file_name = f"{expected_file_name.split('|')[0]}_Matches.txt"
        running_time = cep.run(events, OutputStream(), data_formatter)

        with open(os.path.join(base_matches_directory, output_file_name), 'w') as output:
            for unit_id, unit in enumerate(runParallelTest.units):
                output.write(f'unit_id={unit_id} \n')
                output.write(unit.format(data_formatter))

        expected_matches_path = os.path.join(absolutePath, 'test', 'TestsExpected', expected_output_file_name)
        actual_matches_path = os.path.join(base_matches_directory, output_file_name)

        is_test_successful = fileCompare(actual_matches_path, expected_matches_path)
        print("Test %s result: %s, Time Passed: %s" % (testName.replace('|', ''),
                                                       "Succeeded" if is_test_successful else "Failed", running_time))
        runTest.over_all_time += running_time
        if is_test_successful:
            os.remove(actual_matches_path)
        else:
            num_failed_tests.increase_counter()
            num_failed_tests.failed_tests.add(testName)

        runParallelTest.units = list()

