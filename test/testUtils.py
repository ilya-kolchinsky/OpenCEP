import os
import pathlib
import sys

from CEP import CEP
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from stream.Stream import OutputStream
from stream.FileStream import FileInputStream, FileOutputStream
from misc.Utils import generate_matches
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters
from plan.TreeCostModels import TreeCostModels
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes
from plugin.stocks.Stocks import MetastockDataFormatter
from tree.PatternMatchStorage import TreeStorageParameters

currentPath = pathlib.Path(os.path.dirname(__file__))
absolutePath = str(currentPath.parent)
sys.path.append(absolutePath)

INCLUDE_BENCHMARKS = False

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

nasdaqEventStreamKC = FileInputStream(os.path.join(absolutePath, "test/EventFiles/NASDAQ_KC.txt"))

DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS = \
    TreeBasedEvaluationMechanismParameters(TreePlanBuilderParameters(TreePlanBuilderTypes.TRIVIAL_LEFT_DEEP_TREE,
                                                                     TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL),
                                           TreeStorageParameters(sort_storage=False,
                                                                 clean_up_interval=10,
                                                                 prioritize_sorting_by_timestamp=True))
DEFAULT_TESTING_DATA_FORMATTER = MetastockDataFormatter()

def numOfLinesInPattern(file):
    """
    get num of lines in file until first blank line == num of lines in pattern
    :param file: file
    :return: int
    """
    counter = 0
    for line in file:
        if line == '\n':
            break
        counter = counter + 1
    return counter


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

    counter1 = numOfLinesInPattern(file1)
    counter2 = numOfLinesInPattern(file2)

    file1.seek(0)
    file2.seek(0)

    # quick check, if both files don't return the same counter, or if both files are empty
    if counter1 != counter2:
        closeFiles(file1, file2)
        return False
    elif counter1 == counter2 and counter1 == 0:
        closeFiles(file1, file2)
        return True

    set1 = set()
    set2 = set()

    fillSet(file1, set1, counter1)
    fillSet(file2, set2, counter2)
    closeFiles(file1, file2)

    return set1 == set2


def fillSet(file, set: set, counter: int):
    """
    fill a set, each element of the set is x consecutive lines of the file, with x = counter
    :param file:
    :param set:
    :param counter:
    :return:
    """
    list = []
    tmp = 0
    for line in file:
        if line == '\n':
            continue
        # solve a problem when no blank lines at end of file
        line = line.strip()
        list.append(line)
        tmp = tmp + 1
        # if we read 'counter' lines, we want to add it to the set, and continue with the next 'counter' lines
        if tmp == counter:
            set.add(tuple(list))
            list = []
            tmp = 0


def outputTestFile(base_path: str, matches: list, output_file_name: str = 'matches.txt'):
    base_matches_directory = os.path.join(os.path.join(base_path), 'test', 'Matches')
    if not os.path.exists(base_matches_directory):
        os.makedirs(base_matches_directory, exist_ok=True)
    with open(os.path.join(base_matches_directory, output_file_name), 'w') as f:
        for match in matches:
            for event in match.events:
                f.write("%s\n" % event)
            f.write("\n")


def createTest(testName, patterns, events=None, eventStream = nasdaqEventStream):
    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()
    pattern = patterns[0]
    matches = generate_matches(pattern, events)
    outputTestFile(absolutePath, matches, '../TestsExpected/%sMatches.txt' % testName)
    print("Finished creating test %s" % testName)


def runTest(testName, patterns, createTestFile = False,
            eval_mechanism_params = DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
            events = None, eventStream = nasdaqEventStream):
    if createTestFile:
        createTest(testName, patterns, events, eventStream = eventStream)
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

    cep = CEP(patterns, eval_mechanism_params)

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
def runMultiTest(testName, patterns, createTestFile = False,
            eval_mechanism_params = DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
            events = None, eventStream = nasdaqEventStream):

    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()

    listShort = ["multiplePatterns", "distinctPatterns", "MultipleNotBeginningShare", "multipleParentsForInternalNode"]
    listHalfShort = ["onePatternIncludesOther", "threeSharingSubtrees"]
    listCustom = []
    listCustom2 = ["FirstMultiPattern", "RootAndInner"]
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

    if createTestFile:
        createExpectedOutput(testName, patterns, eval_mechanism_params, events.duplicate(), eventStream)

    cep = CEP(patterns, eval_mechanism_params)

    base_matches_directory = os.path.join(absolutePath, 'test', 'Matches')
    output_file_name = "%sMatches.txt" % testName
    actual_matches_path = os.path.join(base_matches_directory, output_file_name)
    expected_matches_path = os.path.join(absolutePath, 'test', 'TestsExpected', output_file_name)
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
    print("Test %s result: %s, Time Passed: %s" % (testName,
          "Succeeded" if res else "Failed", running_time))
    runTest.over_all_time += running_time
    os.remove(actual_matches_path)


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
                      eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS):
    # print('{} is a test to check the tree structure, without actually running a test'.format(testName))
    # print('place a breakpoint after creating the CEP object to debug it.\n')
    cep = CEP(patterns, eval_mechanism_params)
    structure_summary = cep.get_evaluation_mechanism_structure_summary()
    print("Test %s result: %s" % (testName,"Succeeded" if structure_summary == expected_result else "Failed"))
