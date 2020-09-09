import os
import pathlib
import sys

from CEP import CEP
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes
from misc.IOUtils import file_input, file_output
from misc.Utils import generate_matches
from plugin.stocks.Stocks import MetastockDataFormatter

currentPath = pathlib.Path(os.path.dirname(__file__))
absolutePath = str(currentPath.parent)
sys.path.append(absolutePath)

INCLUDE_BENCHMARKS = False

nasdaqEventStreamTiny = file_input(absolutePath, os.path.join("test", "EventFiles", "NASDAQ_TINY.txt"), MetastockDataFormatter())
nasdaqEventStreamShort = file_input(absolutePath, os.path.join("test", "EventFiles", "NASDAQ_SHORT.txt"), MetastockDataFormatter())
nasdaqEventStreamMedium = file_input(absolutePath, os.path.join("test", "EventFiles", "NASDAQ_MEDIUM.txt"), MetastockDataFormatter())
nasdaqEventStreamFrequencyTailored = file_input(absolutePath, os.path.join("test", "EventFiles", "NASDAQ_FREQUENCY_TAILORED.txt"), MetastockDataFormatter())
nasdaqEventStream_AAPL_AMZN_GOOG = file_input(absolutePath, os.path.join("test", "EventFiles", "NASDAQ_AAPL_AMZN_GOOG.txt"), MetastockDataFormatter())
nasdaqEventStream = file_input(absolutePath, os.path.join("test", "EventFiles", "NASDAQ_LONG.txt"), MetastockDataFormatter())

nasdaqEventStreamHalfShort = file_input(absolutePath, os.path.join("test", "EventFiles", "NASDAQ_HALF_SHORT.txt"), MetastockDataFormatter())
custom = file_input(absolutePath, os.path.join("test", "EventFiles", "custom.txt"), MetastockDataFormatter())
custom2 = file_input(absolutePath, os.path.join("test", "EventFiles", "custom2.txt"), MetastockDataFormatter())
custom3 = file_input(absolutePath, os.path.join("test", "EventFiles", "custom3.txt"), MetastockDataFormatter())
nasdaqEventStreamKC = file_input(absolutePath, os.path.join("test", "EventFiles", "NASDAQ_KC.txt"), MetastockDataFormatter())

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


def createTest(testName, patterns, events=None, eventStream = nasdaqEventStream):
    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()
    pattern = patterns[0]
    matches = generate_matches(pattern, events)
    file_output(absolutePath, matches, os.path.join('..', 'TestsExpected/%sMatches.txt' % testName))
    print("Finished creating test %s" % testName)


def runTest(testName, patterns, createTestFile = False,
            eval_mechanism_type = EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
            eval_mechanism_params = None, events = None, eventStream = nasdaqEventStream):
    if createTestFile:
        createTest(testName, patterns, events, eventStream = eventStream)
    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()

    listShort = ["OneNotBegin", "MultipleNotBegin", "MultipleNotMiddle"]
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

    cep = CEP(patterns, eval_mechanism_type, eval_mechanism_params)
    running_time = cep.run(events)
    matches = cep.get_pattern_match_stream()
    file_output(absolutePath, matches, '%sMatches.txt' % testName)
    expected_matches_path = os.path.join("TestsExpected", "%sMatches.txt" % testName)
    actual_matches_path = os.path.join("Matches", "%sMatches.txt" % testName)
    print("Test %s result: %s, Time Passed: %s" % (testName,
          "Succeeded" if fileCompare(actual_matches_path, expected_matches_path) else "Failed", running_time))
    runTest.over_all_time += running_time
    #os.remove(absolutePath + "\\" + actual_matches_path)

def runBenchMark(testName, patterns, eval_mechanism_type=EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                 eval_mechanism_params=None, events=None):
    """
    this runs a bench mark ,since some outputs for benchmarks are very large,
    we assume correct functionality and only check runtimes. (not a test)
    """
    if events is None:
        events = nasdaqEventStream.duplicate()
    else:
        events = events.duplicate()
    cep = CEP(patterns, eval_mechanism_type, eval_mechanism_params)
    running_time = cep.run(events)
    print("Bench Mark %s completed, Time Passed: %s" % (testName, running_time))
    runTest.over_all_time += running_time

def runStructuralTest(testName, patterns, expected_result,
                      eval_mechanism_type=EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                      eval_mechanism_params=None):
    # print('{} is a test to check the tree structure, without actually running a test'.format(testName))
    # print('place a breakpoint after creating the CEP object to debug it.\n')
    cep = CEP(patterns, eval_mechanism_type, eval_mechanism_params)
    structure_summary = cep.get_evaluation_mechanism_structure_summary()
    print("Test %s result: %s" % (testName,"Succeeded" if structure_summary == expected_result else "Failed"))
