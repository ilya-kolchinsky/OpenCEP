from CEP import CEP
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes, \
    IterativeImprovementEvaluationMechanismParameters
from misc.IOUtils import file_input, file_output, current_project_directory
from misc.Stocks import MetastockDataFormatter
from misc.Utils import generate_matches
from evaluation.LeftDeepTreeBuilders import *
from evaluation.BushyTreeBuilders import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, MulTerm, \
    EqFormula, IdentifierTerm, AtomicTerm, AndFormula, TrueFormula
from base.PatternStructure import AndOperator, SeqOperator, QItem
from base.Pattern import Pattern
import os

eventFileLocation = os.path.join(current_project_directory, 'EventFiles')
nasdaqEventStreamTiny = file_input(os.path.join(eventFileLocation, 'NASDAQ_TINY.txt'), MetastockDataFormatter())
nasdaqEventStreamShort = file_input(os.path.join(eventFileLocation, 'NASDAQ_SHORT.txt'), MetastockDataFormatter())
nasdaqEventStreamMedium = file_input(os.path.join(eventFileLocation, 'NASDAQ_MEDIUM.txt'), MetastockDataFormatter())
nasdaqEventStreamFrequencyTailored = file_input(os.path.join(eventFileLocation, 'NASDAQ_FREQUENCY_TAILORED.txt'),
                                                MetastockDataFormatter())
nasdaqEventStream_AAPL_AMZN_GOOG = file_input(os.path.join(eventFileLocation, 'NASDAQ_AAPL_AMZN_GOOG.txt'),
                                              MetastockDataFormatter())
nasdaqEventStream = file_input(os.path.join(eventFileLocation, 'NASDAQ_LONG.txt'), MetastockDataFormatter())


def closeFiles(file1, file2):
    file1.close()
    file2.close()


def fileCompare(pathA, pathB):
    file1 = open(pathA)
    file2 = open(pathB)
    file1List = []  # List of unique patterns
    file2List = []  # List of unique patterns
    lineStack = ""
    for line in file1:
        if not line.strip():
            lineStack += line
        elif not (lineStack in file1List):
            file1List.append(lineStack)
            lineStack = ""
    lineStack = ""
    for line in file2:
        if not line.strip():
            lineStack += line
        elif not (lineStack in file2List):
            file2List.append(lineStack)
            lineStack = ""
    if len(file1List) != len(file2List):  # Fast check
        closeFiles(file1, file2)
        return False
    for line in file1List:
        if not (line in file2List):
            closeFiles(file1, file2)
            return False
    for line in file2List:
        if not (line in file1List):
            closeFiles(file1, file2)
            return False
    closeFiles(file1, file2)
    return True


def createTest(testName, patterns, events=None):
    if events == None:
        events = nasdaqEventStream.duplicate()
    else:
        events = events.duplicate()
    pattern = patterns[0]
    matches = generate_matches(pattern, events)
    file_output(matches, os.path.join(current_project_directory, 'test', 'TestsExpected', '%sMatches.txt' % testName))
    print("Finished creating test %s" % testName)


def runTest(testName, patterns, createTestFile=False,
            eval_mechanism_type=EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
            eval_mechanism_params=None, events=None):
    if createTestFile:
        createTest(testName, patterns, events)
    if events is None:
        events = nasdaqEventStream.duplicate()
    else:
        events = events.duplicate()
    cep = CEP(patterns, eval_mechanism_type, eval_mechanism_params)
    running_time = cep.run(events)
    matches = cep.get_pattern_match_stream()
    file_output(matches, '%sMatches.txt' % testName)
    expected_matches_path = os.path.join(current_project_directory, 'TestsExpected', '%sMatches.txt' % testName)
    actual_matches_path = os.path.join(current_project_directory, 'Matches', '%sMatches.txt' % testName)
    print("Test %s result: %s, Time Passed: %s" % (testName,
                                                   "Succeeded" if fileCompare(actual_matches_path,
                                                                              expected_matches_path) else "Failed",
                                                   running_time))
    # os.remove(actual_matches_path)


def runStructuralTest(testName, patterns, expected_result,
                      eval_mechanism_type=EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                      eval_mechanism_params=None):
    # print('{} is a test to check the tree structure, without actually running a test'.format(testName))
    # print('place a breakpoint after creating the CEP object to debug it.\n')
    cep = CEP(patterns, eval_mechanism_type, eval_mechanism_params)
    print("Test %s result: %s" % (testName,"Succeeded" if
                                    cep.get_tree_structure_for_test() == expected_result else "Failed"))
