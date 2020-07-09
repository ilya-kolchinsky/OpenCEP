import os
from CEP import CEP
from evaluation.EvaluationMechanismFactory import EvaluationMechanismTypes, \
    IterativeImprovementEvaluationMechanismParameters
from misc.IOUtils import file_input, file_output
from misc.Tweets import MetatweetDataFormatter
from misc.Utils import generate_matches
from evaluation.LeftDeepTreeBuilders import *
from evaluation.BushyTreeBuilders import *
from datetime import timedelta
from base.Formula import GreaterThanFormula, SmallerThanFormula, SmallerThanEqFormula, GreaterThanEqFormula, MulTerm, \
    EqFormula, IdentifierTerm, AtomicTerm, AndFormula, TrueFormula
from base.PatternStructure import AndOperator, SeqOperator, QItem
from base.Pattern import Pattern
from misc.IOUtils import TweetsStreamSessionInput

#tweetEventStream = file_input("../test/TweetEventFiles/JsonExampleTweets.txt", MetatweetDataFormatter())
streaming = TweetsStreamSessionInput()
tweepy = streaming.get_tweepy_stream()
tweepy.filter(track=['liverpool'], is_async=True)
tweetEventStream = streaming.get_stream_queue()


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
        events = tweetEventStream.duplicate()
    else:
        events = events.duplicate()
    pattern = patterns[0]
    matches = generate_matches(pattern, events)
    file_output(matches, 'TestsExpected/%sMatches.txt' % testName)
    print("Finished creating test %s" % testName)


def runTest(testName, patterns, createTestFile=False,
            eval_mechanism_type=EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
            eval_mechanism_params=None, events=None):
    if createTestFile:
        createTest(testName, patterns, events)
    if events is None:
        events = tweetEventStream.duplicate()
    else:
        events = events.duplicate()
    cep = CEP(patterns, eval_mechanism_type, eval_mechanism_params)
    print("file output1")
    running_time = cep.run(events, is_tweeter_streaming=True, file_path="../test/TestsExpected/output.txt")
    print("file output2")
    matches = cep.get_pattern_match_stream()
    print("file output")
    file_output(matches, '%sMatches.txt' % testName)
    expected_matches_path = "../test/TestsExpected/%sMatches.txt" % testName
    actual_matches_path = "../test/Matches/%sMatches.txt" % testName
    print("Test %s result: %s, Time Passed: %s" % (testName,
                                                   "Succeeded" if fileCompare(actual_matches_path,
                                                                              expected_matches_path) else "Failed",
                                                   running_time))
    os.remove(actual_matches_path)


def oneArgumentsearchTestTweet(createTestFile=False):
    pattern = Pattern(
        SeqOperator([QItem("2", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["ID"]), AtomicTerm(1)),
        timedelta.max
    )
    runTest("one", [pattern], createTestFile)

oneArgumentsearchTestTweet()
