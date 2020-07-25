from itertools import chain
from datetime import datetime
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


def runStructuralTest(testName, patterns,
                      eval_mechanism_type=EvaluationMechanismTypes.TRIVIAL_LEFT_DEEP_TREE,
                      eval_mechanism_params=None):
    print('{} is a test to check the tree structure, without actually running a test'.format(testName))
    print('place a breakpoint after creating the CEP object to debug it.\n')
    cep = CEP(patterns, eval_mechanism_type, eval_mechanism_params)
    return None


def structuralTest1():
    """
    Seq([a, KC(And([KC(d), KC(Seq([e, f]))]))])
    """
    structural_test_pattern = Pattern(
        SeqOperator([QItem("GOOG", "a"),
                     KleeneClosureOperator(
                         AndOperator([QItem("GOOG", "b"),
                                      KleeneClosureOperator(QItem("GOOG", "c"),
                                                            min_size=1, max_size=5),
                                      KleeneClosureOperator(SeqOperator([QItem("GOOG", "d"), QItem("GOOG", "e")]),
                                                            min_size=1, max_size=5)]
                                     ),
                         min_size=1, max_size=5,
                     )]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    runStructuralTest('structuralTest1', [structural_test_pattern])


def structuralTest2():
    """
    KC(a)
    """
    structural_test_pattern = Pattern(
        KleeneClosureOperator(QItem("GOOG", "a")),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    runStructuralTest('structuralTest2', [structural_test_pattern])


def structuralTest3():
    """
    Seq([a, KC(b)])
    """
    structural_test_pattern = Pattern(
        SeqOperator([
            QItem("GOOG", "a"), KleeneClosureOperator(QItem("GOOG", "b"))
        ]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    runStructuralTest('structuralTest3', [structural_test_pattern])


def structuralTest4():
    """
    And([KC(a), b])
    """
    structural_test_pattern = Pattern(
        AndOperator([
            KleeneClosureOperator(QItem("GOOG", "a")), QItem("GOOG", "b")
        ]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    runStructuralTest('structuralTest4', [structural_test_pattern])


def structuralTest5():
    """
    KC(Seq([KC(a), KC(b)]))
    """
    structural_test_pattern = Pattern(
        KleeneClosureOperator(
            SeqOperator([
                KleeneClosureOperator(QItem("GOOG", "a"), min_size=3, max_size=5),
                KleeneClosureOperator(QItem("GOOG", "b"))  # default values of min=1, max=5 defined at PatternStructure.py
            ]), min_size=1, max_size=3
        ),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    runStructuralTest('structuralTest5', [structural_test_pattern])


def structuralTest6():
    """
    Seq([a, Seq([ b, And([c, d])])])
    """
    structural_test_pattern = Pattern(
        SeqOperator([
            QItem("GOOG", "a"),
            SeqOperator([
                QItem("GOOG", "b"),
                AndOperator([
                    QItem("GOOG", "c"),
                    QItem("GOOG", "d")
                ]),
                QItem("GOOG", "e")
            ]),
        ]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    runStructuralTest('structuralTest6', [structural_test_pattern])


def structuralTest7():
    """
    And([a, b, c, Seq([
                        d, KC(And([
                                e, KC(f), g
                              ]),
                        And([ KC(h), KC(Seq([ i, j
                                        ])
                        ])
                   ]),
        k, l
    ])
    """
    structural_test_pattern = Pattern(
        AndOperator([
            QItem("GOOG", "a"), QItem("GOOG", "b"), QItem("GOOG", "c"),
            SeqOperator([
                QItem("GOOG", "d"),
                KleeneClosureOperator(
                    AndOperator([
                        QItem("GOOG", "e"), KleeneClosureOperator(QItem("GOOG", "f")), QItem("GOOG", "g")
                    ])
                ), AndOperator([
                    KleeneClosureOperator(QItem("GOOG", "h")),
                    KleeneClosureOperator(
                        SeqOperator([
                            QItem("GOOG", "i"), QItem("GOOG", "j")
                        ]),
                    ),
                ]),
            ]),
            QItem("GOOG", "k"), QItem("GOOG", "l")
        ]),
        AndFormula(
            SmallerThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), IdentifierTerm("b", lambda x: x["Peak Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Peak Price"]), IdentifierTerm("c", lambda x: x["Peak Price"]))
        ),
        timedelta(minutes=3)
    )
    runStructuralTest('structuralTest7', [structural_test_pattern])


"""
identical to the first test in the file, with 1 exception - the QItem object is wrapped with a KC operator
"""
def oneArgumentsearchTestKleeneClosure(createTestFile=False):
    pattern = Pattern(
        SeqOperator([KleeneClosureOperator(QItem("AAPL", "a"))]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]), AtomicTerm(135)),
        timedelta(minutes=5)
    )
    runTest("oneArgumentKC", [pattern], createTestFile)


# ------------------------------------------------------
#       KleeneClosure tests
# ------------------------------------------------------

oneArgumentsearchTestKleeneClosure()


# ------------------------------------------------------
#   tests for the tree structure, CEP only created not used!.
# ------------------------------------------------------

# structuralTest1()
# structuralTest2()
# structuralTest3()
# structuralTest4()
# structuralTest5()
# structuralTest6()
# structuralTest7()
