from test.testUtils import *
from datetime import timedelta
from base.Formula import GreaterThanEqFormula, SmallerThanEqFormula, GreaterThanFormula, SmallerThanFormula, IdentifierTerm, AtomicTerm, AndFormula
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure, NegationOperator
from base.Pattern import Pattern

currentPath = pathlib.Path(os.path.dirname(__file__))
absolutePath = str(currentPath.parent)
sys.path.append(absolutePath)

def createExpectedOutput(testName, patterns, eval_mechanism_params=DEFAULT_TESTING_EVALUATION_MECHANISM_SETTINGS,
                         events=None, eventStream=nasdaqEventStream):
    if events is None:
        events = eventStream.duplicate()
    else:
        events = events.duplicate()

    listShort = []
    listHalfShort = []
    listCustom = []
    listCustom2 = ["FirstMultiPattern"]

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

    for i in range(len(patterns)):
        cep = CEP(patterns[i], eval_mechanism_params)
        expected_directory = os.path.join(absolutePath, 'test', 'TestsExpected', 'MultiPatternMatches')
        output_file_name = "%sMatches.txt" % (testName + str(i))
        matches_stream = FileOutputStream(expected_directory, output_file_name)
        cep.run(events, matches_stream, DEFAULT_TESTING_DATA_FORMATTER)

def twoPatternsOneArgument(createTestFile = False):
    pattern1 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a")]),
        GreaterThanFormula(IdentifierTerm("a", lambda x: x["Peak Price"]), AtomicTerm(135)),
        timedelta(minutes=5)
    )
    pattern2 = Pattern(
        SeqOperator([PrimitiveEventStructure("AAPL", "a"), NegationOperator(PrimitiveEventStructure("AMZN", "b")), PrimitiveEventStructure("GOOG", "c")]),
        AndFormula(
            GreaterThanFormula(IdentifierTerm("a", lambda x: x["Opening Price"]),
                               IdentifierTerm("b", lambda x: x["Opening Price"])),
            SmallerThanFormula(IdentifierTerm("b", lambda x: x["Opening Price"]),
                               IdentifierTerm("c", lambda x: x["Opening Price"]))),
        timedelta(minutes=5)
    )

    runMultiTest("FirstMultiPattern", [pattern1, pattern2], createTestFile)




