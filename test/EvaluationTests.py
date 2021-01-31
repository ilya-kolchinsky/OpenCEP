from datetime import timedelta

from base.Pattern import Pattern
from base.PatternStructure import PrimitiveEventStructure, SeqOperator, AndOperator
from condition.BaseRelationCondition import SmallerThanEqCondition, GreaterThanEqCondition, EqCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import BinaryCondition, Variable, SimpleCondition
from test.EvalTestsDefaults import *
from test.testUtils import runTest
from test.BasicTests import *


def simple_1():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def simple_2():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def simple_3():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def simple_4():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def simple_5():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def simple_6():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def simple_7():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def simple_8():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def googleAscendPatternSearchTest_1():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleAscendPatternSearchTest_2():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleAscendPatternSearchTest_3():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleAscendPatternSearchTest_4():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def googleAscendPatternSearchTest_5():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def googleAscendPatternSearchTest_6():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def googleAscendPatternSearchTest_7():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleAscendPatternSearchTest_8():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def amazonInstablePatternSearchTest_1():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def amazonInstablePatternSearchTest_2():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def amazonInstablePatternSearchTest_3():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def amazonInstablePatternSearchTest_4():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def amazonInstablePatternSearchTest_5():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def amazonInstablePatternSearchTest_6():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def amazonInstablePatternSearchTest_7():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def amazonInstablePatternSearchTest_8():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def msftDrivRacePatternSearchTest_1():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def msftDrivRacePatternSearchTest_2():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def msftDrivRacePatternSearchTest_3():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def msftDrivRacePatternSearchTest_4():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def msftDrivRacePatternSearchTest_5():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def msftDrivRacePatternSearchTest_6():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def msftDrivRacePatternSearchTest_7():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def msftDrivRacePatternSearchTest_8():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def googleIncreasePatternSearchTest_1():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleIncreasePatternSearchTest_2():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleIncreasePatternSearchTest_3():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleIncreasePatternSearchTest_4():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def googleIncreasePatternSearchTest_5():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def googleIncreasePatternSearchTest_6():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def googleIncreasePatternSearchTest_7():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleIncreasePatternSearchTest_8():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def amazonSpecificPatternSearchTest_1():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def amazonSpecificPatternSearchTest_2():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def amazonSpecificPatternSearchTest_3():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def amazonSpecificPatternSearchTest_4():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def amazonSpecificPatternSearchTest_5():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def amazonSpecificPatternSearchTest_6():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def amazonSpecificPatternSearchTest_7():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def amazonSpecificPatternSearchTest_8():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)


def googleAmazonLowPatternSearchTest_1():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleAmazonLowPatternSearchTest_2():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS)


def googleAmazonLowPatternSearchTest_3():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleAmazonLowPatternSearchTest_4():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER)


def googleAmazonLowPatternSearchTest_5():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS)


def googleAmazonLowPatternSearchTest_6():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER)


def googleAmazonLowPatternSearchTest_7():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER)


def googleAmazonLowPatternSearchTest_8():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER)
