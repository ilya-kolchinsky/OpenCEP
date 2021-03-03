from test.EvalTestsDefaults import *
from test.BasicTests import *


def simple_1():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                            test_name='simple|_1')


def simple_2():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                            test_name = 'simple|_2')


def simple_3():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'simple|_3')


def simple_4():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'simple|_4')


def simple_5():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                            test_name = 'simple|_5')


def simple_6():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'simple|_6')


def simple_7():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'simple|_7')


def simple_8():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER,
        test_name = 'simple|_8')


def googleAscendPatternSearchTest_1():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = 'googleAscend|_1')


def googleAscendPatternSearchTest_2():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                  test_name = 'googleAscend|_2')


def googleAscendPatternSearchTest_3():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleAscend|_3')


def googleAscendPatternSearchTest_4():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleAscend|_4')


def googleAscendPatternSearchTest_5():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = 'googleAscend|_5')


def googleAscendPatternSearchTest_6():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'googleAscend|_6')


def googleAscendPatternSearchTest_7():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleAscend|_7')


def googleAscendPatternSearchTest_8():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleAscend|_8')


def amazonInstablePatternSearchTest_1():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'amazonInstable|_1')


def amazonInstablePatternSearchTest_2():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                    test_name = 'amazonInstable|_2')


def amazonInstablePatternSearchTest_3():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'amazonInstable|_3')


def amazonInstablePatternSearchTest_4():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'amazonInstable|_4')


def amazonInstablePatternSearchTest_5():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'amazonInstable|_5')


def amazonInstablePatternSearchTest_6():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'amazonInstable|_6')


def amazonInstablePatternSearchTest_7():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'amazonInstable|_7')


def amazonInstablePatternSearchTest_8():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER,
        test_name = 'amazonInstable|_8')


def msftDrivRacePatternSearchTest_1():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = 'msftDrivRace|_1')


def msftDrivRacePatternSearchTest_2():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                  test_name = 'msftDrivRace|_2')


def msftDrivRacePatternSearchTest_3():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'msftDrivRace|_3')


def msftDrivRacePatternSearchTest_4():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'msftDrivRace|_4')


def msftDrivRacePatternSearchTest_5():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = 'msftDrivRace|_5')


def msftDrivRacePatternSearchTest_6():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'msftDrivRace|_6')


def msftDrivRacePatternSearchTest_7():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'msftDrivRace|_7')


def msftDrivRacePatternSearchTest_8():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER,
        test_name = 'msftDrivRace|_8')


def googleIncreasePatternSearchTest_1():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'googleIncrease|_1')


def googleIncreasePatternSearchTest_2():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                    test_name = 'googleIncrease|_2')


def googleIncreasePatternSearchTest_3():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleIncrease|_3')


def googleIncreasePatternSearchTest_4():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleIncrease|_4')


def googleIncreasePatternSearchTest_5():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'googleIncrease|_5')


def googleIncreasePatternSearchTest_6():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'googleIncrease|_6')


def googleIncreasePatternSearchTest_7():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleIncrease|_7')


def googleIncreasePatternSearchTest_8():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleIncrease|_8')


def amazonSpecificPatternSearchTest_1():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'amazonSpecific|_1')


def amazonSpecificPatternSearchTest_2():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                    test_name = 'amazonSpecific|_2')


def amazonSpecificPatternSearchTest_3():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'amazonSpecific|_3')


def amazonSpecificPatternSearchTest_4():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'amazonSpecific|_4')


def amazonSpecificPatternSearchTest_5():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'amazonSpecific|_5')


def amazonSpecificPatternSearchTest_6():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'amazonSpecific|_6')


def amazonSpecificPatternSearchTest_7():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'amazonSpecific|_7')


def amazonSpecificPatternSearchTest_8():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER,
        test_name = 'amazonSpecific|_8')


def googleAmazonLowPatternSearchTest_1():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                     test_name = 'googleAmazonLow|_1')


def googleAmazonLowPatternSearchTest_2():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                     test_name = 'googleAmazonLow|_2')


def googleAmazonLowPatternSearchTest_3():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleAmazonLow|_3')


def googleAmazonLowPatternSearchTest_4():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleAmazonLow|_4')


def googleAmazonLowPatternSearchTest_5():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                     test_name = 'googleAmazonLow|_5')


def googleAmazonLowPatternSearchTest_6():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'googleAmazonLow|_6')


def googleAmazonLowPatternSearchTest_7():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleAmazonLow|_7')


def googleAmazonLowPatternSearchTest_8():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTRREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleAmazonLow|_8')
