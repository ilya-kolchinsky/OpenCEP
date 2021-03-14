from test.EvalTestsDefaults import *
from test.BasicTests import *


def simple_1():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                            test_name='simple|_adaptive_trivial_optimizer_trivial_tree_update')


def simple_2():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                            test_name = 'simple|_adaptive_deviation_optimizer_trivial_tree_update')


def simple_3():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'simple|_adaptive_greedy_invariant_optimizer_trivial_tree_update')


def simple_4():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'simple|_adaptive_zstream_invariant_optimizer_trivial_tree_update')


def simple_5():
    simplePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                            test_name = 'simple|_adaptive_trivial_optimizer_simultaneous_tree_update')


def simple_6():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'simple|_adaptive_deviation_optimizer_simultaneous_tree_update')


def simple_7():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'simple|_adaptive_greedy_invariant_optimizer_simultaneous_tree_update')


def simple_8():
    simplePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'simple|_adaptive_zstream_invariant_optimizer_simultaneous_tree_update')


def googleAscendPatternSearchTest_1():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = 'googleAscend|_adaptive_trivial_optimizer_trivial_tree_update')


def googleAscendPatternSearchTest_2():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                  test_name = 'googleAscend|_adaptive_deviation_optimizer_trivial_tree_update')


def googleAscendPatternSearchTest_3():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleAscend|_adaptive_greedy_invariant_optimizer_trivial_tree_update')


def googleAscendPatternSearchTest_4():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleAscend|_adaptive_zstream_invariant_optimizer_trivial_tree_update')


def googleAscendPatternSearchTest_5():
    googleAscendPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = 'googleAscend|_adaptive_trivial_optimizer_simultaneous_tree_update')


def googleAscendPatternSearchTest_6():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'googleAscend|_adaptive_deviation_optimizer_simultaneous_tree_update')


def googleAscendPatternSearchTest_7():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleAscend|_adaptive_greedy_invariant_optimizer_simultaneous_tree_update')


def googleAscendPatternSearchTest_8():
    googleAscendPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleAscend|_adaptive_zstream_invariant_optimizer_simultaneous_tree_update')


def amazonInstablePatternSearchTest_1():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'amazonInstable|_adaptive_trivial_optimizer_trivial_tree_update')


def amazonInstablePatternSearchTest_2():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                    test_name = 'amazonInstable|_adaptive_deviation_optimizer_trivial_tree_update')


def amazonInstablePatternSearchTest_3():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'amazonInstable|_adaptive_greedy_invariant_optimizer_trivial_tree_update')


def amazonInstablePatternSearchTest_4():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'amazonInstable|_adaptive_zstream_invariant_optimizer_trivial_tree_update')


def amazonInstablePatternSearchTest_5():
    amazonInstablePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'amazonInstable|_adaptive_trivial_optimizer_simultaneous_tree_update')


def amazonInstablePatternSearchTest_6():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'amazonInstable|_adaptive_deviation_optimizer_simultaneous_tree_update')


def amazonInstablePatternSearchTest_7():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'amazonInstable|_adaptive_greedy_invariant_optimizer_simultaneous_tree_update')


def amazonInstablePatternSearchTest_8():
    amazonInstablePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'amazonInstable|_adaptive_zstream_invariant_optimizer_simultaneous_tree_update')


def msftDrivRacePatternSearchTest_1():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = 'msftDrivRace|_adaptive_trivial_optimizer_trivial_tree_update')


def msftDrivRacePatternSearchTest_2():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                  test_name = 'msftDrivRace|_adaptive_deviation_optimizer_trivial_tree_update')


def msftDrivRacePatternSearchTest_3():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'msftDrivRace|_adaptive_greedy_invariant_optimizer_trivial_tree_update')


def msftDrivRacePatternSearchTest_4():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'msftDrivRace|_adaptive_zstream_invariant_optimizer_trivial_tree_update')


def msftDrivRacePatternSearchTest_5():
    msftDrivRacePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                  test_name = 'msftDrivRace|_adaptive_trivial_optimizer_simultaneous_tree_update')


def msftDrivRacePatternSearchTest_6():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'msftDrivRace|_adaptive_deviation_optimizer_simultaneous_tree_update')


def msftDrivRacePatternSearchTest_7():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'msftDrivRace|_adaptive_greedy_invariant_optimizer_simultaneous_tree_update')


def msftDrivRacePatternSearchTest_8():
    msftDrivRacePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'msftDrivRace|_adaptive_zstream_invariant_optimizer_simultaneous_tree_update')


def googleIncreasePatternSearchTest_1():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'googleIncrease|_adaptive_trivial_optimizer_trivial_tree_update')


def googleIncreasePatternSearchTest_2():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                    test_name = 'googleIncrease|_adaptive_deviation_optimizer_trivial_tree_update')


def googleIncreasePatternSearchTest_3():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleIncrease|_adaptive_greedy_invariant_optimizer_trivial_tree_update')


def googleIncreasePatternSearchTest_4():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleIncrease|_adaptive_zstream_invariant_optimizer_trivial_tree_update')


def googleIncreasePatternSearchTest_5():
    googleIncreasePatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'googleIncrease|_adaptive_trivial_optimizer_simultaneous_tree_update')


def googleIncreasePatternSearchTest_6():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'googleIncrease|_adaptive_deviation_optimizer_simultaneous_tree_update')


def googleIncreasePatternSearchTest_7():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleIncrease|_adaptive_greedy_invariant_optimizer_simultaneous_tree_update')


def googleIncreasePatternSearchTest_8():
    googleIncreasePatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleIncrease|_adaptive_zstream_invariant_optimizer_simultaneous_tree_update')


def amazonSpecificPatternSearchTest_1():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'amazonSpecific|_adaptive_trivial_optimizer_trivial_tree_update')


def amazonSpecificPatternSearchTest_2():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                    test_name = 'amazonSpecific|_adaptive_deviation_optimizer_trivial_tree_update')


def amazonSpecificPatternSearchTest_3():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'amazonSpecific|_adaptive_greedy_invariant_optimizer_trivial_tree_update')


def amazonSpecificPatternSearchTest_4():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'amazonSpecific|_adaptive_zstream_invariant_optimizer_trivial_tree_update')


def amazonSpecificPatternSearchTest_5():
    amazonSpecificPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                    test_name = 'amazonSpecific|_adaptive_trivial_optimizer_simultaneous_tree_update')


def amazonSpecificPatternSearchTest_6():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'amazonSpecific|_adaptive_deviation_optimizer_simultaneous_tree_update')


def amazonSpecificPatternSearchTest_7():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'amazonSpecific|_adaptive_greedy_invariant_optimizer_simultaneous_tree_update')


def amazonSpecificPatternSearchTest_8():
    amazonSpecificPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'amazonSpecific|_adaptive_zstream_invariant_optimizer_simultaneous_tree_update')


def googleAmazonLowPatternSearchTest_1():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS,
                                     test_name = 'googleAmazonLow|_adaptive_trivial_optimizer_trivial_tree_update')


def googleAmazonLowPatternSearchTest_2():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_DEVIATION_AWARE_OPTIMIZER,
                                     test_name = 'googleAmazonLow|_adaptive_deviation_optimizer_trivial_tree_update')


def googleAmazonLowPatternSearchTest_3():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleAmazonLow|_adaptive_greedy_invariant_optimizer_trivial_tree_update')


def googleAmazonLowPatternSearchTest_4():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_TRIVIAL_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleAmazonLow|_adaptive_zstream_invariant_optimizer_trivial_tree_update')


def googleAmazonLowPatternSearchTest_5():
    googleAmazonLowPatternSearchTest(eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS,
                                     test_name = 'googleAmazonLow|_adaptive_trivial_optimizer_simultaneous_tree_update')


def googleAmazonLowPatternSearchTest_6():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_T_OPTIMIZER,
        test_name = 'googleAmazonLow|_adaptive_deviation_optimizer_simultaneous_tree_update')


def googleAmazonLowPatternSearchTest_7():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_GREEDY_INVARIANT_OPTIMIZER,
        test_name = 'googleAmazonLow|_adaptive_greedy_invariant_optimizer_simultaneous_tree_update')


def googleAmazonLowPatternSearchTest_8():
    googleAmazonLowPatternSearchTest(
        eval_mechanism_params=DEFAULT_TESTING_SIMULTANEOUS_EVALUATION_MECHANISM_SETTINGS_AND_ZSTREAM_INVARIANT_OPTIMIZER,
        test_name = 'googleAmazonLow|_adaptive_zstream_invariant_optimizer_simultaneous_tree_update')
