from test.BasicTests import *
from test.EvaluationTests import *
from test.OptimizerTests import *
from test.TreeConstructionTests import *
from test.KC_tests import *
from test.NegationTests import *
from test.PolicyTests import *
from test.MultiPattern_tests import *
from test.StorageTests import *
from test.UnitTests.test_storage import run_storage_tests


runTest.over_all_time = 0

# basic functionality tests
# oneArgumentsearchTest()
# simplePatternSearchTest()
# googleAscendPatternSearchTest()
# amazonInstablePatternSearchTest()
# msftDrivRacePatternSearchTest()
# googleIncreasePatternSearchTest()
# amazonSpecificPatternSearchTest()
# googleAmazonLowPatternSearchTest()
# nonsensePatternSearchTest()
# hierarchyPatternSearchTest()
# duplicateEventTypeTest()

# tree plan generation algorithms
# arrivalRatesPatternSearchTest()
# nonFrequencyPatternSearchTest()
# frequencyPatternSearchTest()
# nonFrequencyPatternSearch2Test()
# frequencyPatternSearch2Test()
# nonFrequencyPatternSearch3Test()
# frequencyPatternSearch3Test()
# nonFrequencyPatternSearch4Test()
# frequencyPatternSearch4Test()
# nonFrequencyPatternSearch5Test()
# frequencyPatternSearch5Test()
# frequencyPatternSearch6Test()
# greedyPatternSearchTest()
# iiRandomPatternSearchTest()
# iiRandom2PatternSearchTest()
# iiGreedyPatternSearchTest()
# iiGreedy2PatternSearchTest()
# zStreamOrdPatternSearchTest()
# zStreamPatternSearchTest()
# dpBPatternSearchTest()
# dpLdPatternSearchTest()
# nonFrequencyTailoredPatternSearchTest()
# frequencyTailoredPatternSearchTest()

# tree structure tests - CEP object only created not used
# structuralTest1()
# structuralTest2()
# structuralTest3()
# structuralTest4()
# structuralTest5()
# structuralTest6()
# structuralTest7()

# Kleene closure tests
# oneArgumentsearchTestKleeneClosure()
# MinMax_0_TestKleeneClosure()
# MinMax_1_TestKleeneClosure()
# MinMax_2_TestKleeneClosure()
# KC_AND()

# Kleene Condition tests
# KC_AND_IndexCondition_01()
# KC_AND_IndexCondition_02()
# KC_AND_NegOffSet_01()
# KC_AllValues()
# KC_Specific_Value()
# KC_Mixed()
# KC_Condition_Failure_01()
# KC_Condition_Failure_02()
# KC_Condition_Failure_03()

# negation tests
# simpleNotTest()
# multipleNotInTheMiddleTest()
# oneNotAtTheBeginningTest()
# multipleNotAtTheBeginningTest()
# oneNotAtTheEndTest()
# multipleNotAtTheEndTest()
# multipleNotBeginAndEndTest()
# testWithMultipleNotAtBeginningMiddleEnd()

# consumption policies tests
# singleType1PolicyPatternSearchTest()
# singleType2PolicyPatternSearchTest()
# contiguousPolicyPatternSearchTest()
# contiguousPolicy2PatternSearchTest()
# freezePolicyPatternSearchTest()
# freezePolicy2PatternSearchTest()

# storage tests
# sortedStorageTest()
# run_storage_tests()

# multi-pattern tests
# first approach: sharing leaves
# leafIsRoot()
# distinctPatterns()
# threePatternsTest()
# samePatternDifferentTimeStamps()
# rootAndInner()

# second approach: sharing equivalent subtrees
# onePatternIncludesOther()
# samePatternSharingRoot()
# severalPatternShareSubtree()
# notInTheBeginningShare()
# multipleParentsForInternalNode()


# Optimizer tests
# create_InvariantAwareGreedyTreeBuilder()
# create_InvariantAwareZStreamTreeBuilder()
# greedy_invariant_optimizer_doesnt_change_the_tree()
# zstream_invariant_optimizer_doesnt_change_the_tree()
# greedy_invariant_optimizer_doesnt_change_the_tree_2()
# greedy_invariant_optimizer_doesnt_change_the_tree_3()
# zstream_invariant_optimizer_doesnt_change_the_tree2()
# zstream_invariant_optimizer_doesnt_change_the_tree3()

# Tree change tests
simple_1()
simple_2()
simple_3()
simple_4()
simple_5()
simple_6()
simple_7()
simple_8()
googleAscendPatternSearchTest_1()
googleAscendPatternSearchTest_2()
googleAscendPatternSearchTest_3()
googleAscendPatternSearchTest_4()
googleAscendPatternSearchTest_5()
googleAscendPatternSearchTest_6()
googleAscendPatternSearchTest_7()
googleAscendPatternSearchTest_8()
amazonInstablePatternSearchTest_1()
amazonInstablePatternSearchTest_2()
amazonInstablePatternSearchTest_3()
amazonInstablePatternSearchTest_4()
amazonInstablePatternSearchTest_5()
amazonInstablePatternSearchTest_6()
amazonInstablePatternSearchTest_7()
amazonInstablePatternSearchTest_8()
msftDrivRacePatternSearchTest_1()
msftDrivRacePatternSearchTest_2()
msftDrivRacePatternSearchTest_3()
msftDrivRacePatternSearchTest_4()
msftDrivRacePatternSearchTest_5()
msftDrivRacePatternSearchTest_6()
msftDrivRacePatternSearchTest_7()
msftDrivRacePatternSearchTest_8()
googleIncreasePatternSearchTest_1()
googleIncreasePatternSearchTest_2()
googleIncreasePatternSearchTest_3()
googleIncreasePatternSearchTest_4()
googleIncreasePatternSearchTest_5()
googleIncreasePatternSearchTest_6()
googleIncreasePatternSearchTest_7()
googleIncreasePatternSearchTest_8()
amazonSpecificPatternSearchTest_1()
amazonSpecificPatternSearchTest_2()
amazonSpecificPatternSearchTest_3()
amazonSpecificPatternSearchTest_4()
amazonSpecificPatternSearchTest_5()
amazonSpecificPatternSearchTest_6()
amazonSpecificPatternSearchTest_7()
amazonSpecificPatternSearchTest_8()
googleAmazonLowPatternSearchTest_1()
googleAmazonLowPatternSearchTest_2()
googleAmazonLowPatternSearchTest_3()
googleAmazonLowPatternSearchTest_4()
googleAmazonLowPatternSearchTest_5()
googleAmazonLowPatternSearchTest_6()
googleAmazonLowPatternSearchTest_7()
googleAmazonLowPatternSearchTest_8()


# benchmarks
if INCLUDE_BENCHMARKS:
    sortedStorageBenchMarkTest()


# Twitter tests
if INCLUDE_TWITTER:
    try:
        from TwitterTest import run_twitter_sanity_check
        run_twitter_sanity_check()
    except ImportError:  # tweepy might not be installed
        pass


print("Finished running all tests, overall time: %s" % runTest.over_all_time)
