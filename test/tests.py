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
oneArgumentsearchTest()
simplePatternSearchTest()
googleAscendPatternSearchTest()
amazonInstablePatternSearchTest()
msftDrivRacePatternSearchTest()
googleIncreasePatternSearchTest()
amazonSpecificPatternSearchTest()
googleAmazonLowPatternSearchTest()
nonsensePatternSearchTest()
hierarchyPatternSearchTest()
duplicateEventTypeTest()

# tree plan generation algorithms
arrivalRatesPatternSearchTest()
nonFrequencyPatternSearchTest()
frequencyPatternSearchTest()
nonFrequencyPatternSearch2Test()
frequencyPatternSearch2Test()
nonFrequencyPatternSearch3Test()
frequencyPatternSearch3Test()
nonFrequencyPatternSearch4Test()
frequencyPatternSearch4Test()
nonFrequencyPatternSearch5Test()
frequencyPatternSearch5Test()
frequencyPatternSearch6Test()
greedyPatternSearchTest()
iiRandomPatternSearchTest()
iiRandom2PatternSearchTest()
iiGreedyPatternSearchTest()
iiGreedy2PatternSearchTest()
zStreamOrdPatternSearchTest()
zStreamPatternSearchTest()
dpBPatternSearchTest()
dpLdPatternSearchTest()
nonFrequencyTailoredPatternSearchTest()
frequencyTailoredPatternSearchTest()

# tree structure tests - CEP object only created not used
structuralTest1()
structuralTest2()
structuralTest3()
structuralTest4()
structuralTest5()
structuralTest6()
structuralTest7()

# Kleene closure tests
oneArgumentsearchTestKleeneClosure()
MinMax_0_TestKleeneClosure()
MinMax_1_TestKleeneClosure()
MinMax_2_TestKleeneClosure()
KC_AND()

# Kleene Condition tests
KC_AND_IndexCondition_01()
KC_AND_IndexCondition_02()
KC_AND_NegOffSet_01()
KC_AllValues()
KC_Specific_Value()
KC_Mixed()
KC_Condition_Failure_01()
KC_Condition_Failure_02()
KC_Condition_Failure_03()

# negation tests
simpleNotTest()
multipleNotInTheMiddleTest()
oneNotAtTheBeginningTest()
multipleNotAtTheBeginningTest()
oneNotAtTheEndTest()
multipleNotAtTheEndTest()
multipleNotBeginAndEndTest()
testWithMultipleNotAtBeginningMiddleEnd()

# consumption policies tests
singleType1PolicyPatternSearchTest()
singleType2PolicyPatternSearchTest()
contiguousPolicyPatternSearchTest()
contiguousPolicy2PatternSearchTest()
freezePolicyPatternSearchTest()
freezePolicy2PatternSearchTest()

# storage tests
sortedStorageTest()
run_storage_tests()

# multi-pattern tests
# first approach: sharing leaves
leafIsRoot()
distinctPatterns()
threePatternsTest()
samePatternDifferentTimeStamps()
rootAndInner()

# second approach: sharing equivalent subtrees
onePatternIncludesOther()
samePatternSharingRoot()
severalPatternShareSubtree()
notInTheBeginningShare()
multipleParentsForInternalNode()


# Optimizer tests
greedyInvariantOptimizerTreeChangeFailTest_1()
greedyInvariantOptimizerTreeChangeFailTest_2()
greedyInvariantOptimizerTreeChangeTest_1()
zstreamInvariantOptimizerTreeChangeFailTest_1()
zstreamInvariantOptimizerTreeChangeTest_1()
zstreamInvariantOptimizerTreeChangeTest_2()

# Adaptivity tests
# trivial evaluation with trivial optimizer
simple_1()
googleAscendPatternSearchTest_1()
amazonInstablePatternSearchTest_1()
msftDrivRacePatternSearchTest_1()
googleIncreasePatternSearchTest_1()
amazonSpecificPatternSearchTest_1()
googleAmazonLowPatternSearchTest_1()

# trivial evaluation with deviation aware optimizer
simple_2()
googleAscendPatternSearchTest_2()
amazonInstablePatternSearchTest_2()
msftDrivRacePatternSearchTest_2()
googleIncreasePatternSearchTest_2()
amazonSpecificPatternSearchTest_2()
googleAmazonLowPatternSearchTest_2()

# trivial evaluation with greedy invariant optimizer
simple_3()
googleAscendPatternSearchTest_3()
amazonInstablePatternSearchTest_3()
msftDrivRacePatternSearchTest_3()
googleIncreasePatternSearchTest_3()
amazonSpecificPatternSearchTest_3()
googleAmazonLowPatternSearchTest_3()

# trivial evaluation with zstream invariant optimizer
simple_4()
googleAscendPatternSearchTest_4()
amazonInstablePatternSearchTest_4()
msftDrivRacePatternSearchTest_4()
googleIncreasePatternSearchTest_4()
amazonSpecificPatternSearchTest_4()
googleAmazonLowPatternSearchTest_4()

# simultaneous evaluation with trivial optimizer
simple_5()
googleAscendPatternSearchTest_5()
amazonInstablePatternSearchTest_5()
msftDrivRacePatternSearchTest_5()
googleIncreasePatternSearchTest_5()
amazonSpecificPatternSearchTest_5()
googleAmazonLowPatternSearchTest_5()

# simultaneous evaluation with deviation aware optimizer
simple_6()
googleAscendPatternSearchTest_6()
amazonInstablePatternSearchTest_6()
msftDrivRacePatternSearchTest_6()
googleIncreasePatternSearchTest_6()
amazonSpecificPatternSearchTest_6()
googleAmazonLowPatternSearchTest_6()

# simultaneous evaluation with greedy invariant optimizer
simple_7()
googleAscendPatternSearchTest_7()
amazonInstablePatternSearchTest_7()
msftDrivRacePatternSearchTest_7()
googleIncreasePatternSearchTest_7()
amazonSpecificPatternSearchTest_7()
googleAmazonLowPatternSearchTest_7()

# simultaneous evaluation with zstream invariant optimizer
simple_8()
googleAscendPatternSearchTest_8()
amazonInstablePatternSearchTest_8()
msftDrivRacePatternSearchTest_8()
googleIncreasePatternSearchTest_8()
amazonSpecificPatternSearchTest_8()
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
