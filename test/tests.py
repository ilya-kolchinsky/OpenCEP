from test.BasicTests import *
from test.TreeConstructionTests import *
from test.KC_tests import *
from test.NegationTests import *
from test.PolicyTests import *
from test.MultiPattern_tests import *
from test.StorageTests import *
from test.ParallelTests import *
from test.HyperCubeAlgorithmTests import *
from test.UnitTests.test_storage import run_storage_tests


runTest.over_all_time = 0

# # # basic functionality tests
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

# # # tree plan generation algorithms
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

# # # tree structure tests - CEP object only created not used
structuralTest1()
structuralTest2()
structuralTest3()
structuralTest4()
structuralTest5()
structuralTest6()
structuralTest7()

# # # Kleene closure tests
MinMax_0_TestKleeneClosure()
MinMax_1_TestKleeneClosure()
MinMax_2_TestKleeneClosure()

# # # Kleene Condition tests
KC_AND_IndexCondition_01()
KC_AND_IndexCondition_02()
KC_AND_NegOffSet_01()
KC_AllValues()
KC_Specific_Value()
KC_Mixed()
KC_Condition_Failure_01()
KC_Condition_Failure_02()
KC_Condition_Failure_03()

# # # negation tests
simpleNotTest()
multipleNotInTheMiddleTest()
oneNotAtTheBeginningTest()
multipleNotAtTheBeginningTest()
oneNotAtTheEndTest()
multipleNotAtTheEndTest()
multipleNotBeginAndEndTest()
testWithMultipleNotAtBeginningMiddleEnd()
#
# # # # # consumption policies tests
singleType1PolicyPatternSearchTest()
singleType2PolicyPatternSearchTest()
contiguousPolicyPatternSearchTest()
contiguousPolicy2PatternSearchTest()
freezePolicyPatternSearchTest()
freezePolicy2PatternSearchTest()
#
# # # # storage tests
sortedStorageTest()
run_storage_tests()
#
# # # # multi-pattern tests
# # # # first approach: sharing leaves
leafIsRoot()
distinctPatterns()
threePatternsTest()
samePatternDifferentTimeStamps()
rootAndInner()
# # # # #
# # # # second approach: sharing equivalent subtrees
onePatternIncludesOther()
samePatternSharingRoot()
severalPatternShareSubtree()
notInTheBeginningShare()
multipleParentsForInternalNode()
#
#
# #
# # benchmarks
if INCLUDE_BENCHMARKS:
    sortedStorageBenchMarkTest()
# #
# #
# # # Twitter tests
if INCLUDE_TWITTER:
    try:
        from TwitterTest import run_twitter_sanity_check
        run_twitter_sanity_check()
    except ImportError:  # tweepy might not be installed
        pass
#
# # Data parallel tests
### Hirzel Algorithm
oneArgumentsearchTestHirzelAlgorithm()
amazonSpecificPatternSearchTestHirzelAlgorithm()
fbNegPatternSearchTestHirzelAlgorithm()
fbEqualToApple1PatternSearchTestHirzelAlgorithm()
fbEqualToApple2PatternSearchTestHirzelAlgorithm()
appleOpenToCloseTestHirzelAlgorithm()
applePeakToOpenTestHirzelAlgorithm()
KCgoogleTestHirzelAlgorithm()
KCequalsPatternSearchTestHirzelAlgorithm()
multyPatternHirzelAlgorithm()


# RIP Algorithm
oneArgumentsearchTestRIPAlgorithm()
simplePatternSearchTestRIPAlgorithm()
googleAscendPatternSearchTestRIPAlgorithm()
amazonInstablePatternSearchTestRIPAlgorithm()
msftDrivRacePatternSearchTestRIPAlgorithm()
googleIncreasePatternSearchTestRIPAlgorithm()
amazonSpecificPatternSearchTestRIPAlgorithm()
googleAmazonLowPatternSearchTestRIPAlgorithm()
nonsensePatternSearchTestRIPAlgorithm()
hierarchyPatternSearchTestRIPAlgorithm()
duplicateEventTypeTestRIPAlgorithm()
structuralTest1RIPAlgorithm()
structuralTest2RIPAlgorithm()
structuralTest3RIPAlgorithm()
structuralTest4RIPAlgorithm()
structuralTest5RIPAlgorithm()
structuralTest6RIPAlgorithm()
structuralTest7RIPAlgorithm()
MinMax_0_TestKleeneClosureRIPAlgorithm()
MinMax_1_TestKleeneClosureRIPAlgorithm()
MinMax_2_TestKleeneClosureRIPAlgorithm()
KC_AND_IndexCondition_01_RIPAlgorithm()
KC_AND_IndexCondition_02_RIPAlgorithm()
KC_AND_NegOffSet_01_RIPAlgorithm()
KC_AllValuesRIPAlgorithm()
KC_Specific_ValueRIPAlgorithm()
KC_MixedRIPAlgorithm()
leafIsRootRIPAlgorithm()
distinctPatternsRIPAlgorithm()
threePatternsTestRIPAlgorithm()
rootAndInnerRIPAlgorithm()
samePatternDifferentTimeStampsRIPAlgorithm()
onePatternIncludesOtherRIPAlgorithm()
samePatternSharingRootRIPAlgorithm()
multipleParentsForInternalNodeRIPAlgorithm()
simpleNotTestRIPAlgorithm()
multipleNotInTheMiddleTestRIPAlgorithm()
singleType1PolicyPatternSearchTestRIPAlgorithm()
singleType2PolicyPatternSearchTestRIPAlgorithm()
contiguousPolicyPatternSearchTestRIPAlgorithm()
contiguousPolicy2PatternSearchTestRIPAlgorithm()
freezePolicyPatternSearchTestRIPAlgorithm()
freezePolicy2PatternSearchTestRIPAlgorithm()
sortedStorageTestRIPAlgorithm()


# # ######Basic functionality tests for HyperCube Algorithm
oneArgumentsearchTestHyperCubeAlgorithm()
simplePatternSearchTestHyperCubeAlgorithm()
googleAmazonLowPatternSearchTestHyperCubeAlgorithm()
nonsensePatternSearchTestHyperCubeAlgorithm()
duplicateEventTypeTestHyperCubeAlgorithm()
amazonSpecificPatternSearchTestHyperCubeAlgorithm()
googleAscendPatternSearchTestHyperCubeAlgorithm()
amazonInstablePatternSearchTestHyperCubeAlgorithm()
msftDrivRacePatternSearchTestHyperCubeAlgorithm()
googleIncreasePatternSearchTestHyperCubeAlgorithm()
hierarchyPatternSearchTestHyperCubeAlgorithm()


# # tree plan generation algorithms for HyperCubeAlgorithm
arrivalRatesPatternSearchTestHyperCubeAlgorithm()
frequencyPatternSearchTestHyperCubeAlgorithm()
nonFrequencyPatternSearchTestHyperCubeAlgorithm()
nonFrequencyPatternSearch3TestHyperCubeAlgorithm()
frequencyPatternSearch3TestHyperCubeAlgorithm()
nonFrequencyPatternSearch2TestHyperCubeAlgorithm()
frequencyPatternSearch2TestHyperCubeAlgorithm()
nonFrequencyPatternSearch4TestHyperCubeAlgorithm()
frequencyPatternSearch4TestHyperCubeAlgorithm()
greedyPatternSearchTestHyperCubeAlgorithm()
iiRandomPatternSearchTestHyperCubeAlgorithm()
iiRandom2PatternSearchTestHyperCubeAlgorithm()
iiGreedyPatternSearchTestHyperCubeAlgorithm()
iiGreedy2PatternSearchTestHyperCubeAlgorithm()
zStreamOrdPatternSearchTestHyperCubeAlgorithm()
zStreamPatternSearchTestHyperCubeAlgorithm()
dpBPatternSearchTestHyperCubeAlgorithm()
dpLdPatternSearchTestHyperCubeAlgorithm()
nonFrequencyTailoredPatternSearchTestHyperCubeAlgorithm()
frequencyTailoredPatternSearchTestHyperCubeAlgorithm()

# # ### Kleene closure tests
MinMax_0_TestKleeneClosureHyperCubeAlgorithm()
MinMax_2_TestKleeneClosureHyperCubeAlgorithm()

# # tree plan generation algorithms for HyperCubeAlgorithm
arrivalRatesPatternSearchTestHyperCubeAlgorithm()
frequencyPatternSearchTestHyperCubeAlgorithm()
nonFrequencyPatternSearchTestHyperCubeAlgorithm()
nonFrequencyPatternSearch3TestHyperCubeAlgorithm()
frequencyPatternSearch3TestHyperCubeAlgorithm()
nonFrequencyPatternSearch2TestHyperCubeAlgorithm()
frequencyPatternSearch2TestHyperCubeAlgorithm()
nonFrequencyPatternSearch4TestHyperCubeAlgorithm()
frequencyPatternSearch4TestHyperCubeAlgorithm()
greedyPatternSearchTestHyperCubeAlgorithm()
iiRandomPatternSearchTestHyperCubeAlgorithm()
iiRandom2PatternSearchTestHyperCubeAlgorithm()
iiGreedyPatternSearchTestHyperCubeAlgorithm()
iiGreedy2PatternSearchTestHyperCubeAlgorithm()
zStreamOrdPatternSearchTestHyperCubeAlgorithm()
zStreamPatternSearchTestHyperCubeAlgorithm()
dpBPatternSearchTestHyperCubeAlgorithm()
dpLdPatternSearchTestHyperCubeAlgorithm()
nonFrequencyTailoredPatternSearchTestHyperCubeAlgorithm()
frequencyTailoredPatternSearchTestHyperCubeAlgorithm()

# # # consumption policies tests
singleType1PolicyPatternSearchTestHyperCubeAlgorithm()
singleType2PolicyPatternSearchTestHyperCubeAlgorithm()
contiguousPolicyPatternSearchTestHyperCubeAlgorithm()
contiguousPolicy2PatternSearchTestHyperCubeAlgorithm()
freezePolicy2PatternSearchTestHyperCubeAlgorithm()

# # storage tests
sortedStorageTestHyperCubeAlgorithm()

# # # multi-pattern tests
distinctPatternsHyperCubeAlgorithm()
samePatternDifferentTimeStampsHyperCubeAlgorithm()
rootAndInnerHyperCubeAlgorithm()
onePatternIncludesOtherHyperCubeAlgorithm()

print("Finished running all tests, overall time: %s" % runTest.over_all_time)
