from test.BasicTests import *
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
