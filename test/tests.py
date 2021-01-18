from test.BasicTests import *
from test.TreeConstructionTests import *
from test.KC_tests import *
from test.NegationTests import *
from test.PolicyTests import *
from test.MultiPattern_tests import *
from test.StorageTests import *
from test.internal_tests import *
from test.UnitTests.test_storage import run_storage_tests
from stream.Stream import *


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
duplicateEventTypeTest()            #failure !!!!

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
oneArgumentsearchTestKleeneClosure()  #failure
MinMax_0_TestKleeneClosure()            #failure
MinMax_1_TestKleeneClosure()            #failure
MinMax_2_TestKleeneClosure()            #failure
KC_AND()                                #failure

# Kleene Condition tests
KC_AND_IndexCondition_01()              #failure
KC_AND_IndexCondition_02()              #failure
KC_AND_NegOffSet_01()                    #failure
KC_AllValues()                           #failure
KC_Specific_Value()                      #failure
KC_Mixed()                                #failure
KC_Condition_Failure_01()
KC_Condition_Failure_02()
KC_Condition_Failure_03()
############################all above failure ###################################
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
#####################################################end of failure################
# storage tests
sortedStorageTest()
run_storage_tests()

# multi-pattern tests
# first approach: sharing leaves
leafIsRoot()                             #failure
distinctPatterns()
threePatternsTest()
samePatternDifferentTimeStamps()
rootAndInner()                          #failure

# second approach: sharing equivalent subtrees
onePatternIncludesOther()                      #failure
samePatternSharingRoot()
severalPatternShareSubtree()                     #failure
notInTheBeginningShare()                        #failure
multipleParentsForInternalNode()                   #failure

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