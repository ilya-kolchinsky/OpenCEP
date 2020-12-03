from abc import ABC, abstractmethod


class DecisionMakerAlgorithm(ABC):

    @abstractmethod
    def run(self, curr_plan):
        pass


##################################################
# algorithm1 - inherit from DecisionMakerAlgorithm
##################################################
class DecisionMakerAlgorithm1(DecisionMakerAlgorithm):

    def run(self, curr_plan):
        pass


##################################################
# algorithm2 - inherit from DecisionMakerAlgorithm
##################################################
class DecisionMakerAlgorithm2(DecisionMakerAlgorithm):

    def run(self, curr_plan):
        pass


##################################################
# algorithm3 - inherit from DecisionMakerAlgorithm
##################################################
class DecisionMakerAlgorithm3(DecisionMakerAlgorithm):

    def run(self, curr_plan):
        pass
