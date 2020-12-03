
class Optimizer:

    #curr_plan =

    def __init__(self, decision_making_algorithm: DecisionMakingAlgorithm):
        self.decision_making_algorithm = decision_making_algorithm


    def optimize(self, statistics):
        self.decision_making_algorithm.run(statistics, self.curr_plan)
        #sent to eval

    def send_to_evaluation_mechanism(self):
        pass


