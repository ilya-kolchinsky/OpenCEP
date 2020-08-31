from evaluation.TreeBasedEvaluationMechanism import Tree



class ParallelTree(Tree):


    def get_data(self):
        raise NotImplementedError()


    def eval(self):
        TreeBased.eval()



    def stop(self):
        raise NotImplementedError()