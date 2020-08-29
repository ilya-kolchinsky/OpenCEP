from base import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework



class ParallelTreeExecutionFramework(ParallelExecutionFramework, TreeBasedEvaluationMechanism):

    def __init__(self, pattern: Pattern, tree_structure: tuple, storage_params: TreeStorageParameters):
        TreeBasedEvaluationMechanism.__init__(pattern, tree_structure, storage_params)
        self.__tree = ParallelTree(tree_structure, pattern, storage_params)



    def get_data(self):
        raise NotImplementedError()

    def eval(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()