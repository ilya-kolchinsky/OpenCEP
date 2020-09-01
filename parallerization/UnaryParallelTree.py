from base import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from parallerization.ParallelTree import ParallelTree


class ParallelUnaryNode(UnaryNode):
    parent
    child
    + mutex


class UnaryParallelTree(ParallelExecutionFramework):

    def __init__(self, tree: TreeBasedEvaluationMechanism):
        self.tree = tree

        self.partial_match_mutex = new
        Mutex() mutex
        self.thread_ended = False
        self.data_from_child
        self.has_leafs =
        self.is_master =

    def stop(self):
            raise NotImplementedError()

    def get_data(self):
            raise NotImplementedError()

    def eval(self, event_stream, pattern_matches, is_async=True, file_path=file_path, time_limit=time_limit):
        thread = create_thread(modified_eval)
        thread.run(self.tree, event_stream, pattern_matches, is_async, file_path, time_limit)


    def modified_eval(self, ):
