from base import Pattern
from evaluation.PartialMatchStorage import TreeStorageParameters
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from parallerization.ParallelExecutionFramework import ParallelExecutionFramework
from parallerization.ParallelTree import ParallelTree


class UnaryParallelTree(ParallelExecutionFramework):

    def __init__(self, tree: TreeBasedEvaluationMechanism):
        self.tree = tree
        self.tree.root.parent

        self.partial_match_mutex = new Mutex()
        self.thread_ended = False
        self.data_from_child
        self.has_leafs =
        self.is_master =

        def eval(self, event_stream, pattern_matches, is_async=True, file_path=file_path, time_limit=time_limit):
            thread = create_thread(run_our_eval)
            thread.run(tree, event_stream, pattern_matches, is_async, file_path, time_limit)

        def stop(self):
            raise NotImplementedError()

        def get_data(self):
            raise NotImplementedError()

        def run_our_eval(self, tree, event_stream, pattern_matches, is_async=True, file_path=file_path, time_limit=time_limit):

            while(not self.thread_ended):

                self.unary_eval(event_stream, pattern_matches, is_async, file_path, time_limit)


            if (self.master):
