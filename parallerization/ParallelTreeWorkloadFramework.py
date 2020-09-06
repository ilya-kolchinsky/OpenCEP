from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from misc.IOUtils import Stream
from parallerization.ParallelWorkLoadFramework import ParallelWorkLoadFramework


class ParallelTreeWorkloadFramework(ParallelWorkLoadFramework):

    def __init__(self, execution_units: int = 1, is_data_splitted: bool = False, pattern_size: int = 1):
        super().__init__(execution_units, is_data_splitted)
        self.pattern_size = pattern_size

    def split_data(self, input_stream : Stream):
        #returns the data stream splitted in 2: the first pattern_size lignes in one part and the rest of the stream in another
        output_stream = []
        if self._is_data_splitted == False:
            output_stream.append(input_stream)
            return output_stream
        elif self._is_data_splitted:
            counter = 0
            for event in input_stream:
                event_stream = Stream()
                event_stream.add_item(event)
                if counter == 0:
                    output_stream.append(event_stream)
                elif counter < self.pattern_size:
                    output_stream[0].add_item(event)
                elif counter == self.pattern_size:
                    output_stream.append(event_stream)
                    output_stream[1].add_item(event)
                else:
                    output_stream[1].add_item(event)
                counter += 1

        else:
            raise Exception() #should never happen

        return output_stream

    def split_structure(self, evaluation_mechanism: TreeBasedEvaluationMechanism):
        # returns objects that implements ParallelExecutionFramework
        """
        returns a list of UnaryParallelTree

        *adds UnaryNode to the tree where we want to separate the tree so that the UnaryNodes are the only connection
        between different part of the tree
        => add them such that no part of the tree have
         sons such that one needs to read the input and the other doesn't
        *create UnaryParallelTree objects and light the has_leaves flag accordingly

        """

        raise NotImplementedError() #not implemented yet
        """
        if type(evaluation_mechanism) is not TreeBasedEvaluationMechanism:
            raise Exception()
        splitted_tree = []

        if self._execution_units == 1 or self._is_tree_splitted == False:#need to change that
            #splitted_tree.append(evaluation_mechanism.get_tree())
            return [evaluation_mechanism]
        splitted_tree = list(evaluation_mechanism.get_tree().get_nodes())
        if len(splitted_tree) <= self._execution_units:
            return splitted_tree
        else:
            while len(splitted_tree) > self._execution_units:
                if splitted_tree[0] is None or splitted_tree[1] is None:
                    raise Exception()
                list_nodes = [splitted_tree[0], splitted_tree[1]]
                splitted_tree.remove(splitted_tree[0])
                splitted_tree.remove(splitted_tree[0])

                splitted_tree.append(list_nodes)

            return splitted_tree
        """
