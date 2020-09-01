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
        if self.is_data_splitted == False:
            output_stream.append(input_stream)
            return output_stream
        elif self.is_data_splitted == True:
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
                counter+=1

        else:
            raise Exception() #should never happen

        return output_stream

    def split_structure(self, evaluation_mechanism: TreeBasedEvaluationMechanism): # returns objects that implements ParallelExecutionFramework
        #split the tree into n <= execution_units parts in a nondescript way
        if type(evaluation_mechanism) is not TreeBasedEvaluationMechanism:
            raise Exception()
        splitted_tree = []

        if self.execution_units == 1 or self.is_tree_splitted == False:#need to change that
            #splitted_tree.append(evaluation_mechanism.get_tree())
            return [evaluation_mechanism]
        #TODO : need to return eval mecanism instead of nodes?
        splitted_tree = list(evaluation_mechanism.get_tree().get_nodes())
        if len(splitted_tree) <= self.execution_units:
            return splitted_tree
        else:
            while len(splitted_tree) > self.execution_units:
                if splitted_tree[0] is None or splitted_tree[1] is None:
                    raise Exception()
                list_nodes = [splitted_tree[0], splitted_tree[1]]
                splitted_tree.remove(splitted_tree[0])
                splitted_tree.remove(splitted_tree[0])

                splitted_tree.append(list_nodes)

            return splitted_tree

