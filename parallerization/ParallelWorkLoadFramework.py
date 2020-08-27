from abc import ABC
from misc.IOUtils import Stream
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, Node



class ParallelWorkLoadFramework(ABC):

    def __init__(self, execution_units : int = 1, is_data_splitted : bool = False):
        self.execution_units = execution_units
        self.is_data_splitted = is_data_splitted

    def split_data(self, input_stream: Stream):#the output needs to be a list of streams of size <= execution_units
        raise NotImplementedError()

    def split_structure(self, evaluation_mechanism):#the output needs to be a list of nodes/a list with the whole tree when execution_units = 1
        raise NotImplementedError()

    def get_execution_units(self):
        return self.execution_units

    def get_is_data_splitted(self):
        return self.is_data_splitted

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

    def split_structure(self, evaluation_mechanism: TreeBasedEvaluationMechanism):
        #split the tree into n <= execution_units parts in a nondescript way
        if type(evaluation_mechanism) != TreeBasedEvaluationMechanism:
            raise Exception()
        splitted_tree = []

        if self.execution_units == 1:#need to change that
            splitted_tree.append(evaluation_mechanism.get_tree())
            return splitted_tree

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







