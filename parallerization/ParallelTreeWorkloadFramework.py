from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism, UnaryNode, BinaryNode, LeafNode
from misc.IOUtils import Stream
from parallerization.ParallelWorkLoadFramework import ParallelWorkLoadFramework
from parallerization.ParallelTreeEval import UnaryParallelTreeEval
from base.Pattern import Pattern
from evaluation.EvaluationMechanism import EvaluationMechanism
from evaluation.EvaluationMechanismFactory import EvaluationMechanismFactory, EvaluationMechanismTypes, EvaluationMechanismParameters
from parallerization.ParallelTreeEval import ParallelUnaryNode
from evaluation.PartialMatchStorage import TreeStorageParameters


class ParallelTreeWorkloadFramework(ParallelWorkLoadFramework):

    def __init__(self, execution_units: int = 1, is_data_splitted: bool = False, is_tree_splitted: bool = False,
                 pattern_size: int = 1,
                 pattern: Pattern = None):
        super().__init__(execution_units, is_data_splitted, is_tree_splitted)
        self.pattern_size = pattern_size
        self.pattern = pattern
        self.masters = [self.get_source_eval_mechanism()] #to be updated in split_data and split_structure

    def split_data(self, input_stream: Stream, eval_mechanism: TreeBasedEvaluationMechanism,
                   eval_mechanism_type: EvaluationMechanismTypes, eval_params: EvaluationMechanismParameters):
        return self.split_data_to_two(input_stream, eval_mechanism,eval_mechanism_type, eval_params)

    def split_data_to_two(self, input_stream: Stream, eval_mechanism: TreeBasedEvaluationMechanism,
                   eval_mechanism_type: EvaluationMechanismTypes, eval_params: EvaluationMechanismParameters):
        #returns the data stream splitted in 2: the first pattern_size lines in one part and the rest of the stream in another

        self.set_source_eval_mechanism(eval_mechanism)
        output_stream = []
        if not self._is_data_splitted:
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
            raise Exception() # should never happen

        self.createTreesCopiesSingleTreeMultipleData(2, eval_mechanism_type, eval_params)

        return output_stream

    def split_data_to_five(self, input_stream: Stream, eval_mechanism: TreeBasedEvaluationMechanism,
                   eval_mechanism_type: EvaluationMechanismTypes, eval_params: EvaluationMechanismParameters):
        # returns the data stream splitted in 2: the first pattern_size lines in one part and the rest of the stream in another

        self.set_source_eval_mechanism(eval_mechanism)
        output_stream = []
        if not self._is_data_splitted:
            output_stream.append(input_stream)
            return output_stream
        elif self._is_data_splitted:
            counter = 0
            for event in input_stream:
                event_stream = Stream()
                event_stream.add_item(event)
                if counter == 0:
                    output_stream.append(event_stream)
                elif counter < input_stream.count()/5:
                    output_stream[0].add_item(event)

                elif counter == self.pattern_size:
                    output_stream.append(event_stream)
                    output_stream[1].add_item(event)
                else:
                    output_stream[1].add_item(event)
                counter += 1

        else:
            raise Exception()  # should never happen

        self.createTreesCopiesSingleTreeMultipleData(2, eval_mechanism_type, eval_params)

        return output_stream

    def createTreesCopiesSingleTreeMultipleData(self, num_of_copies: int, eval_mechanism_type: EvaluationMechanismTypes,
                                                   eval_params: EvaluationMechanismParameters):
        self.masters = []
        for i in range(num_of_copies):
            tree_based_eval = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_mechanism_type,
                                                                                             eval_params, self.pattern)
            self.masters.append(UnaryParallelTreeEval(tree_based_eval, True))

    def get_masters(self):
        return self.masters

    def split_structure(self, evaluation_mechanism: EvaluationMechanism,
                        eval_mechanism_type: EvaluationMechanismTypes = None,
                        eval_params: EvaluationMechanismParameters = None):
        # returns objects that implements ParallelExecutionFramework
        """
        returns a list of UnaryParallelTree

        *adds UnaryNode to the tree where we want to separate the tree so that the UnaryNodes are the only connection
        between different part of the tree
        => add them such that no part of the tree have
         sons such that one needs to read the input and the other doesn't
        *create UnaryParallelTree objects and light the has_leaves flag accordingly

        after we have splitted the tree, we need to copy all those parts in prder to support the data

        update self.masters: add it the ParallelTreeExecutionFR that contains the root
        even if the tree is not splitted, add a copy to masters
        """
        eval_mechanism_list = []
        if self.get_is_data_splitted():
            eval_mechanism_one = self.masters[0]
            eval_mechanism_two = self.masters[1]
            #TODO: continue this case
        if not self.get_is_data_splitted():
            tree_based_eval_one = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_mechanism_type,
                                                                                                 eval_params,
                                                                                                 self.pattern)
            tree_based_eval_two = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(
                eval_mechanism_type,
                eval_params,
                self.pattern)

            tree = tree_based_eval_one.get_tree()
            root = tree.get_root()
            unary_node = ParallelUnaryNode(True, root._sliding_window, child=root)
            unary_node.set_subtree(root)
            root.set_parent(unary_node)
            tree_based_eval_one.set_root(unary_node)

            storageparams = TreeStorageParameters(True)
            tree_based_eval_one.get_tree().get_root().create_storage_unit(storageparams)
            if type(root) == LeafNode:
                unaryeval = UnaryParallelTreeEval(tree_based_eval_one, True)
                eval_mechanism_list.append(unaryeval)
            else:
                unaryeval = UnaryParallelTreeEval(tree_based_eval_one, False)
                eval_mechanism_list.append(unaryeval)

            current = tree.get_root()._child
            if isinstance(current, UnaryNode):
                unary = ParallelUnaryNode(False, root._sliding_window, child=root)
                unary.set_subtree(current._child)
                unary.set_parent(current)
                current._child.set_parent(unary)
                current.set_child(unary)
                tree_based_eval_two.set_root(unary)

                tree_based_eval_two.get_tree().get_root().create_storage_unit(storageparams)
                eval_mechanism_list.append(UnaryParallelTreeEval(tree_based_eval_two, True))

            if isinstance(current, BinaryNode):
                unary = ParallelUnaryNode(False, root._sliding_window, child=root)
                unary.set_subtree(current._left_subtree)
                unary.set_parent(current)
                current._left_subtree.set_parent(unary)
                current.set_subtrees(unary, current._right_subtree)
                tree_based_eval_two.set_root(unary)

                tree_based_eval_two.get_tree().get_root().create_storage_unit(storageparams)
                eval_mechanism_list.append(UnaryParallelTreeEval(tree_based_eval_two, True))

                tree_based_eval_three = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(
                    eval_mechanism_type,
                    eval_params,
                    self.pattern)

                unarytwo = ParallelUnaryNode(False, root._sliding_window, child=root)
                unarytwo.set_subtree(current._right_subtree)
                unarytwo.set_parent(current)
                current._right_subtree.set_parent(unary)
                current.set_subtrees(current._right_subtree, unarytwo)
                tree_based_eval_three.set_root(unarytwo)

                tree_based_eval_three.get_tree().get_root().create_storage_unit(storageparams)

                eval_mechanism_list.append(UnaryParallelTreeEval(tree_based_eval_three, True))
            self.masters = [unaryeval]

        return eval_mechanism_list