"""
This class contains an implementation of one of two plugins required for implementing parallelism.
"""

from tree.UnaryNode import UnaryNode
from tree.BinaryNode import BinaryNode
from tree.LeafNode import LeafNode
from stream.Stream import Stream
from parallerization.ParallelWorkLoadFramework import ParallelWorkLoadFramework
from parallerization.tree_implemintation.ParallelTreeEval import ParallelTreeEval
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismFactory, EvaluationMechanismParameters
from parallerization.tree_implemintation.ParallelUnaryNode import ParallelUnaryNode
from tree.PatternMatchStorage import TreeStorageParameters


class ParallelTreeWorkloadFramework(ParallelWorkLoadFramework):
    """
    Manager uses this class object to handle parallelism.
    This class implements the methods required to:
    data stream split, evaluation mechanism split, data stream distribution, and more ...
    """

    def __init__(self, pattern: Pattern, execution_units: int = 1, is_data_parallel: bool = False,
                 is_structure_parallel: bool = False, num_of_families: int = 0):
        super().__init__(execution_units, is_data_parallel, is_structure_parallel, num_of_families)

        # masters are the evaluation mechanisms which contain the results, meaning root trees.
        self.masters = []
        self.pattern = pattern

        # relevant only for multiple structures + single data scenario:
        self.tree_structures = []
        self.trees_with_leafs = []
        self.trees_with_leafs_indexes = []

        # relevant only for multiple structures multiple data scenario
        # families only:
        self.families = []                           # family = [[f1_tree_structures1,f1_tree_structures2],[f2_tree_structures1,f2_tree_structures2]]
        # leaves of base tree structure
        self.families_trees_with_leafs = []          # [[f1_trees_with_leafs],[f2_trees_with_leafs],[]]
        self.families_trees_with_leafs_indexes = []  # [[f1_index1, f1_index2],[f2_index1, f2_index2],[]]

    def wait_masters_to_finish(self):
        multiple_data = self.get_is_data_parallel()
        multiple_structures = self.get_is_structure_parallel()

        if not multiple_data and not multiple_structures:
            raise Exception("Not supported")
        elif multiple_data and not multiple_structures:
            self.join_only_masters()
        elif not multiple_data and multiple_structures:
            self.join_only_tree_structures()
        elif multiple_data and multiple_structures:
            self.join_all_structures()

    def join_all_structures(self):
        """
        join all threads
        """
        for family in self.families:
            for em in family:
                em.join()

    def join_only_masters(self):
        """
        join master threads
        """
        for em in self.masters:
            em.join()

    def join_only_tree_structures(self):
        """
        In not multiple_data and multiple_structures scenario, mechanisms are in self.tree_structures
        """
        for em in self.tree_structures:
            em.join()

    def duplicate_structure(self, eval_params: EvaluationMechanismParameters = None, num_of_copies=None):
        """
        Duplicates evaluation mechanism
        """
        if num_of_copies is None:
            num_of_copies = self.get_execution_units()

        for i in range(num_of_copies):
            # Here we create a ParallelTreeEval with a new TreeBasedEvaluationMechanism where we also added a UnaryNode
            # as a root
            tree_based_eval = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_params, self.pattern)
            root = tree_based_eval.get_tree().get_root()
            unary_root = ParallelUnaryNode(root._sliding_window, child=root)
            root.set_parent(unary_root)
            tree_based_eval.set_root(unary_root)
            storage_params = TreeStorageParameters(True)
            unary_root.create_storage_unit(storage_params)
            unary_eval = ParallelTreeEval(tree_based_eval, True, is_main_root=True, data_formatter=self.data_formatter)

            self.masters.append(unary_eval)
            self.trees_with_leafs.append(unary_eval)

        return self.masters, self.masters

    def get_data_stream_and_destinations(self):
        """
        This function is used to get next data stream event(s) and all destination(s)
        """
        multiple_data = self.get_is_data_parallel()
        multiple_structures = self.get_is_structure_parallel()

        if not multiple_data and not multiple_structures:
            raise Exception("Not supported")
        elif multiple_data and not multiple_structures:
            return self.get_multiple_data_single_structure_info()
        elif not multiple_data and multiple_structures:
            return self.get_single_data_multiple_structure_info()
        elif multiple_data and multiple_structures:
            return self.get_multiple_data_multiple_structures_info()

    def get_single_data_multiple_structure_info(self):
        """
        Returns copies of single event to be processed by different mechanisms.
        Also returns indexes of relevant mechanisms.
        """
        next_event = None
        count = self.event_stream.count()

        if count > 1:
            next_event = self.event_stream.get_item()

        if next_event is None:
            return None, None

        em_indexes = self.get_indexes()
        num_of_copies = len(em_indexes)
        input_streams = self.create_copies_of_event(next_event, num_of_copies)

        return input_streams, em_indexes

    def get_multiple_data_single_structure_info(self):
        """
        Returns copies of single event to be processed by different copies of evaluation mechanisms.
        Also returns indexes of relevant mechanisms.
        """
        next_event = None
        count = self.event_stream.count()

        if count > 1:
            next_event = self.event_stream.get_item()
        if next_event is None:
            return None, None
        em_indexes = self.get_indexes()

        num_of_copies = len(em_indexes)
        input_streams = self.create_copies_of_event(next_event, num_of_copies)

        return input_streams, em_indexes

    def get_multiple_data_multiple_structures_info(self):
        """
        Returns copies of single event to be processed by different families of mechanisms.
        Also returns indexes of relevant mechanisms in each family.
        """
        families_indexes = self.get_indexes()
        indexes_of_ems_in_each_family = self.get_indexes_of_ems_in_each_family()

        next_event = None
        count = self.event_stream.count()

        if count > 1:
            next_event = self.event_stream.get_item()

        if next_event is None:
            return None, None, None

        input_streams = self.get_family_events(next_event)

        return input_streams, families_indexes, indexes_of_ems_in_each_family

    def get_family_events(self, next_event):
        """
        Returns event copy for each mechanism in each family.
        """
        input_streams = []

        for i in range(self.num_of_families):
            streams = self.create_copies_of_event(next_event, self.get_execution_units())
            input_streams.append(streams)

        return input_streams

    def get_indexes_of_ems_in_each_family(self):
        """
        Returns indexes of mechanism in each family for mechanisms that have leaves and should process events.
        """
        indexes_list_of_list = []
        indexes_list = []

        for i in range(self.num_of_families):
            for j in self.families_trees_with_leafs_indexes[i]:
                indexes_list.append(j)

            indexes_list_of_list.append(indexes_list)
            indexes_list = []

        return indexes_list_of_list

    def get_indexes(self):
        """
        Returns indexes of evaluation mechanisms which should process events.
        """
        multiple_data = self.get_is_data_parallel()
        multiple_structures = self.get_is_structure_parallel()

        if not multiple_data and not multiple_structures:
            raise Exception("Not supported")
        elif multiple_data and not multiple_structures:
            return self.get_all_indexes()
        elif not multiple_data and multiple_structures:
            return self.trees_with_leafs_indexes
        elif multiple_data and multiple_structures:
            return self.get_indexes_for_families()

    def get_all_indexes(self):
        indexes = []

        for i in range(len(self.masters)):
            indexes.append(i)

        return indexes

    def get_indexes_for_families(self):
        families_indexes = []

        for i in range(self.num_of_families):
            families_indexes.append(i)

        return families_indexes

    def create_input_stream(self, event):
        input_stream = Stream()
        input_stream.add_item(event)
        input_stream.close()
        return input_stream

    def create_copies_of_event(self, event_to_copy, num_of_copies):
        input_streams = []

        for i in range(num_of_copies):
            input_streams.append(self.create_input_stream(event_to_copy))

        return input_streams

    # **** Split structure functions: *******

    def split_structure_to_families(self, eval_params: EvaluationMechanismParameters):
        """
        Creates families of evaluation mechanisms from single tree evaluation mechanism, according to parallelism parameters.
        """
        for i in range(self.num_of_families):
            structures, masters = self.split_structure(eval_params)

            self.families.append(self.tree_structures)
            self.families_trees_with_leafs.append(self.trees_with_leafs)
            self.families_trees_with_leafs_indexes.append(self.trees_with_leafs_indexes)

            self.tree_structures = []
            self.trees_with_leafs = []
            self.trees_with_leafs_indexes = []

        return self.families, self.masters

    def split_structure(self, eval_params: EvaluationMechanismParameters):
        """
        Splits single evaluation mechanism to many, according by parallelism parameters.
        Also return the masters.
        """
        self.split_structure_util(eval_params, self.get_execution_units())
        self.set_unary_children()

        return self.tree_structures, self.masters

    def split_structure_util(self, eval_params: EvaluationMechanismParameters, execution_units_left: int,
                             next_root=None, source_is_left: bool = False):
        if execution_units_left < 1:
            return

        tree_based_eval = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_params, self.pattern)
        storage_params = TreeStorageParameters(True)

        if next_root is None:   # then this is the first entry to this function
            root = tree_based_eval.get_tree().get_root()
            unary_root = ParallelUnaryNode(root._sliding_window, child=root)
            root.set_parent(unary_root)
            tree_based_eval.set_root(unary_root)
            unary_root.create_storage_unit(storage_params)
        else:
            root = next_root
            unary_root = ParallelUnaryNode(root._sliding_window, child=root)
            unary_root.set_parent(root._parent)
            root.set_parent(unary_root)

            if source_is_left:
                unary_root._parent.set_subtrees(unary_root, unary_root._parent._right_subtree)
            else:
                unary_root._parent.set_subtrees(unary_root._parent._left_subtree, unary_root)

            tree_based_eval.set_root(unary_root)
            unary_root.create_storage_unit(storage_params)

        if int((execution_units_left - 1)/2) < 1 or isinstance(next_root, LeafNode):
            unary_eval = ParallelTreeEval(tree_based_eval, has_leafs=True, is_main_root=(next_root is None),
                                          data_formatter=self.data_formatter)
            self.trees_with_leafs.append(unary_eval)
            self.trees_with_leafs_indexes.append(len(self.tree_structures))
        else:
            unary_eval = ParallelTreeEval(tree_based_eval, has_leafs=False, is_main_root=(next_root is None),
                                          data_formatter=self.data_formatter)

        self.tree_structures.append(unary_eval)
        if next_root is None:
            self.masters.append(unary_eval)

        current = unary_root.get_child()
        if isinstance(current, UnaryNode):
            self.split_structure_util(eval_params, execution_units_left - 1, current.get_child())
        elif isinstance(current, BinaryNode):
            self.split_structure_util(eval_params, int((execution_units_left - 1) / 2), current._left_subtree, True)
            self.split_structure_util(eval_params, int((execution_units_left - 1) / 2), current._right_subtree, False)

    def set_unary_children(self):
        for tree_structure in self.tree_structures:
            unary_root = tree_structure.get_evaluation_mechanism().get_tree().get_root()
            unary_children = unary_root.get_unary_children()

            for unary_child in unary_children:
                self.find_and_set_child(tree_structure, unary_child)

    def find_and_set_child(self, tree_to_set, unary_child):
        for tree_structure in self.tree_structures:
            unary_root = tree_structure.get_evaluation_mechanism().get_tree().get_root()

            if unary_child == unary_root:
                tree_to_set.add_child(tree_structure)

