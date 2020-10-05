from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from base.Pattern import Pattern
from tree.UnaryNode import UnaryNode
from tree.BinaryNode import BinaryNode
from tree.LeafNode import LeafNode
from stream.Stream import Stream
from parallerization.ParallelWorkLoadFramework import ParallelWorkLoadFramework
from parallerization.tree_implemintation.ParallelTreeEval import ParallelTreeEval
from base.Pattern import Pattern
from evaluation.EvaluationMechanism import EvaluationMechanism
from evaluation.EvaluationMechanismFactory import EvaluationMechanismFactory, EvaluationMechanismTypes, EvaluationMechanismParameters
from parallerization.tree_implemintation.ParallelUnaryNode import ParallelUnaryNode
from tree.PatternMatchStorage import TreeStorageParameters


class ParallelTreeWorkloadFramework(ParallelWorkLoadFramework):

    def __init__(self, pattern: Pattern, execution_units: int = 1, is_data_parallelized: bool = False,
                 is_structure_parallelized: bool = False, num_of_families: int = 0):
        super().__init__(execution_units, is_data_parallelized, is_structure_parallelized, num_of_families)

        self.masters = []
        self.pattern = pattern

        self.tree_structures = []
        self.trees_with_leafs = []
        self.trees_with_leafs_indexes = []

        self.families = [[]]                           # [[tree_structures1],[tree_structures2],[]]
        self.families_trees_with_leafs = [[]]          # [[trees_with_leafs1],[trees_with_leafs2],[]]
        self.families_trees_with_leafs_indexes = [[]]  # [[index1, index2],[index1, index2],[]]

    def duplicate_structure(self, evaluation_mechanism: EvaluationMechanism,
                            eval_params: EvaluationMechanismParameters = None):
        for i in range(self.get_execution_units()):
            tree_based_eval = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_params, self.pattern)
            storageparams = TreeStorageParameters(True)

            root = tree_based_eval.get_tree().get_root()
            unary_root = ParallelUnaryNode(True, root._sliding_window, child=root)
            root.set_parent(unary_root)
            tree_based_eval.set_root(unary_root)
            unary_root.create_storage_unit(storageparams)

            unaryeval = ParallelTreeEval(tree_based_eval, True, is_main_root=True, data_formatter=self.data_formatter)
            self.masters.append(unaryeval)
        return self.masters, self.masters#TODO: check if in manager we need both of them

    def split_structure_to_families(self, evaluation_mechanism: EvaluationMechanism,
                                    eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()

    def get_next_event_and_destinations_em(self):
        next_event = None
        count = self.event_stream.count()
        if count > 1:
            next_event = self.event_stream.get_item()

        if next_event is None:
            return None, None
        else:
            input_stream = Stream()
            input_stream.add_item(next_event)
            input_stream.close()
            return input_stream, self.trees_with_leafs_indexes

    def get_next_event_families_indexes_and_destinations_ems(self):
        next_event = self.event_stream.get_item()
        indexes_of_families = [1]
        first_family = self.families[1]
        indexes_of_ems_in_each_family = []
        indexes_of_first_family = []
        for i in range(len(first_family)):
            indexes_of_first_family.append(i)

        indexes_of_ems_in_each_family.append(indexes_of_first_family)

        return next_event, indexes_of_families, indexes_of_ems_in_each_family

    def split_structure(self, eval_params: EvaluationMechanismParameters = None):
        self.split_structure_utils(eval_params, self.get_execution_units())
        self.set_unary_children_for_all_structures()
        return self.tree_structures, self.masters
        #TODO: go over tree_structures to update all unary children fields and check other fields

    def split_structure_utils(self, eval_params: EvaluationMechanismParameters, execution_units_left: int,
                              next_root=None, source_is_left: bool = False):
        if execution_units_left < 1:
            return
        tree_based_eval = EvaluationMechanismFactory.build_single_pattern_eval_mechanism(eval_params, self.pattern)
        storageparams = TreeStorageParameters(True)

        if next_root is None:   #then this is the first entry to this function
            root = tree_based_eval.get_tree().get_root()
            unary_root = ParallelUnaryNode(True, root._sliding_window, child=root)
            root.set_parent(unary_root)
            tree_based_eval.set_root(unary_root)
            unary_root.create_storage_unit(storageparams)
        else:
            root = next_root
            unary_root = ParallelUnaryNode(False, root._sliding_window, child=root)
            unary_root.set_parent(root._parent)
            root.set_parent(unary_root)
            if source_is_left:
                unary_root._parent.set_subtrees(unary_root, unary_root._parent._right_subtree)
            else:
                unary_root._parent.set_subtrees(unary_root._parent._left_subtree, unary_root)

            tree_based_eval.set_root(unary_root)
            unary_root.create_storage_unit(storageparams)

        if int((execution_units_left - 1)/2) < 1:
            unaryeval = ParallelTreeEval(tree_based_eval, has_leafs=True, is_main_root=(next_root is None), data_formatter=self.data_formatter)
            self.trees_with_leafs.append(unaryeval)
            self.trees_with_leafs_indexes.append(len(self.tree_structures))
        else:
            unaryeval = ParallelTreeEval(tree_based_eval, has_leafs=False, is_main_root=(next_root is None), data_formatter=self.data_formatter)

        self.tree_structures.append(unaryeval)
        if next_root is None:
            self.masters.append(unaryeval)

        current = unary_root.get_child()
        if isinstance(current, UnaryNode):
            self.split_structure_utils(eval_params, execution_units_left-1, current.get_child())
        elif isinstance(current, BinaryNode):
            self.split_structure_utils(eval_params, int((execution_units_left - 1)/2), current._left_subtree, True)
            self.split_structure_utils(eval_params, int((execution_units_left - 1) / 2), current._right_subtree, False)

    def set_unary_children_for_all_structures(self):
        raise NotImplementedError()
