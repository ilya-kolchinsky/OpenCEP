from tree.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from tree.UnaryNode import UnaryNode
from tree.BinaryNode import BinaryNode
from tree.LeafNode import LeafNode
from stream.Stream import Stream
from parallerization.ParallelWorkLoadFramework import ParallelWorkLoadFramework
from tree_implemintation.old_ParallelTreeEval import ParallelTreeEval
from base.Pattern import Pattern
from evaluation.EvaluationMechanism import EvaluationMechanism
from evaluation.EvaluationMechanismFactory import EvaluationMechanismFactory, EvaluationMechanismTypes, EvaluationMechanismParameters
from tree_implemintation.old_ParallelTreeEval import ParallelUnaryNode
from tree.PatternMatchStorage import TreeStorageParameters


class ParallelTreeWorkloadFramework(ParallelWorkLoadFramework):

    def __init__(self, execution_units: int = 1, is_data_parallelised: bool = False, is_tree_splitted: bool = False , famalies = 0):
        super().__init__(execution_units, is_data_parallelised, is_tree_splitted, famalies)

        self.tree_structures = None
        self.trees_with_leafs = None

        self.famalies = None                    # [[tree_structures1],[tree_structures2],[]]
        self.famalies_trees_with_leafs = None  # [[trees_with_leafs1],[trees_with_leafs2],[]]

    def duplicate_structure(self, evaluation_mechanism: EvaluationMechanism, eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()

    def split_structure_to_families(self, evaluation_mechanism: EvaluationMechanism, eval_params: EvaluationMechanismParameters = None):
        raise NotImplementedError()

    def split_structure(self, evaluation_mechanism: EvaluationMechanism, eval_params: EvaluationMechanismParameters = None):

        self.tree_structures =  [ParallelTreeEval]..
        self.trees_with_leafs = ....
        raise NotImplementedError()

    def get_next_event_and_destinations_em(self):
        # next_event = self.event_stream.get_item()
        # ems = self.get_destinations()
        #
        # return ems

    def get_next_event_family_and_destinations_em(self):
        # next_event = self.event_stream.get_item()
        # ems = []
        #
        # for family in self.famalies:
        #     ems += self.get_destinations_for_families()



    def get_destinations(self):
        # return self.trees_with_leafs

    def get_destinations_for_families(self):
        # ems = []
        #
        # for trees_with_leafs in self.famalies_trees_with_leafs:
        #     ems += trees_with_leafs
        #
        # return ems