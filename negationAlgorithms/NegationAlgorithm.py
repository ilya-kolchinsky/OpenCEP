from base.Pattern import Pattern
from misc.StatisticsTypes import StatisticsTypes
from base.PatternStructure import PrimitiveEventStructure, NegationOperator
from plan.TreePlan import TreePlanBinaryNode, TreePlanLeafNode, TreePlanNode
from misc import DefaultConfig
from negationAlgorithms.NegationAlgorithmTypes import NegationAlgorithmTypes


class NegationAlgorithm:
    """
    The main class of the negation algorithms, contains the information and parameters needed in order to build the
    tree plan according to the chosen negation algorithm.
    """
    def __init__(self, negation_algorithm_type: NegationAlgorithmTypes = DefaultConfig.DEFAULT_NEGATION_ALGORITHM):
        self.negation_algorithm_type = negation_algorithm_type
        self.negative_original_indices = []
        self.positive_original_indices = []

    def add_negative_part(self, pattern: Pattern, positive_tree_plan: TreePlanBinaryNode):
        """
        This method adds the negative part to the tree plan (that includes only the positive part),
        according to the negation algorithm the user chose
        """
        raise NotImplementedError()

    def calculate_original_indices(self, pattern: Pattern):
        """
        This method calculates the original index (i.e. the index in the pattern) of each event, and saves it in
        the "positive_original_indices" and "positive_original_indices" lists according to the event's type.
        """
        self.positive_original_indices = []
        self.negative_original_indices = []
        for index, arg in enumerate(pattern.full_structure.args):
            if type(arg) == NegationOperator:
                self.negative_original_indices.append(index)
            elif type(arg) == PrimitiveEventStructure:
                self.positive_original_indices.append(index)

    @staticmethod
    def extract_statistics(event_index, pattern: Pattern):
        """
        Given the index of an event at the pattern structure (the full one), the function finds its arrival statistic
        """
        if pattern.statistics_type is StatisticsTypes.ARRIVAL_RATES:
            return pattern.full_statistics[event_index]
        if pattern.statistics_type is StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
            return pattern.full_statistics[1][event_index]

    def create_sorted_statistics_list(self, pattern: Pattern, negative_events_chunk=None):
        """
        This function returns list of negative events sorted by their arrival stats.
        """
        range_to_sort = range(0, len(
            pattern.negative_structure.args)) if negative_events_chunk is None else negative_events_chunk
        # list of tuples, each tuple saves a pair of negative event and its arrival statistic
        indices_statistics_list = []
        for i in range_to_sort:
            # find the arrival rate of the specific negative event
            i_negative_event_statistic = self.extract_statistics(self.negative_original_indices[i], pattern)
            indices_statistics_list.append((i, i_negative_event_statistic))
        # sorting the negative events according to the arrival rates statistic
        indices_statistics_list.sort(key=lambda x: x[1], reverse=True)
        return indices_statistics_list

    def update_indices(self, node: TreePlanNode, pattern: Pattern):
        """
        If the node is leaf node (i.e. represents specific event), this method will convert its index, from fake one
        (its place in the negative or positive structure) to the original one (its place in the pattern).
        """
        if type(node) == TreePlanLeafNode:
            if node.event_index >= len(pattern.positive_structure.args):
                node.event_index = self.negative_original_indices[node.event_index - len(pattern.positive_structure.args)]
            else:
                node.event_index = self.positive_original_indices[
                    node.event_index]

    def adjust_tree_plan_indices(self, tree_topology: TreePlanBinaryNode, pattern: Pattern):
        """
        This method traverse the full tree plan (including both positive and negative parts) and convert the events
        fake indices to their original ones.
        """
        if type(tree_topology) == TreePlanLeafNode:
            return
        self.adjust_tree_plan_indices(tree_topology.left_child, pattern)
        self.adjust_tree_plan_indices(tree_topology.right_child, pattern)
        self.update_indices(tree_topology.left_child, pattern)
        self.update_indices(tree_topology.right_child, pattern)
