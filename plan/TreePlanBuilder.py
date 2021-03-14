from abc import ABC
from copy import deepcopy
from typing import List, Dict

from adaptive.statistics.StatisticsTypes import StatisticsTypes
from base.Pattern import Pattern
from base.PatternStructure import PatternStructure, UnaryStructure, KleeneClosureOperator, CompositeStructure, \
    NegationOperator
from plan.TreeCostModel import IntermediateResultsTreeCostModel
from plan.negation.NegationAlgorithmFactory import NegationAlgorithmFactory
from plan.negation.NegationAlgorithmTypes import NegationAlgorithmTypes
from base.PatternStructure import AndOperator, SeqOperator, PrimitiveEventStructure
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode, TreePlanUnaryNode, \
    TreePlanNestedNode, TreePlanLeafNode, TreePlanKCNode


class TreePlanBuilder(ABC):
    """
    The base class for the builders of tree-based plans.
    """
    def __init__(self, cost_model_type: TreeCostModels, negation_algorithm_type: NegationAlgorithmTypes):
        self.__cost_model = TreeCostModelFactory.create_cost_model(cost_model_type)
        self.__negation_algorithm = NegationAlgorithmFactory.create_negation_algorithm(negation_algorithm_type)

    def build_tree_plan(self, pattern: Pattern, statistics: Dict):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        statistics_copy = deepcopy(statistics)
        pattern_positive_args = pattern.get_top_level_structure_args(positive_only=True)
        pattern_positive_statistics = TreePlanBuilder.extract_positive_statistics(pattern, statistics_copy)
        if any(not isinstance(arg, PrimitiveEventStructure) for arg in pattern_positive_args):
            # the pattern contains nested parts and should be treated accordingly
            nested_topology, _ = self.__create_nested_topology(pattern, pattern_positive_statistics)
            positive_root = TreePlanBuilder.__adjust_nested_indices(pattern, nested_topology)
        else:
            # the pattern is entirely flat
            positive_root = self._create_tree_topology(pattern, pattern_positive_statistics, self.__init_tree_leaves(pattern))
        if isinstance(pattern.positive_structure, UnaryStructure):
            # an edge case where the topmost operator is a unary operator
            positive_root = self._instantiate_unary_node(pattern, positive_root)
        root = self.__negation_algorithm.handle_pattern_negation(pattern, statistics_copy, positive_root)
        pattern_condition = deepcopy(pattern.condition)  # copied since apply_condition modifies its input parameter
        root.apply_condition(pattern_condition)
        return TreePlan(root)

    @staticmethod
    def extract_positive_statistics(pattern: Pattern, statistics: Dict):
        """
        Returns a statistics object representing only the statistics related to the positive part of the pattern.
        """
        if not isinstance(pattern.full_structure, CompositeStructure) or statistics is None or len(statistics) == 0:
            return statistics
        positive_statistics = {}

        # TODO: this solution only works for negative events at the top level of the pattern - no nesting is supported
        negative_indices = []
        actual_index = 0
        for i, arg in enumerate(pattern.full_structure.args):
            if type(arg) == NegationOperator:
                negative_indices.append(actual_index)
            actual_index += len(arg.get_all_event_names())

        if StatisticsTypes.ARRIVAL_RATES in statistics:
            positive_statistics[StatisticsTypes.ARRIVAL_RATES] = \
                TreePlanBuilder.__remove_entries_at_indices(statistics[StatisticsTypes.ARRIVAL_RATES], negative_indices)

        if StatisticsTypes.SELECTIVITY_MATRIX not in statistics:
            return positive_statistics
        positive_selectivity_rows = TreePlanBuilder.__remove_entries_at_indices(
            statistics[StatisticsTypes.SELECTIVITY_MATRIX], negative_indices)
        positive_selectivities = [TreePlanBuilder.__remove_entries_at_indices(row, negative_indices)
                                  for row in positive_selectivity_rows]
        positive_statistics[StatisticsTypes.SELECTIVITY_MATRIX] = positive_selectivities

        return positive_statistics

    @staticmethod
    def __remove_entries_at_indices(target_list: List[object], indices: List[int]):
        """
        Returns the copy of the given list without the entries in the given indices.
        """
        return [item for i, item in enumerate(target_list) if i not in indices]

    @staticmethod
    def __adjust_nested_indices(pattern: Pattern, root, offset=0):
        """
        After building a tree plan, this function will correct the indexes of sub trees to continue the
        indices of the main tree, instead of restarting in each sub tree(which was important while building the plan)
        """
        for index, arg in enumerate(pattern.get_top_level_structure_args(positive_only=True)):
            node = TreePlanBuilder.__get_node_by_index(root, index)
            if isinstance(node, TreePlanLeafNode):
                node.event_index += offset
            elif isinstance(node, TreePlanNestedNode):
                nested_pattern = TreePlanBuilder.__create_dummy_subpattern(pattern, arg)
                TreePlanBuilder.__adjust_nested_indices(nested_pattern, node.sub_tree_plan,
                                                        node.nested_event_index + offset)
                offset += nested_pattern.count_primitive_events(positive_only=True) - 1
        return root

    @staticmethod
    def __get_node_by_index(root, index):
        """
        Given a tree and an event index, this will recursively search for the node structure with the index.
        """
        if isinstance(root, TreePlanBinaryNode):
            node = TreePlanBuilder.__get_node_by_index(root.left_child, index)
            if node is None:
                node = TreePlanBuilder.__get_node_by_index(root.right_child, index)
            return node
        elif isinstance(root, TreePlanUnaryNode):
            return TreePlanBuilder.__get_node_by_index(root.child, index)
        elif isinstance(root, TreePlanNestedNode):
            return root if root.nested_event_index == index else None
        elif isinstance(root, TreePlanLeafNode):
            return root if root.event_index == index else None
        else:
            raise Exception("Illegal Root")

    def _create_tree_topology(self, pattern: Pattern, statistics: Dict, leaves: List[TreePlanNode]):
        """
        An abstract method for creating the actual tree topology.
        This method works on the flat (not nested) case.
        """
        raise NotImplementedError()

    @staticmethod
    def __init_tree_leaves(pattern: Pattern,
                           nested_topologies: List[TreePlanNode] = None, nested_args: List[PatternStructure] = None,
                           nested_cost: List[float] = None):
        """
        Initializes the leaves of the tree plan. If the nested parameters are given, creates nested nodes instead of
        regular leaves where necessary.
        """
        leaves = []
        pattern_positive_args = pattern.get_top_level_structure_args(positive_only=True)
        for i, arg in enumerate(pattern_positive_args):
            if nested_topologies is None or nested_topologies[i] is None:
                # the current argument can either be a PrimitiveEventStructure or an UnaryOperator surrounding it
                event_structure = arg if isinstance(arg, PrimitiveEventStructure) else arg.child
                new_leaf = TreePlanLeafNode(i, event_structure.type, event_structure.name)
            else:
                nested_topology = nested_topologies[i].sub_tree_plan \
                    if isinstance(nested_topologies[i], TreePlanNestedNode) else nested_topologies[i]
                new_leaf = TreePlanNestedNode(i, nested_topology,
                                              nested_args[i], nested_cost[i])
            if isinstance(arg, UnaryStructure):
                new_leaf = TreePlanBuilder._instantiate_unary_node(
                    TreePlanBuilder.__create_dummy_subpattern(pattern, arg), new_leaf)
            leaves.append(new_leaf)
        return leaves

    def __create_nested_topology(self, pattern: Pattern, statistics: Dict):
        """
        A recursive method for creating a tree topology for the nested case.
        """
        pattern, modified_statistics, nested_topologies, nested_args, nested_cost = \
            self.__extract_nested_pattern(pattern, statistics)
        tree_topology = self._create_tree_topology(pattern, modified_statistics,
                                                   self.__init_tree_leaves(pattern, nested_topologies,
                                                                           nested_args, nested_cost))
        return tree_topology, modified_statistics

    def _get_plan_cost(self, pattern: Pattern, plan: TreePlanNode, statistics: Dict):
        """
        Returns the cost of a given plan for the given plan according to a predefined cost model.
        """
        return self.__cost_model.get_plan_cost(pattern, plan, statistics)

    @staticmethod
    def _instantiate_unary_node(pattern: Pattern, subtree: TreePlanNode):
        """
        A helper method for instantiating unary tree plan nodes depending on the operator.
        """
        if isinstance(subtree, TreePlanLeafNode):
            unary_node_index = subtree.original_event_index
        elif isinstance(subtree, TreePlanNestedNode):
            unary_node_index = subtree.nested_event_index
        else:
            raise Exception("Invalid node type under an unary node")
        if isinstance(pattern.positive_structure, KleeneClosureOperator):
            return TreePlanKCNode(subtree, unary_node_index,
                                  pattern.positive_structure.min_size, pattern.positive_structure.max_size)
        raise Exception("Unsupported unary operator")

    @staticmethod
    def _instantiate_binary_node(pattern: Pattern, left_subtree: TreePlanNode, right_subtree: TreePlanNode):
        """
        A helper method for instantiating binary tree plan nodes depending on the operator.
        """
        pattern_structure = pattern.positive_structure
        if isinstance(pattern_structure, AndOperator):
            operator_type = OperatorTypes.AND
        elif isinstance(pattern_structure, SeqOperator):
            operator_type = OperatorTypes.SEQ
        else:
            raise Exception("Unsupported binary operator")
        return TreePlanBinaryNode(operator_type, left_subtree, right_subtree)

    def __extract_nested_pattern(self, pattern: Pattern, statistics: Dict):
        """
        This function is done recursively, to support nested pattern's operators (i.e. And(Seq,Seq)).
        When encounters KleeneClosure or CompositeStructure, it computes this operator's tree plan (recursively...)
        and uses the returned tree plan's root as a new "simple" (primitive) event (with its statistics updated
        according to its subtree nodes), such that all nested Kleene/Composite operators can be treated as "simple"
        ones.
        For every "flat" pattern, it invokes an algorithm (to be implemented by subclasses) that builds an evaluation
        order of the operands, and converts it into a left-deep tree topology.
        """
        if isinstance(pattern.positive_structure, PrimitiveEventStructure):
            return pattern, statistics, None, None, None
        # a nested structure
        nested_topologies = []
        nested_args = []
        nested_cost = []
        nested_arrival_rates = []
        modified_statistics = {}
        if StatisticsTypes.SELECTIVITY_MATRIX in statistics:
            # If we have a selectivity matrix, than we need to create new one, that has the size of the "flat" root
            # operator and all its entries are computed according to the nested operators in each composite/kleene
            # structure.
            modified_statistics[StatisticsTypes.SELECTIVITY_MATRIX] = \
                TreePlanBuilder.__create_selectivity_matrix_for_nested_operators(pattern, statistics)
        for arg in pattern.get_top_level_structure_args(positive_only=True):
            # This loop creates (recursively) all the nested subtrees
            if not isinstance(arg, PrimitiveEventStructure):
                # If we are here than this structure is composite or unary.
                # And first create new pattern that fits the nested operator's stats and structure.
                pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args = \
                    self.__handle_nested_operator(arg, pattern, statistics, nested_topologies,
                                                  nested_arrival_rates, nested_cost, nested_args)
            else:
                # If we are here, than this structure is primitive
                pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args = \
                    TreePlanBuilder.__handle_primitive_event(pattern, statistics, nested_topologies,
                                                             nested_arrival_rates, nested_cost, nested_args)
        modified_statistics[StatisticsTypes.ARRIVAL_RATES] = nested_arrival_rates
        return pattern, modified_statistics, nested_topologies, nested_args, nested_cost

    @staticmethod
    def __create_selectivity_matrix_for_nested_operators(pattern: Pattern, statistics: Dict):
        """
        This function creates a selectivity matrix that fits the root operator (kind of flattening the selectivity
        of the nested operators, if exists).
        """
        selectivity_matrix = statistics[StatisticsTypes.SELECTIVITY_MATRIX]
        if pattern.count_primitive_events(positive_only=True) != len(selectivity_matrix):
            raise Exception("size mismatch")
        nested_selectivity_matrix = []
        primitive_sons_list = []
        # event_names are all the events in this pattern (including those under nested operators)
        event_names = [name for name in pattern.positive_structure.get_all_event_names()]
        for arg in pattern.get_top_level_structure_args(positive_only=True):
            # This is a list with size of the number of args, where each entry in the list is the events' name of the
            # primitive events under this arg.
            primitive_sons_list.append([name for name in arg.get_all_event_names()])
        for i, row_entry in enumerate(primitive_sons_list):
            nested_selectivity_matrix.append([])
            for col_entry in primitive_sons_list:
                # Building the new matrix, which is (#args x #args), where each entry is calculated based on the events
                # under the specific args respectively
                nested_selectivity = TreePlanBuilder.__calculate_nested_selectivity(event_names, selectivity_matrix,
                                                                                    row_entry, col_entry)
                nested_selectivity_matrix[i].append(nested_selectivity)
        return nested_selectivity_matrix

    @staticmethod
    def __calculate_nested_selectivity(all_events_names, selectivity_matrix, arg1_names, arg2_names):
        """
        Calculates the selectivity of a new entry in the top (new generated) matrix, based on list of all the names in
        pattern, and the 2 args' nested primitive events, that we want to calculate their selectivity.
        We multiply all the selectivities combinations of pairs that contain one from the first arg events and one from
        the second arg events.
        """
        selectivity = 1.0
        for arg1_name in arg1_names:
            first_index = all_events_names.index(arg1_name)
            for arg2_name in arg2_names:
                second_index = all_events_names.index(arg2_name)
                selectivity = selectivity * selectivity_matrix[first_index][second_index]
        return selectivity

    @staticmethod
    def __handle_primitive_event(pattern, statistics,
                                 nested_topologies, nested_arrival_rates, nested_cost, nested_args):
        """
        This function updates the needed nested parameters for primitive nodes.
        """
        nested_topologies.append(None)
        nested_args.append(None)
        nested_cost.append(None)
        nested_arrival_rates.append(statistics[StatisticsTypes.ARRIVAL_RATES].pop(0))
        return pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args

    def __handle_nested_operator(self, arg, pattern, statistics,
                                 nested_topologies, nested_arrival_rates, nested_cost, nested_args):
        """
        This function is building a sub tree for a given nested operator, and returns all the parameters of this subtree
        up to the main tree, that will treat this subtree as a fake leaf.
        """
        nested_pattern = TreePlanBuilder.__create_dummy_subpattern(pattern, arg)
        nested_statistics = {}
        if StatisticsTypes.ARRIVAL_RATES in statistics:
            arg_arrival_rates = statistics[StatisticsTypes.ARRIVAL_RATES][0:len(arg.get_all_event_names())]
            nested_statistics[StatisticsTypes.ARRIVAL_RATES] = arg_arrival_rates
        if StatisticsTypes.SELECTIVITY_MATRIX in statistics:
            # Take only the relevant part of the selectivity matrix:
            nested_statistics[StatisticsTypes.SELECTIVITY_MATRIX] = \
                TreePlanBuilder.__chop_matrix(pattern, arg, statistics[StatisticsTypes.SELECTIVITY_MATRIX])
        nested_topology, nested_statistics = self.__create_nested_topology(nested_pattern, nested_statistics)
        nested_topologies.append(nested_topology)  # Save nested topology to add it as a field to its root
        if len(statistics) > 0:
            # Get the cost of the nested structure to calculate the new arrival rate and save it for other
            # functions (all functions that uses cost of a tree and will need the cost of nested subtrees).
            cost = IntermediateResultsTreeCostModel().get_plan_cost(nested_pattern, nested_topology, nested_statistics)
            if isinstance(arg, UnaryStructure):
                if isinstance(arg, KleeneClosureOperator):
                    nested_arrival_rates.append(pow(2, cost) / pattern.window.total_seconds())
                else:
                    raise Exception("Unsupported unary operator")
            else:
                nested_arrival_rates.append(cost / pattern.window.total_seconds())
            nested_cost.append(cost)
        else:
            nested_cost.append(None)
        nested_args.append(arg.args if isinstance(arg, CompositeStructure) else arg.arg)
        return pattern, nested_topologies, nested_arrival_rates, nested_cost, nested_args

    @staticmethod
    def __chop_matrix(pattern: Pattern, arg: PatternStructure, selectivity_matrix: List[List[float]]):
        """
        Chop the matrix and returns only the rows and columns that are relevant to this arg (assuming this arg is in
        pattern's operators), based on the nested events' name in this arg.
        """
        if pattern.count_primitive_events(positive_only=True) != len(selectivity_matrix):
            raise Exception("size mismatch")
        event_names = [name for name in pattern.positive_structure.get_all_event_names()]
        primitive_sons = [name for name in arg.get_all_event_names()]
        chop_start = event_names.index(primitive_sons[0])
        chop_end = event_names.index(primitive_sons[-1])
        return TreePlanBuilder.__chop_matrix_aux(selectivity_matrix, chop_start, chop_end)

    @staticmethod
    def __chop_matrix_aux(matrix, start, end):
        """
        Helper function for matrix chopping, extracting a square(from start to end) out of the matrix.
        """
        chopped_matrix = []
        for row in range(start, end+1):
            chopped_row = []
            for index in range(start, end+1):
                chopped_row.append(matrix[row][index])
            chopped_matrix.append(chopped_row)
        return chopped_matrix

    @staticmethod
    def __create_dummy_subpattern(pattern: Pattern, arg: PatternStructure):
        """
        Creates a dummy pattern representing the given argument of the given pattern.
        """
        return Pattern(arg, None, pattern.window)
