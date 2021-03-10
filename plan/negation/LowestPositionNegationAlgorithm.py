from plan.negation.NegationAlgorithm import *
from plan.negation.StatisticNegationAlgorithm import StatisticNegationAlgorithm
from base.PatternStructure import AndOperator
from plan.TreePlan import OperatorTypes, TreePlanBinaryNode, TreePlanLeafNode


class LowestPositionNegationAlgorithm(NegationAlgorithm):
    """
    This class represents the lowest position negation algorithm, and saves the data related to it.
    """
    def __init__(self):
        super().__init__(NegationAlgorithmTypes.LOWEST_POSITION_NEGATION_ALGORITHM)
        self.positive_related_events_per_each_negative_event = None
        self.internal_node_descendants_map = {}
        self.pointers_to_leaves_map = {}

    @staticmethod
    def create_name_index_map(pattern: Pattern):
        """
        Creates a mapping from event's name to its fake index (the index in the positive structure or the negative one)
        """
        name_index_map = {}
        for index, arg in enumerate(pattern.negative_structure.args):
            name_index_map[arg.arg.name] = index + len(pattern.positive_structure.args)
        for index, arg in enumerate(pattern.positive_structure.args):
            name_index_map[arg.name] = index
        return name_index_map

    def update_related_conditions(self, pattern: Pattern):
        """
        Updates the "positive_related_events_per_each_negative_event" list, with the indices of the positive events
        each negative one have condition with.
        """
        self.positive_related_events_per_each_negative_event = [set() for _ in
                                                                range(len(pattern.negative_structure.args))]
        positive_events_num = len(pattern.positive_structure.args)
        name_index_map = self.create_name_index_map(pattern)
        for condition in pattern.condition.get_conditions_list():
            negative_indices = set()
            positive_indices = set()
            for term in condition.terms:
                term_index = name_index_map[term.name]
                if term_index >= positive_events_num:
                    negative_indices.add(term_index - positive_events_num)
                else:
                    positive_indices.add(term_index)
                for neg_index in negative_indices:
                    self.positive_related_events_per_each_negative_event[neg_index].update(positive_indices)

    def update_maps(self, node: TreePlanNode):
        """
        Updates pointers_to_leaves_map and internal_node_descendants_map in case the node is leaf node, and
        internal_node_descendants_map only in case of internal node
        """
        if type(node) == TreePlanLeafNode:
            self.pointers_to_leaves_map[node.event_index] = node
            self.internal_node_descendants_map[node.parent][node.event_index] = True
        else:
            for index, value in enumerate(self.internal_node_descendants_map[node]):
                if value is True:
                    self.internal_node_descendants_map[node][index] = True

    def traverse_positive_tree_plan(self, tree_topology, positive_events_num):
        """
        This function traverse the positive tree plan and updates the descendants array of each internal node,
        the pointers to all leaf nodes map, and the "parent" field of each node
        """
        if type(tree_topology) == TreePlanLeafNode:
            return
        self.traverse_positive_tree_plan(tree_topology.left_child, positive_events_num)
        self.traverse_positive_tree_plan(tree_topology.right_child, positive_events_num)
        tree_topology.right_child.parent = tree_topology
        tree_topology.left_child.parent = tree_topology
        self.internal_node_descendants_map[tree_topology] = [False]*positive_events_num
        self.update_maps(tree_topology.left_child)
        self.update_maps(tree_topology.right_child)

    def find_negative_chunk(self, negative_iterator, right_positive_index, pattern):
        """
        The function finds sequence of negative events that are related to the same positive events (a chunk might include
        only one negative event in case there is no other event has the same related positive events
        """
        negative_chunk = []
        negative_events_num = len(pattern.negative_structure.args)
        # Increases the right positive idx as long as it points a positive event to the left of the i(th) negative event
        while self.positive_original_indices[right_positive_index] < self.negative_original_indices[negative_iterator]:
            right_positive_index += 1
            # In this case we're at the end of the pattern and there are only negative events there (so they should all
            # be in the same chunk)
            if right_positive_index == len(pattern.positive_structure.args):
                negative_chunk = [*range(negative_iterator, negative_events_num)]
                for negative_index in negative_chunk:
                    self.positive_related_events_per_each_negative_event[negative_index].update({right_positive_index - 1})
                negative_iterator = negative_events_num
                break
        if right_positive_index != len(pattern.positive_structure.args):
            set_edges = {right_positive_index}
            if right_positive_index > 0:
                set_edges.add(right_positive_index - 1)
            self.positive_related_events_per_each_negative_event[negative_iterator].update(set_edges)
            negative_chunk.append(negative_iterator)
            negative_iterator += 1
            # As long as the negative events in the list are still to the left of the right positive index
            while negative_iterator < negative_events_num and self.negative_original_indices[negative_iterator] < \
                    self.positive_original_indices[right_positive_index]:
                self.positive_related_events_per_each_negative_event[negative_iterator].update(set_edges)
                # If the events are related to the same positive events they should be inserted to the same place on tree
                if self.positive_related_events_per_each_negative_event[negative_iterator] == \
                        self.positive_related_events_per_each_negative_event[negative_iterator-1]:
                    negative_chunk.append(negative_iterator)
                    negative_iterator += 1
                else:
                    break
        return negative_chunk, negative_iterator, right_positive_index

    def find_lowest_position(self, positive_related_list):
        """
        Finds the lowest possible position in the tree for a negative node that is related to the positive events included
        in pos_related_list (the lowest node that all the leaf nodes represents pos_related_list are its descendants)
        """
        lowest_position = self.pointers_to_leaves_map[positive_related_list[0]]
        temp_lowest_position = lowest_position.parent
        while len(positive_related_list) > 0 and temp_lowest_position is not None:
            if temp_lowest_position.operator != OperatorTypes.NAND and temp_lowest_position.operator != OperatorTypes.NSEQ:
                while len(positive_related_list) > 0 and self.internal_node_descendants_map[temp_lowest_position][positive_related_list[0]]:
                    del positive_related_list[0]
            lowest_position = temp_lowest_position
            temp_lowest_position = temp_lowest_position.parent
        return lowest_position

    @staticmethod
    def add_negative_event(negative_event_to_insert, place_to_add, negative_operator_type):
        """
        This function inserts the negative node and the negative event itself (internal node and leaf node) to the tree
        plan and updates all relevant nodes
        """
        new_node = TreePlanBinaryNode(negative_operator_type, place_to_add, TreePlanLeafNode(negative_event_to_insert))
        new_node.parent = place_to_add.parent
        if new_node.parent is not None:
            if new_node.parent.left_child == place_to_add:
                new_node.parent.left_child = new_node
            else:
                new_node.parent.right_child = new_node
        place_to_add.parent = new_node
        new_node.right_child.parent = new_node

    def add_chunk_to_tree(self, negative_events_chunk, pattern, place_to_add):
        """
        Iterates all nodes in chunk (=nodes that have the same list of related positive nodes)
        and adds them to the tree one by one.
        """
        for negative_event in negative_events_chunk:
            # In case there are already negative internal nodes in the position we're trying to insert the chunk to,
            # we check if the nodes to insert should be above or below the existing one (according to the arrival stats)
            while place_to_add.parent is not None and (place_to_add.parent.operator == OperatorTypes.NAND
                                                       or place_to_add.parent.operator == OperatorTypes.NSEQ):
                index_in_negative_struct = place_to_add.parent.right_child.event_index - len(pattern.positive_structure.args)
                original_index_of_negative_event = self.negative_original_indices[index_in_negative_struct]
                parent_negative_event_statistics = self.extract_statistics(original_index_of_negative_event, pattern)
                if parent_negative_event_statistics > negative_event[1]:
                    place_to_add = place_to_add.parent
                else:
                    break
            negative_operator_type = OperatorTypes.NAND if \
                isinstance(pattern.full_structure, AndOperator) else OperatorTypes.NSEQ
            self.add_negative_event(negative_event[0] +
                                    len(pattern.positive_structure.args), place_to_add, negative_operator_type)
            place_to_add = place_to_add.parent
        return place_to_add

    def find_lowest_position_and_add_negative_nodes(self, pattern, tree_topology):
        """
        The main function of the LOWEST POSITION negation algorithm. It iterates all negative events, and call to
        sub functions that find chunks of negative events (events that are related to the same positive event and therefore
        should be inserted to the same place on tree), sorts each chunk statistically (in order to insert the nodes of the
        chunk in the most beneficial order), find the lowest position to add the chunk and eventually add it.
        """
        negative_iterator = 0
        # The index represents the place of the event in the positive events list, and it indicates the most left
        # positive event to the right of the negative event, i.e. the positive event that is the most close to the
        # negative event from its right side
        right_positive_index = self.negative_original_indices[0] - 1 if self.negative_original_indices[0] != 0 else 0
        while negative_iterator < len(self.negative_original_indices):
            # Find chunk
            negative_events_chunk, negative_iterator, right_positive_index = \
                self.find_negative_chunk(negative_iterator, right_positive_index, pattern)
            # Sort chunk
            negative_events_chunk = self.create_sorted_statistics_list(pattern, negative_events_chunk)
            # In case there are negative events at the beginning of the pattern
            if right_positive_index == 0:
                place_to_add = tree_topology
            # In case there are negative events at the end of the pattern
            elif right_positive_index == len(pattern.positive_structure.args):
                current = tree_topology
                while current.operator == OperatorTypes.NSEQ or current.operator == OperatorTypes.NAND:
                    current = current.left_child
                place_to_add = current
            else:
                # Find lowest place in tree
                place_to_add = self.find_lowest_position(list(self.positive_related_events_per_each_negative_event[negative_events_chunk[0][0]]))
            # Add chunk to tree
            potential_new_root = self.add_chunk_to_tree(negative_events_chunk, pattern, place_to_add)
            if potential_new_root.parent is None:
                tree_topology = potential_new_root
        return tree_topology

    def add_negative_part(self, pattern: Pattern, positive_tree_plan: TreePlanBinaryNode):
        """
        This method adds the negative part to the tree plan (that includes only the positive part),
        according to the lowest position algorithm, i.e. - each negative node inserted at the lowest position possible
        (such that the positive nodes related to it are beneath it).
        """
        tree_topology = positive_tree_plan
        if pattern.negative_structure is None:
            return tree_topology
        # If the negation algorithm is Lowest Position, but the top operator is "AND", all the negative nodes are
        # unbounded and therefore should be inserted on the top of the tree, so the Statistic negation algorithm
        # would handle it the best.
        if pattern.full_structure.get_top_operator() == AndOperator:
            statistic_helper_object = StatisticNegationAlgorithm(NegationAlgorithmTypes.STATISTIC_NEGATION_ALGORITHM)
            return statistic_helper_object.add_negative_part(pattern, positive_tree_plan)
        self.calculate_original_indices(pattern)
        self.update_related_conditions(pattern)
        # Holds the pointers to all leaf nodes of the tree plan (which includes only the positive part so far)
        self.traverse_positive_tree_plan(tree_topology, len(pattern.positive_structure.args))
        tree_topology = self.find_lowest_position_and_add_negative_nodes(pattern, tree_topology)
        self.adjust_tree_plan_indices(tree_topology, pattern)
        return tree_topology
