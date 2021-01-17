from abc import ABC

from base.Pattern import Pattern
from base.PatternStructure import AndOperator, SeqOperator
from plan.TreeCostModel import TreeCostModelFactory
from plan.TreeCostModels import TreeCostModels
from plan.TreePlan import TreePlan, TreePlanNode, OperatorTypes, TreePlanBinaryNode, TreePlanLeafNode
from plan.NegationAlgorithmTypes import NegationAlgorithmTypes as NegAlg
from misc.StatisticsTypes import StatisticsTypes
from bitmap import BitMap


def traverse_pos_tree_plan(tree_topology, pointers_to_leaves_map, pos_events_num):
    """
    This function traverse the positive tree plan and updates the descendants bitmap of each internal node,
    the pointers to all leaf nodes map, and the "parent" field of each node
    """
    if type(tree_topology) == TreePlanLeafNode:
        return
    traverse_pos_tree_plan(tree_topology.left_child, pointers_to_leaves_map, pos_events_num)
    traverse_pos_tree_plan(tree_topology.right_child, pointers_to_leaves_map, pos_events_num)
    tree_topology.right_child.parent = tree_topology
    tree_topology.left_child.parent = tree_topology
    tree_topology.descendants_bitmap = BitMap(pos_events_num)
    if type(tree_topology.left_child) == TreePlanLeafNode:
        pointers_to_leaves_map[tree_topology.left_child.event_index] = tree_topology.left_child
        tree_topology.descendants_bitmap.set(tree_topology.left_child.event_index)
    else:
        for idx in tree_topology.left_child.descendants_bitmap.nonzero():
            tree_topology.descendants_bitmap.set(idx)
    if type(tree_topology.right_child) == TreePlanLeafNode:
        pointers_to_leaves_map[tree_topology.right_child.event_index] = tree_topology.right_child
        tree_topology.descendants_bitmap.set(tree_topology.right_child.event_index)
    else:
        for idx in tree_topology.right_child.descendants_bitmap.nonzero():
            tree_topology.descendants_bitmap.set(idx)


def extract_stat(event_idx, pattern):
    """
    Given the index of an event at the pattern structure (the full one), the function finds its arrival statistic
    """
    if pattern.statistics_type is StatisticsTypes.ARRIVAL_RATES:
        return pattern.full_statistics[event_idx]
    if pattern.statistics_type is StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES:
        return pattern.full_statistics[1][event_idx]


def create_sorted_stat_list(pattern, negative_events_chunk=None):
    """
    This function returns list of negative events sorted by their arrival stats.
    """
    range_to_sort = range(0, len(
        pattern.negative_structure.get_args())) if negative_events_chunk is None else negative_events_chunk
    # list of tuples, each tuple saves a pair of negative event and its arrival statistic
    idx_statistics_list = []
    for i in range_to_sort:
        # find the arrival rate of the specific negative event
        i_neg_event_statistic = extract_stat(pattern.negative_structure.args[i].arg.orig_idx, pattern)
        idx_statistics_list.append((i, i_neg_event_statistic))
    # sorting the negative events according to the arrival rates statistic
    idx_statistics_list.sort(key=lambda x: x[1], reverse=True)
    return idx_statistics_list


def find_lowest_pos(pos_related_list, pointers_to_leaves_map):
    """
    Finds the lowest possible position in the tree for a negative node that is related to the positive events included
    in pos_related_list (the lowest node that all the leaf nodes represents pos_related_list are its descendants)
    """
    prev = pointers_to_leaves_map[pos_related_list[0]]
    current = prev.parent
    while len(pos_related_list) > 0 and current is not None:
        if current.operator != OperatorTypes.NAND and current.operator != OperatorTypes.NSEQ:
            while len(pos_related_list) > 0 and current.descendants_bitmap.test(pos_related_list[0]):
                del pos_related_list[0]
        prev = current
        current = current.parent
    return prev


def add_negative_event(negative_event_to_insert, place_to_add, neg_operator_type):
    """
    This func inserts the negative node and the negative event itself (internal node and leaf node) to the tree plan
    and updates all relevant nodes
    """
    new_node = TreePlanBinaryNode(neg_operator_type, place_to_add, TreePlanLeafNode(negative_event_to_insert))
    new_node.parent = place_to_add.parent
    if new_node.parent is not None:
        if new_node.parent.left_child == place_to_add:
            new_node.parent.left_child = new_node
        else:
            new_node.parent.right_child = new_node
    place_to_add.parent = new_node
    new_node.right_child.parent = new_node


def add_chunk_to_tree(negative_events_chunk, pattern, place_to_add):
    """
    Iterates all nodes in chunk (=nodes that have the same list of related positive nodes)
    and adds them to the tree one by one.
    """
    for negative_event in negative_events_chunk:
        # In case there are already negative internal nodes in the position we're trying to insert the chunk to,
        # we check if the nodes to insert should be above or below the existing one (according to the arrival stats)
        while place_to_add.parent is not None and (place_to_add.parent.operator == OperatorTypes.NAND
                                or place_to_add.parent.operator == OperatorTypes.NSEQ):
            idx_in_neg_struct = place_to_add.parent.right_child.event_index - len(pattern.positive_structure.get_args())
            orig_idx_of_neg_event = pattern.negative_structure.get_args()[idx_in_neg_struct].arg.orig_idx
            parent_neg_event_stats = extract_stat(orig_idx_of_neg_event, pattern)
            if parent_neg_event_stats > negative_event[1]:
                place_to_add = place_to_add.parent
            else:
                break
        neg_operator_type = OperatorTypes.NAND if isinstance(pattern.full_structure, AndOperator) else OperatorTypes.NSEQ
        add_negative_event(negative_event[0] + len(pattern.positive_structure.get_args()), place_to_add, neg_operator_type)
        place_to_add = place_to_add.parent
    return place_to_add


def find_negative_chunk(i, right_positive_idx, pattern):
    """
    The function finds sequence of negative events that are related to the same positive events (a chunk might include
    only one negative event in case there is no other event has the same related positive events
    """
    negative_chunk = []
    negative_events_list = pattern.negative_structure.get_args()
    # Increases the right positive idx as long as it points a positive event to the left of the i(th) negative event
    while pattern.positive_structure.get_args()[right_positive_idx].orig_idx < negative_events_list[i].arg.orig_idx:
        right_positive_idx += 1
        # In this case we're at the end of the pattern and there are only negative events there (so they should all be
        # in the same chunk)
        if right_positive_idx == len(pattern.positive_structure.get_args()):
            negative_chunk = [*range(i, len(negative_events_list))]
            for neg_idx in negative_chunk:
                negative_events_list[neg_idx].pos_related_idxs.update({right_positive_idx-1})
            i = len(negative_events_list)
            break
    if right_positive_idx != len(pattern.positive_structure.get_args()):
        set_edges = {right_positive_idx}
        if right_positive_idx > 0:
            set_edges.add(right_positive_idx-1)
        negative_events_list[i].pos_related_idxs.update(set_edges)
        negative_chunk.append(i)
        i += 1
        # As long as the neg events in the list are still to the left of the right pos idx
        while i < len(negative_events_list) and negative_events_list[i].arg.orig_idx <\
                pattern.positive_structure.get_args()[right_positive_idx].orig_idx:
            negative_events_list[i].pos_related_idxs.update(set_edges)
            # If the events are related to the same positive events they should be inserted to the same place on tree
            if negative_events_list[i].pos_related_idxs == negative_events_list[i-1].pos_related_idxs:
                negative_chunk.append(i)
                i += 1
            else:
                break
    return negative_chunk, i, right_positive_idx


def find_lowest_pos_and_add_negs(pattern, tree_topology, pointers_to_leaves_map):
    """
    The main function of the LOWEST POSITION negation algorithm. It iterates all negative events, and call to
    sub functions that find chunks of negative events (events that are related to the same positive event and therefore
    should be inserted to the same place on tree), sorts each chunk statistically (in order to insert the nodes of the
    chunk in the most beneficial order), find the lowest position to add the chunk and eventually add it.
    """
    i = 0
    negative_events_list = pattern.negative_structure.get_args()
    # The index represents the place of the event in the positive events list, and it indicates the most left positive
    # event to the right of the negative event, i.e. the positive event that is the most close to the negative event
    # from its right side
    right_positive_idx = negative_events_list[0].arg.orig_idx - 1 if\
        negative_events_list[0].arg.orig_idx != 0 else 0
    while i < len(negative_events_list):
        # Find chunk
        negative_events_chunk, i, right_positive_idx = find_negative_chunk(i, right_positive_idx, pattern)
        # Sort chunk
        negative_events_chunk = create_sorted_stat_list(pattern, negative_events_chunk)
        # In case there are negative events at the beginning of the pattern
        if right_positive_idx == 0:
            place_to_add = tree_topology
        # In case there are negative events at the end of the pattern
        elif right_positive_idx == len(pattern.positive_structure.get_args()):
            current = tree_topology
            while current.operator == OperatorTypes.NSEQ or current.operator == OperatorTypes.NAND:
                current = current.left_child
            place_to_add = current
        else:
            # Find lowest place in tree
            place_to_add = find_lowest_pos(list(negative_events_list[negative_events_chunk[0][0]].pos_related_idxs),
                                       pointers_to_leaves_map)
        # Add chunk to tree
        potential_new_root = add_chunk_to_tree(negative_events_chunk, pattern, place_to_add)
        if potential_new_root.parent is None:
            tree_topology = potential_new_root

    return tree_topology


class TreePlanBuilder(ABC):
    """
    The base class for the builders of tree-based plans.
    """
    def __init__(self, cost_model_type: TreeCostModels):
        self.__cost_model = TreeCostModelFactory.create_cost_model(cost_model_type)

    def build_tree_plan(self, pattern: Pattern):
        """
        Creates a tree-based evaluation plan for the given pattern.
        """
        return TreePlan(self._create_tree_topology(pattern))

    def _create_tree_topology(self, pattern: Pattern):
        """
        An abstract method for creating the actual tree topology.
        """
        raise NotImplementedError()

    def _get_plan_cost(self, pattern: Pattern, plan: TreePlanNode):
        """
        Returns the cost of a given plan for the given plan according to a predefined cost model.
        """
        return self.__cost_model.get_plan_cost(pattern, plan)

    @staticmethod
    def _add_negative_part(positive_subtree: TreePlanBinaryNode, pattern: Pattern):
        """
        This method adds the negative part to the tree plan (that includes only the positive part),
        according to the negation algorithm the user chose
        """
        tree_topology = positive_subtree
        if pattern.negative_structure is not None:
            neg_algo = pattern.negation_algorithm
            negative_events_num = len(pattern.negative_structure.get_args())
            order = [*range(len(pattern.positive_structure.get_args()), len(pattern.full_structure.get_args()))]
            # If the neg alg is Lowest Pos, but the top operator is "AND", all the negative nodes are unbounded and
            # therefore should be inserted on the top of the tree, so the Stat neg alg would handle it the best.
            flag = True if pattern.full_structure.get_top_operator() == AndOperator else False
            if (neg_algo == NegAlg.STATISTIC_NEGATION_ALGORITHM and pattern.full_statistics is not None) or \
               (neg_algo == NegAlg.LOWEST_POSITION_NEGATION_ALGORITHM and flag):
                idx_statistics_list = create_sorted_stat_list(pattern)
                # Rearrange "order" negative events by the desired order (according to the statistics)
                for i in range(0, negative_events_num):
                    order[i] = idx_statistics_list[i][0] + len(pattern.positive_structure.get_args())
            # The neg part being added to the tree plan (in case of naive or statistic neg algs only)
            if pattern.negation_algorithm != NegAlg.LOWEST_POSITION_NEGATION_ALGORITHM or flag:
                for i in range(0, negative_events_num):
                    tree_topology = TreePlanBuilder._instantiate_binary_node(pattern, tree_topology, TreePlanLeafNode(order[i]))
            if pattern.negation_algorithm == NegAlg.LOWEST_POSITION_NEGATION_ALGORITHM and not flag:
                pattern.update_conds_lists()
                # Holds the pointers to all leaf nodes of the tree plan (which includes only the positive part so far)
                pointers_to_leaves_map = {}
                traverse_pos_tree_plan(tree_topology, pointers_to_leaves_map, len(pattern.positive_structure.get_args()))
                tree_topology = find_lowest_pos_and_add_negs(pattern, tree_topology, pointers_to_leaves_map)
        return tree_topology

    @staticmethod
    def _instantiate_binary_node(pattern: Pattern, left_subtree: TreePlanNode, right_subtree: TreePlanNode):
        """
        A helper method for the subclasses to instantiate tree plan nodes depending on the operator.
        """
        pattern_structure = pattern.positive_structure
        if isinstance(pattern_structure, AndOperator):
            operator_type = OperatorTypes.AND
        elif isinstance(pattern_structure, SeqOperator):
            operator_type = OperatorTypes.SEQ
        else:
            raise Exception("Unsupported binary operator")
        if pattern.negative_structure is not None:
            if type(right_subtree) is TreePlanLeafNode:
                if right_subtree.event_index >= len(pattern.positive_structure.get_args()):
                    if isinstance(pattern_structure, AndOperator):
                        operator_type = OperatorTypes.NAND
                    else:
                        operator_type = OperatorTypes.NSEQ
        return TreePlanBinaryNode(operator_type, left_subtree, right_subtree)
