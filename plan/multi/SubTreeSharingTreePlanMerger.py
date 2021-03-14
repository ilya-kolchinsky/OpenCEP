from plan.TreePlan import TreePlanNode
from plan.multi.RecursiveTraversalTreePlanMerger import RecursiveTraversalTreePlanMerger


class SubTreeSharingTreePlanMerger(RecursiveTraversalTreePlanMerger):
    """
    Merges the given patterns by sharing arbitrary equivalent subtrees.
    """
    def _are_suitable_for_share(self, first_node: TreePlanNode, second_node: TreePlanNode):
        """
        This algorithm allows any type of node to be shared.
        """
        return first_node.is_equivalent(second_node)
