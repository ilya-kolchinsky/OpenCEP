from plan.TreePlan import TreePlanNode, TreePlanLeafNode
from plan.multi.RecursiveTraversalTreePlanMerger import RecursiveTraversalTreePlanMerger


class ShareLeavesTreePlanMerger(RecursiveTraversalTreePlanMerger):
    """
    Merges the given patterns by sharing equivalent leaves.
    """
    def _are_suitable_for_share(self, first_node: TreePlanNode, second_node: TreePlanNode):
        """
        This algorithm restricts all shareable nodes to be tree plan leaves.
        """
        return isinstance(first_node, TreePlanLeafNode) and \
               isinstance(second_node, TreePlanLeafNode) and first_node.is_equivalent(second_node)
