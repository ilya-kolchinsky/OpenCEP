from abc import ABC
from typing import Dict

from base.Pattern import Pattern
from plan.TreePlan import TreePlan


class TreePlanMerger(ABC):
    """
    An abstract class for tree plan mergers utilizing various multi-pattern sharing techniques.
    """
    def merge_tree_plans(self, pattern_to_tree_plan_map: Dict[Pattern, TreePlan]):
        """
        Merges the given tree plans of individual tree plans into a global shared structure.
        """
        raise NotImplementedError()
