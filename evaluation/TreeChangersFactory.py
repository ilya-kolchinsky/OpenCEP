from evaluation.TreeChangers import TrivialTreeChanger, ParallelTreeChanger
from misc import DefaultConfig
from misc.TreeChangerTypes import TreeChangerTypes


class TreeChangerParameters:
    """
    Parameters for the tree changer algorithm.
    """

    def __init__(self, tree_changer_type: TreeChangerTypes = DefaultConfig.DEFAULT_TREE_PLAN_BUILDER):
        self.tree_changer_type = tree_changer_type


class TreeChangerFactory:

    @staticmethod
    def create_tree_changer_algorithm(tree_changer_params: TreeChangerParameters):
        if tree_changer_params.tree_changer_type == TreeChangerTypes.TRIVIAL_TREE_CHANGER:
            return TrivialTreeChanger()

        if tree_changer_params.tree_changer_type == TreeChangerTypes.PARALLEL_TREE_CHANGER:
            return ParallelTreeChanger()

        raise Exception("Unknown tree changer type: %s" % (tree_changer_params.tree_changer_type,))
