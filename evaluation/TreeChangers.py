from abc import ABC, abstractmethod
from tree.Tree import Tree


class TreeChanger(ABC):

    @abstractmethod
    def run(self, tree: Tree):
        pass


class TrivialTreeChanger(TreeChanger):

    def run(self, tree: Tree):
        pass


class ParallelTreeChanger(TreeChanger):

    def run(self, tree: Tree):
        pass