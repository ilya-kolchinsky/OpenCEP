from base.Formula import RelopTypes, EquationSides
from tree.BinaryNode import BinaryNode
from tree.PatternMatchStorage import TreeStorageParameters


class AndNode(BinaryNode):
    """
    An internal node representing an "AND" operator.
    """
    def get_structure_summary(self):
        return ("And",
                self._left_subtree.get_structure_summary(),
                self._right_subtree.get_structure_summary())

    def get_structure_hash(self):
        return ("And",
                self._left_subtree.get_structure_hash(),
                self._right_subtree.get_structure_hash())

    def is_structure_equal(self, other):
        if not isinstance(other, type(self)):
            return False
        v1 = self._left_subtree.is_structure_equal(other.get_left_subtree())
        v2 = self._right_subtree.is_structure_equal(other.get_right_subtree())
        v3 = self._left_subtree.is_structure_equal(other.get_right_subtree())
        v4 = self._right_subtree.is_structure_equal(other.get_left_subtree())
        return (v1 and v2) or (v3 and v4)

    def create_storage_unit(self, storage_params: TreeStorageParameters, sorting_key: callable = None,
                            rel_op: RelopTypes = None, equation_side: EquationSides = None,
                            sort_by_first_timestamp: bool = False):
        self._init_storage_unit(storage_params, sorting_key, rel_op, equation_side)
        if not storage_params.sort_storage:
            # efficient storage is disabled
            self._left_subtree.create_storage_unit(storage_params)
            self._right_subtree.create_storage_unit(storage_params)
            return
        # efficient storage was explicitly enabled
        left_key, left_rel_op, left_equation_size, right_key, right_rel_op, right_equation_size = \
            self._get_condition_based_sorting_keys(storage_params.attributes_priorities)
        self._left_subtree.create_storage_unit(storage_params, left_key, left_rel_op, left_equation_size)
        self._right_subtree.create_storage_unit(storage_params, right_key, right_rel_op, right_equation_size)
