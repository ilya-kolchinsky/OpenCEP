"""
 Data parallel HyperCube algorithms
"""
from abc import ABC
from parallel.data_parallel.DataParallelExecutionAlgorithm import DataParallelExecutionAlgorithm
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from math import floor
from base.PatternMatch import *
from functools import reduce
# from numpy import array
from misc.Utils import is_int, is_float
from typing import Tuple, Set, Union
from collections.abc import Sequence


class array:
    """
    Simple implementation of numpy.array
    """

    def __init__(self, array_like):
        if not isinstance(array_like, Sequence):
            array_like = [array_like]
        self._data = array_like
        shape = list()
        if self._data:
            temp = self._data
            while isinstance(temp, (Sequence, array)):
                shape.append(len(temp))
                temp = temp[0]
        self.shape = tuple(shape)
        self.size = self._list_mul(self.shape)
        self.ndim = len(self.shape)

    def reshape(self, *newshape: Union[int, Tuple[int], List[int]]):
        if all(isinstance(d, int) for d in newshape):
            pass
        elif not isinstance(newshape[0], Sequence) or len(newshape) != 1:
            raise TypeError
        else:
            newshape = newshape[0]
        newshape = list(newshape)
        newsize = self._list_mul(newshape)
        if newshape.count(-1) > 1:
            raise ValueError("can only specify one unknown dimension")
        if -1 not in newshape:
            if newsize != self.size:
                raise ValueError(f'cannot reshape array of size {self.size} into shape {tuple(newshape)}')
        else:
            newsize *= -1
            if abs(newsize) > self.size:
                raise ValueError(f'cannot reshape array of size {self.size} into shape {tuple(newshape)}')
            newshape[newshape.index(-1)] = self.size // newsize

        flat_array = self._ndarray_to_1darray()
        if len(newshape) <= 1:
            return flat_array
        else:
            return flat_array._1darray_to_ndarray(newshape)

    def __getitem__(self, *slices):
        if all(isinstance(d, int) for d in slices):
            pass
        elif not isinstance(slices[0], Sequence) or len(slices) != 1:
            raise TypeError
        else:
            slices = slices[0]
        slices = list(slices)
        if len(slices) > self.ndim:
            raise ValueError
        if not slices:
            return self
        elif len(slices) == 1:
            return self._data[slices[0]]
        elif isinstance(slices[0], int):
            return array(self._data[slices[0]])[slices[1:]]
        else:
            return array([array(layer)[slices[1:]] for layer in self._data[slices[0]]])

    def __repr__(self):
        return self._data.__repr__()

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return self.shape[0] if self.shape else 0

    @staticmethod
    def _list_mul(lst):
        return reduce(lambda a, b: a * b, lst)

    def _ndarray_to_1darray(self):
        def matrix_to_list(matrix):
            return [item for sublist in matrix for item in sublist]

        new_list = self._data
        if new_list:
            for _ in range(self.ndim - 1):
                new_list = matrix_to_list(new_list)
        return array(new_list)

    def _1darray_to_ndarray(self, newshape):
        def list_to_matrix(flat_list, inner_dim):
            m = len(flat_list) // inner_dim
            return [[flat_list[i + inner_dim * j] for i in range(inner_dim)] for j in range(m)]

        new_list = self._data
        if len(newshape) > 1:
            for d in newshape[-1:0:-1]:
                new_list = list_to_matrix(new_list, d)
        return array(new_list)


class HyperCubeParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the HyperCube algorithm.
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform, attributes_dict: dict):
        dims = 0
        self.attributes_dict = dict()
        for k, v in attributes_dict.items():
            if isinstance(v, list):
                self.attributes_dict[k] = [(v, dims + i) for i, v in enumerate(v)]
                dims += len(v)
            elif isinstance(v, str):
                self.attributes_dict[k] = [(v, dims)]
                dims += 1
            else:
                raise Exception
        shares, cube_size = self._calc_cubic_shares(units_number, dims)
        self.cube = array(range(cube_size)).reshape(shares)
        super().__init__(self.cube.size, patterns, eval_mechanism_params, platform)

    def _classifier(self, event: Event) -> Set[int]:
        attributes = self.attributes_dict.get(event.type)
        if attributes:
            units = set()
            for attribute, index in attributes:
                # find for all attributes the union of the column/cube face
                indices = [slice(None)] * self.cube.ndim
                value = event.payload.get(attribute)
                if value is None:
                    raise Exception(f"attribute {attribute} is not existing in type {event.type}")
                elif not is_int(value) and not is_float(value):
                    raise Exception(f"Non numeric key {attribute} = {value}")
                col = int(value) % self.cube.shape[index]
                indices[index] = slice(col, col + 1)
                units.update(set(list(self.cube[tuple(indices)].reshape(-1))))
            return units
        return set(range(self.cube.size))

    @staticmethod
    def _calc_cubic_shares(units_number, dims) -> Tuple[Tuple[int], int]:
        """
        find the most equal share, and try to improve it with extra units
        for example: _calc_cubic_shares(10, 2) = [3,3] not [5,2]
        max(shares)-min(shares) <= 1
        """
        equal_share = floor(units_number ** (1 / dims))
        shares = [equal_share] * dims

        def used_units() -> int:
            return reduce(lambda a, b: a * b, shares)

        change = True
        while change:
            change = False
            for s in range(len(shares)):
                if used_units() / shares[s] * (shares[s] + 1) <= units_number:
                    shares[s] += 1
                    change = True
                else:
                    continue
        return shares, used_units()

    def _create_skip_item(self, unit_id: int):
        """
        Creates and returns FilterStream object.
        """

        def skip_item(item: PatternMatch):
            min_unit = min(reduce(set.intersection, map(self._classifier, item.events)))
            return min_unit == unit_id

        return skip_item
