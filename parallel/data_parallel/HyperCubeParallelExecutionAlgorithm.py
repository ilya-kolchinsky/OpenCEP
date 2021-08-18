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
from misc.Utils import array, ndarray
from misc.Utils import is_int, is_float
from typing import Tuple, Set


class HyperCubeParallelExecutionAlgorithm(DataParallelExecutionAlgorithm, ABC):
    """
    Implements the HyperCube algorithm.
    attributes_dict - Dictionary contains keys->attribute, and values->parameters.
    units_number - Indicate the number of units/threads to run, doesn't include the "main execution unit".
    """

    def __init__(self, units_number, patterns: Pattern or List[Pattern],
                 eval_mechanism_params: EvaluationMechanismParameters, platform, attributes_dict: dict):
        if isinstance(patterns, Pattern):
            patterns = [patterns]
        for pattern in patterns:
            primitive_events = pattern.get_primitive_events()
            types = [e.type for e in primitive_events]
            if len(set(types)) != len(types):
                raise Exception(f"Not Support multiple typing of events, got {primitive_events}")

        dims = 0
        self._attributes_dict = dict()
        # alloc cube dimension for every attribute value
        for k, v in attributes_dict.items():
            if isinstance(v, list):
                self._attributes_dict[k] = [(v, dims + i) for i, v in enumerate(v)]
                dims += len(v)
            elif isinstance(v, str):
                self._attributes_dict[k] = [(v, dims)]
                dims += 1
            else:
                raise Exception
        if not self._attributes_dict:
            raise Exception("attributes_dict is empty")

        # create ndarray cube

        shares, cube_size = self._calc_cubic_shares(units_number, dims)
        self._cube = array(range(cube_size)).reshape(shares)
        super().__init__(self._cube.size, patterns, eval_mechanism_params, platform)

    def _classifier(self, event: Event) -> Set[int]:
        """
        :param event: from the input fileStream
        :return: The classification unit/s that this event will be sent to
        """
        attributes = self._attributes_dict.get(event.type)  # get event attributes
        if attributes:  # if attributes exists we need to find the right dimension(s) of the cube to return
            units = set()
            for attribute, index in attributes:  # loop of events attributes
                # find for all attributes the union of the column/"cube face"
                indices = [slice(None)] * self._cube.ndim  # default is slice(None) (all dim units)
                value = event.payload.get(attribute)
                # check correctness
                if value is None or (not is_int(value) and not is_float(value)):
                    return set()

                col = int(value) % self._cube.shape[index]  # column number
                indices[index] = slice(col, col + 1)
                selected_units = self._cube[tuple(indices)]
                if isinstance(selected_units, ndarray):
                    selected_units = list(self._cube[tuple(indices)].reshape(-1))
                units.update(set(selected_units))  # update units set
            return units
        return set(range(self._cube.size))  # return all possible units

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
        Preforming the _classifier again on all match events and get units intersection.
        For every match we not(!) skipping if the unit has the smallest id.
        """
        def skip_item(item: PatternMatch):
            min_unit = min(reduce(set.intersection, map(self._classifier, item.events)))
            return min_unit != unit_id

        return skip_item
