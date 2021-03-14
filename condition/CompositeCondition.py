"""
This file contains the composite condition classes.
"""
from abc import ABC

from adaptive.statistics.StatisticsCollector import StatisticsCollector
from condition.Condition import Condition, AtomicCondition
from condition.KCCondition import KCCondition


class CompositeCondition(Condition, ABC):
    """
    This class represents a composite condition consisting of a number of simple (atomic) conditions combined using
    logic operators such as conjunction and disjunction.
    """
    def __init__(self, terminating_result: bool, *condition_list):
        self.__conditions = list(condition_list)
        self.__terminating_result = terminating_result
        self._statistics_collector = None

    def eval(self, binding: dict = None):
        if self.get_num_conditions() == 0:
            return True
        for condition in self.__conditions:
            if condition.eval(binding) == self.__terminating_result:
                return self.__terminating_result
        return not self.__terminating_result

    def __eq__(self, other):
        if self == other:
            return True
        if type(self) != type(other) or self.get_num_conditions() != other.get_num_conditions():
            return False
        for condition in self.__conditions:
            if condition not in other.__conditions:
                return False
        return True


    def get_condition_of(self, names: set, get_kleene_closure_conditions=False, consume_returned_conditions=False):
        """
        Returns a new composite condition which only contains those conditions from this composite condition operating
        exclusively on the names from the given list.
        Optionally removes the returned sub-conditions from this composite condition.
        """
        result_conditions = []
        conditions_to_remove = []
        for index, current_condition in enumerate(self.__conditions):
            if isinstance(current_condition, CompositeCondition):
                inner_condition = current_condition.get_condition_of(names, get_kleene_closure_conditions,
                                                                     consume_returned_conditions)
                if inner_condition.get_num_conditions() > 0:
                    # non-empty nested condition was returned
                    result_conditions.append(inner_condition)
                if consume_returned_conditions and current_condition.get_num_conditions() == 0:
                    # all previously existing nested conditions were probably consumed - consume this empty condition
                    conditions_to_remove.append(index)
                continue
            # this is a simple condition
            if not current_condition.is_condition_of(names):
                continue
            if get_kleene_closure_conditions != isinstance(current_condition, KCCondition):
                # either this is a KC condition and we asked for non-KC conditions,
                # or this is a non-KC condition and we asked for KC conditions
                continue
            result_conditions.append(current_condition)
            if consume_returned_conditions:
                conditions_to_remove.append(index)

        # remove the conditions at previously saved indices
        for index in reversed(conditions_to_remove):
            self.__conditions.pop(index)

        return CompositeCondition(self.__terminating_result, *result_conditions)

    def get_num_conditions(self):
        """
        Returns the number of conditions encapsulated by this composite condition.
        """
        return len(self.__conditions)

    def get_conditions_list(self):
        """
        Returns the list of conditions encapsulated by this composite condition.
        """
        return self.__conditions

    def extract_atomic_conditions(self):
        result = []
        for f in self.__conditions:
            result.extend(f.extract_atomic_conditions())
        return result

    def add_atomic_condition(self, condition: AtomicCondition):
        """
        Adds a new atomic condition to this composite condition.
        """
        self.__conditions.append(condition)
        condition.set_statistics_collector(self._statistics_collector)

    def set_statistics_collector(self, statistics_collector: StatisticsCollector):
        """
        Sets the statistic collector for all contained atomic conditions.
        """
        self._statistics_collector = statistics_collector
        for condition in self.extract_atomic_conditions():
            condition.set_statistics_collector(statistics_collector)

    def __repr__(self):
        res_list = []
        for condition in self.__conditions:
            res_list.append(condition.__repr__())
        return res_list

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if len(self.__conditions) != other.get_num_conditions():
            return False
        for condition in self.__conditions:
            if condition not in other.get_conditions_list():
                return False
        return True


class AndCondition(CompositeCondition):
    """
    This class uses CompositeCondition with False as the terminating result, which complies with AND operator logic.
    AND stops at the first FALSE from the evaluation and returns False.
    """
    def __init__(self, *condition_list):
        super().__init__(False, *condition_list)

    def get_condition_of(self, names: set, get_kleene_closure_conditions=False, consume_returned_conditions=False):
        composite_condition = super().get_condition_of(names, get_kleene_closure_conditions, consume_returned_conditions)
        # at-least 1 condition was retrieved using get_condition_of for the list of conditions
        if composite_condition:
            return AndCondition(*composite_condition.get_conditions_list())
        return None

    def __repr__(self):
        return " AND ".join(super().__repr__())

    def get_event_names(self):
        """
        Returns the event names associated with this condition.
        """
        sets_of_names = list(condition.get_event_names() for condition in self.get_conditions_list())
        flat_list = [item for sublist in sets_of_names for item in sublist]
        return set(flat_list)


class OrCondition(CompositeCondition):
    """
    This class uses CompositeCondition with True as the terminating result, which complies with OR operator logic.
    OR stops at the first TRUE from the evaluation and return True.
    """
    def __init__(self, *condition_list):
        super().__init__(True, *condition_list)

    def get_condition_of(self, names: set, get_kleene_closure_conditions=False, consume_returned_conditions=False):
        composite_condition = super().get_condition_of(names, get_kleene_closure_conditions, consume_returned_conditions)
        # at-least 1 condition was retrieved using get_condition_of for the list of conditions
        if composite_condition:
            return OrCondition(*composite_condition.get_conditions_list())
        return None

    def __repr__(self):
        return " OR ".join(super().__repr__())
