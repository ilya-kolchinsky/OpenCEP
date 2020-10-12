from parallerization.tree_implemintation.ParallelTreeWorkloadFramework import ParallelTreeWorkloadFramework
from base.Pattern import Pattern

# This new plugin is different from the other only in how it handles the distribution of the data in a multiple data
# scenario:
# it send an event only to one of the eval_mechanism at a time.
# This only works on single event patterns


class WorkloadFrameworkForSingleEventTests(ParallelTreeWorkloadFramework):
    def __init__(self, pattern: Pattern, execution_units: int = 1, is_data_parallel: bool = False,
                 is_structure_parallel: bool = False, num_of_families: int = 0):
        super().__init__(pattern, execution_units, is_data_parallel, is_structure_parallel, num_of_families)

    def get_indexes_for_families(self):
        families_indexes = []
        count = self.event_stream.count()
        execution_units = self.get_execution_units()
        families_indexes.append(count % execution_units)
        return families_indexes

    def get_indexes_for_duplicated_data(self):
        count = self.event_stream.count()
        execution_units = self.get_execution_units()
        res =[]
        res.append(count % execution_units)
        return res

