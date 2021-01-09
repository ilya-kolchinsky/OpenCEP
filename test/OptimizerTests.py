from datetime import timedelta

from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.Condition import SimpleCondition, Variable
from misc.OptimizerTypes import OptimizerTypes
from misc.StatisticsTypes import StatisticsTypes
from optimizer.OptimizerFactory import OptimizerFactory, InvariantsAwareOptimizerParameters

from plan.TreeCostModels import TreeCostModels
from plan.TreePlanBuilderFactory import TreePlanBuilderParameters
from plan.TreePlanBuilderTypes import TreePlanBuilderTypes



def get_pattern_test():
    pattern = Pattern(
        SeqOperator(PrimitiveEventStructure("AAPL", "a"), PrimitiveEventStructure("AMZN", "b"),
                    PrimitiveEventStructure("LOCM", "c")),
        SimpleCondition(Variable("a", lambda x: x["Opening Price"]),
                        Variable("b", lambda x: x["Opening Price"]),
                        Variable("c", lambda x: x["Opening Price"]),
                        relation_op=lambda x, y, z: x > y > z),
        timedelta(minutes=5)
    )
    return pattern

"""
class TestClass(unittest.TestCase):

    def create_optimizer(self):
        pattern = get_pattern_test()
        arrival_rates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
        selectivity_matrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                              [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
        pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES,
                               (selectivity_matrix, arrival_rates))

        opt_type = OptimizerTypes.USING_INVARIANT

        builder_type = TreePlanBuilderTypes.GREEDY_LEFT_DEEP_TREE
        tree_cost_models = TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL
        tree_plan_params = TreePlanBuilderParameters(builder_type, tree_cost_models)

        optimizer_parameters = InvariantsAwareOptimizerParameters(opt_type, tree_plan_params)

        optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)

        optimizer.is_need_reoptimize(pattern)
        
        self.assertIsInstance(optimizer, InvariantAwareOptimizer)


if __name__ == '__main__':
    unittest.main()
"""


def create_optimizer():
    pattern = get_pattern_test()
    arrival_rates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    selectivity_matrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                          [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]
    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES,
                           (selectivity_matrix, arrival_rates))

    opt_type = OptimizerTypes.USING_INVARIANT

    builder_type = TreePlanBuilderTypes.INVARIANT_AWARE_GREEDY_LEFT_DEEP_TREE
    tree_cost_models = TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL
    tree_plan_params = TreePlanBuilderParameters(builder_type, tree_cost_models)

    optimizer_parameters = InvariantsAwareOptimizerParameters(opt_type, tree_plan_params)

    optimizer = OptimizerFactory.build_optimizer(optimizer_parameters)

    tree_plan = optimizer.build_new_tree_plan(pattern)

    arrival_rates = [0.016597077244258872, 0.01454418928322895, 0.013917884481558803, 0.012421711899791231]
    selectivity_matrix = [[1.0, 0.9457796098355941, 1.0, 1.0], [0.9457796098355941, 1.0, 0.15989723367389616, 1.0],
                          [1.0, 0.15989723367389616, 1.0, 0.9992557393942864], [1.0, 1.0, 0.9992557393942864, 1.0]]

    pattern.set_statistics(StatisticsTypes.SELECTIVITY_MATRIX_AND_ARRIVAL_RATES, (selectivity_matrix, arrival_rates))
    is_need_new_build = optimizer.is_need_reoptimize(pattern)

