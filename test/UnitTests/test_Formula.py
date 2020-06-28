import unittest
from base.Formula import *


class TestFormula(unittest.TestCase):
    def test_CreateTerm(self):
        term_5 = AtomicTerm(5)
        term_8 = AtomicTerm(8)

        term_id_x = IdentifierTerm("x", lambda x: x)
        term_id_y = IdentifierTerm("y", lambda x: x)

        term_8plus5 = PlusTerm(term_8, term_5)
        term_8div5 = DivTerm(term_8, term_5)
        term_x_mul_y = MulTerm(term_id_x, term_id_y)
        term_xy_plus_8 = PlusTerm(term_x_mul_y, term_8)
        term_x_minus_8 = MinusTerm(term_id_x, term_8)

        # simplifiable property assertions
        self.assertEqual(term_5.simplifiable, True)
        self.assertEqual(term_8plus5.simplifiable, True)
        self.assertEqual(term_id_x.simplifiable, True)
        self.assertEqual(term_8div5.simplifiable, False)
        self.assertEqual(term_x_mul_y.simplifiable, False)
        self.assertEqual(term_xy_plus_8.simplifiable, False)

        # evaluation assertions
        self.assertEqual(term_8plus5.eval(), 13)
        self.assertEqual(term_id_x.eval({"x": 1611}), 1611)

        # abstract terms property assertions
        self.assertEqual(term_5.abstract_terms[0]["is_id"], False)
        self.assertEqual(term_x_mul_y.abstract_terms[0]["is_id"], True)
        self.assertEqual(term_x_mul_y.abstract_terms[1]["is_id"], True)
        self.assertEqual(term_xy_plus_8.abstract_terms[0]["is_id"], True)  # x
        self.assertEqual(term_xy_plus_8.abstract_terms[1]["is_id"], True)  # y
        self.assertEqual(term_xy_plus_8.abstract_terms[2]["is_id"], False)  # 8
        self.assertEqual(term_x_minus_8.abstract_terms[1]["is_id"], False)  # 8
        self.assertEqual(term_x_minus_8.abstract_terms[1]["sign"], -1)  # -8

    def test_simplifyFormula(self):
        term_5 = AtomicTerm(5)
        term_8 = AtomicTerm(8)

        term_id_x = IdentifierTerm("x", lambda x: x)
        term_id_y = IdentifierTerm("y", lambda x: x)

        term_8_plus_5 = PlusTerm(term_8, term_5)
        term_8_div_5 = DivTerm(term_8, term_5)
        term_x_mul_y = MulTerm(term_id_x, term_id_y)
        term_x_mul_y_plus_8 = PlusTerm(term_x_mul_y, term_8)

        # should make new term/item in the abstart list and not use same one
        # 8 is negative according to one term and positive according to another.

        term_x_minus_8 = MinusTerm(term_id_x, term_8)
        term_x_plus_y = PlusTerm(term_id_x, term_id_y)
        formula_xy_eq_8 = EqFormula(term_x_mul_y, term_8)
        formula_xplusy_steq_8 = SmallerThanEqFormula(term_x_plus_y, term_8)

        #  lhs vars and rhs vars are already in lhs term and rhs term, returns self
        self.assertEqual(formula_xy_eq_8.simplify_formula({"x"}, {"y"}), None)
        self.assertEqual(
            formula_xy_eq_8.simplify_formula({"x", "y"}, {}).left_term, formula_xy_eq_8.left_term
        )

        self.assertNotEqual(formula_xplusy_steq_8.simplify_formula({"x"}, {"y"}), None)
        self.assertNotEqual(
            formula_xplusy_steq_8.simplify_formula({"x"}, {"y"}), formula_xplusy_steq_8
        )

        # always first abstract term in a simplified formula is an atom 0, keep in mind
        simplified = formula_xplusy_steq_8.simplify_formula({"x"}, {"y"})
        self.assertEqual(
            formula_xplusy_steq_8.eval({"x": 7, "y": 1}),
            simplified.eval({"x": 7, "y": 1}),
        )

    def test_simplifyANDFormula(self):
        term_5 = AtomicTerm(5)
        term_8 = AtomicTerm(8)

        term_id_x = IdentifierTerm("x", lambda x: x)
        term_id_y = IdentifierTerm("y", lambda x: x)

        formula_x_steq_8 = SmallerThanEqFormula(term_id_x, term_8)   
        formula_y_steq_5 = SmallerThanEqFormula(term_id_y, term_5)

        formula_x_steq_8_And_y_steq_5 = AndFormula(formula_x_steq_8, formula_y_steq_5)

        self.assertEqual(formula_x_steq_8_And_y_steq_5.seperatable_formulas , True)
        
        #setting priority for y to be higher than x, should simplify f2.
        simplified_Formula = formula_x_steq_8_And_y_steq_5.simplify_formula({"x","y"},{},{"x":1, "y":10})
        # formula to sort by should be f2 which is formula_y_steq_5 (with no change so we can assert equal)
        self.assertEqual(simplified_Formula.formula_to_sort_by.left_term , formula_y_steq_5.left_term)

        simplified_Formula = formula_x_steq_8_And_y_steq_5.simplify_formula({"x"},{"y"},{"x":1, "y":10})

        self.assertIsNotNone(simplified_Formula)

if __name__ == "__main__":
    unittest.main()
    