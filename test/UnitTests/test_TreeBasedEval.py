import unittest
from evaluation.Storage import SortedStorage, UnsortedStorage
from collections.abc import Sequence, Iterable, Sized, Container
from datetime import time, datetime
from typing import List
from base.PatternStructure import SeqOperator, QItem, AndOperator
from base.Pattern import Pattern
from evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from test.UnitTests.prettyjson import prettyjson
from base.Event import Event
from misc.IOUtils import Stream
from base.Formula import AtomicTerm, IdentifierTerm, MinusTerm, PlusTerm, MulTerm, SmallerThanFormula


class TestConstruction(unittest.TestCase):
    def setUp(self):
        self.q1 = QItem("apple", "a")
        self.q2 = QItem("banana", "b")
        self.q3 = QItem("coco", "c")
        self.q4 = QItem("drum", "d")
        # m -> g -> f -> a
        # self.pattern = Pattern(SeqOperator([q1, q2, q3, q4]))
        # self.tree_structure = (0,1,2,3)

    def rtest_seq_empty(self):
        pattern = Pattern(SeqOperator([]))
        tree_structure = ()
        TreeBasedEvaluationMechanism(pattern, tree_structure)

    def rtest_seq_one_event(self):
        pattern = Pattern(SeqOperator([self.q1]))
        tree_structure = 0
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        print(prettyjson(tbem.json_repr()))

    def rtest_seq_two_events(self):
        pattern = Pattern(SeqOperator([self.q1, self.q2]))
        tree_structure = (0, 1)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        print(prettyjson(tbem.json_repr()))

    def rtest_seq_four_events(self):
        pattern = Pattern(SeqOperator([self.q1, self.q2, self.q3, self.q4]))
        tree_structure = (0, 1, 2, 3)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        print(prettyjson(tbem.json_repr()))
        print("END")

    def rtest_and_two_events(self):
        pattern = Pattern(AndOperator([self.q1, self.q2]))
        tree_structure = (0, 1)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        print(prettyjson(tbem.json_repr()))

    def rtest_and_four_events(self):
        pattern = Pattern(AndOperator([self.q1, self.q2, self.q3, self.q4]))
        tree_structure = (0, 1, 2, 3)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        print(prettyjson(tbem.json_repr()))

    def rtest_and_two_events_condition(self):
        two = AtomicTerm(2)
        a = IdentifierTerm("a", lambda x: x.price)  # gonna be using payload with attribute "price"
        b = IdentifierTerm("b", lambda x: x.price)
        aplusb = PlusTerm(a, b)
        cond = SmallerThanFormula(aplusb, two)
        pattern = Pattern(AndOperator([self.q1, self.q2]), cond)
        tree_structure = (0, 1)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        print(prettyjson(tbem.json_repr()))


class TestANDEvaluationWithoutStream(unittest.TestCase):
    def setUp(self):
        self.q1 = QItem("amicrosoft", "a")
        self.q2 = QItem("bgoogle", "b")
        self.q3 = QItem("cfacebook", "c")
        self.q4 = QItem("dapple", "d")
        two = AtomicTerm(2)
        a = IdentifierTerm("a", lambda x: x["price"])
        b = IdentifierTerm("b", lambda x: x["price"])
        b_plus_two = PlusTerm(b, two)
        self.condition = SmallerThanFormula(a, b_plus_two)
        # self.event2 = Event(pl,self.q2.event_type,time(second=20))
        # self.event3 = Event(pl,self.q3.event_type,time(second=122))
        # self.event4 = Event(pl,self.q4.event_type,time(second=190))

    def rtest_one_event_two_leaves(self):
        pattern = Pattern(AndOperator([self.q1, self.q2]))
        tree_structure = (0, 1)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        event1 = Event("payloadA", self.q1.event_type, time(second=10))
        events = [event1]
        matches = ["DUMMY"]
        tbem.eval(events, matches)
        for match in matches:
            print(match)

    def test_two_events_two_leaves_one_match(self):
        pattern = Pattern(AndOperator([self.q1, self.q2]), self.condition)
        tree_structure = (0, 1)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        """dictA = {
            price: 1000

        }"""
        event1 = Event({"price": 1000}, self.q1.event_type, time(second=10))
        event2 = Event({"price": 2000}, self.q2.event_type, time(second=11))
        events = [event1, event2]
        matches = ["DUMMY"]
        tbem.eval(events, matches)
        for match in matches:
            print(match)


"""
class TestSEQEvaluationWithoutStream(unittest.TestCase):
    def setUp(self):
        self.q1 = QItem("amicrosoft", "a")
        self.q2 = QItem("bgoogle", "b")
        self.q3 = QItem("cfacebook", "c")
        self.q4 = QItem("dapple", "d")

        # self.event2 = Event(pl,self.q2.event_type,time(second=20))
        # self.event3 = Event(pl,self.q3.event_type,time(second=122))
        # self.event4 = Event(pl,self.q4.event_type,time(second=190))

    def rtest_one_event_two_leaves(self):
        pattern = Pattern(SeqOperator([self.q1, self.q2]))
        tree_structure = (0, 1)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        event1 = Event("payloadA", self.q1.event_type, time(second=10))
        events = [event1]
        matches = ["DUMMY"]
        tbem.eval(events, matches)
        for match in matches:
            print(match)

    def rtest_two_events_two_leaves_one_match(self):
        print("%%%%%%%%SECOND_TEST%%%%%%%%%")
        pattern = Pattern(SeqOperator([self.q1, self.q2]))
        tree_structure = (0, 1)
        tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
        event1 = Event("payloadA", self.q1.event_type, time(second=10))
        event2 = Event("payloadB", self.q2.event_type, time(second=11))
        events = [event1, event2]
        matches = ["DUMMY"]
        tbem.eval(events, matches)
        for match in matches:
            print(match)

"""

# for job in iter(queue.get


if __name__ == "__main__":
    unittest.main()
    """ 
    q1 = QItem("microsoft", "m")
    pattern = Pattern(SeqOperator([q1]))
    tree_structure = 0
    tbem = TreeBasedEvaluationMechanism(pattern, tree_structure)
    print(prettyjson(tbem.json_repr()))
    print("END")
    """
