import unittest
from evaluation.Storage import ArrayStorage
from collections.abc import Sequence, Iterable, Sized, Container
from datetime import time, datetime
from typing import List


"""class TestConstruction(unittest.TestCase):
    def test_empty(self):
        s = ArrayStorage([])
        print(s)

    def test_from_sequence(self):
        s = ArrayStorage([7, 8, 3, 1])
        print(s)

    def test_from_iterable(self):
        def gen6842():
            yield 6
            yield 8
            yield 4
            yield 2

        g = gen6842()
        s = ArrayStorage(g)
        print(s)

    def test_default_empty(self):
        s = ArrayStorage()
        print(s)"""


class TestContainerProtocol(unittest.TestCase):
    def setUp(self):
        self.s = ArrayStorage([6, 7, 3, 9])

    def test_positive_contained(self):
        self.assertTrue(6 in self.s)

    def test_negative_contained(self):
        self.assertFalse(2 in self.s)

    def test_positive_not_contained(self):
        self.assertTrue(5 not in self.s)

    def test_negative_not_contained(self):
        self.assertFalse(9 not in self.s)

    def test_protocol(self):
        self.assertTrue(issubclass(ArrayStorage, Container))


class TestSizedProtocol(unittest.TestCase):
    def test_empty(self):
        s = ArrayStorage()
        self.assertEqual(len(s), 0)

    def test_one(self):
        s = ArrayStorage([42])
        self.assertEqual(len(s), 1)

    def test_ten(self):
        s = ArrayStorage(range(10))
        self.assertEqual(len(s), 10)

    def test_with_duplications(self):
        s = ArrayStorage([5, 5, 5])
        self.assertEqual(len(s), 3)

    def test_protocol(self):
        self.assertTrue(issubclass(ArrayStorage, Sized))


class TestIterableProtocol(unittest.TestCase):
    def setUp(self):
        self.s = ArrayStorage([7, 2, 1, 1, 9])

    def test_iter(self):
        i = iter(self.s)
        self.assertEqual(next(i), 7)
        self.assertEqual(next(i), 2)
        self.assertEqual(next(i), 1)
        self.assertEqual(next(i), 1)
        self.assertEqual(next(i), 9)
        self.assertRaises(StopIteration, lambda: next(i))

    def test_for_loop(self):
        index = 0
        expected = [7, 2, 1, 1, 9]
        for item in self.s:
            self.assertEqual(item, expected[index])
            index += 1

    def test_protocol(self):
        self.assertTrue(issubclass(ArrayStorage, Iterable))


class TestSequenceProtocol(unittest.TestCase):
    def setUp(self):
        self.s = ArrayStorage([1, 4, 9, 13, 15])

    def test_index_zero(self):
        self.assertEqual(self.s[0], 1)

    def test_index_four(self):
        self.assertEqual(self.s[4], 15)

    def test_index_one_beyond_the_end(self):
        with self.assertRaises(IndexError):
            self.s[5]

    def test_index_minus_one(self):
        self.assertEqual(self.s[-1], 15)

    def test_index_minus_five(self):
        self.assertEqual(self.s[-5], 1)

    def test_index_one_before_the_beginning(self):
        with self.assertRaises(IndexError):
            self.s[-6]

    def test_slice_from_start(self):
        self.assertEqual(self.s[:3], ArrayStorage([1, 4, 9]))

    def test_slice_to_end(self):
        self.assertEqual(self.s[3:], ArrayStorage([13, 15]))

    def test_slice_empty(self):
        self.assertEqual(self.s[10:], ArrayStorage())

    def test_slice_arbitrary(self):
        self.assertEqual(self.s[2:4], ArrayStorage([9, 13]))

    def test_slice_full(self):
        self.assertEqual(self.s[:], self.s)

    def test_reversed(self):
        s = ArrayStorage([1, 3, 5, 7])
        r = reversed(s)
        self.assertEqual(next(r), 7)
        self.assertEqual(next(r), 5)
        self.assertEqual(next(r), 3)
        self.assertEqual(next(r), 1)
        with self.assertRaises(StopIteration):
            next(r)

    def test_index_positive(self):
        s = ArrayStorage([1, 5, 8, 9])
        self.assertEqual(s.index(8), 2)

    def test_index_negative(self):
        s = ArrayStorage([1, 5, 8, 9])
        with self.assertRaises(ValueError):
            s.index(15)

    def test_count_zero(self):
        s = ArrayStorage([1, 5, 7, 9, 7, 8, 8, 8, 8])
        self.assertEqual(s.count(-11), 0)

    def test_count_four(self):
        s = ArrayStorage([8, 1, 5, 7, 9, 7, 8, 8, 8, 8])
        self.assertEqual(s.count(8), 5)

    def test_protocol(self):
        self.assertTrue(issubclass(ArrayStorage, Sequence))

    """def test_concatenate(self):
        s = ArrayStorage([2, 8, 4, 7, 0])
        t = ArrayStorage([1, 1, 1, 1, 1, -5, 6, -5, 0, 0, 0])
        self.assertEqual(
            s + t, ArrayStorage([2, 8, 4, 7, 0, 1, 1, 1, 1, 1, -5, 6, -5, 0, 0, 0])
        )
    """


class TestEqualityProtocol(unittest.TestCase):
    def test_positive_equal(self):
        self.assertTrue(ArrayStorage([4, 5, 6]) == ArrayStorage([4, 5, 6]))

    def test_negative_equal(self):
        self.assertFalse(ArrayStorage([4, 5, 6]) == ArrayStorage([1, 2, 3]))

    def test_type_mismatch(self):
        self.assertFalse(ArrayStorage([4, 5, 6]) == [4, 5, 6])

    def test_identical(self):
        s = ArrayStorage([10, 11, 12])
        self.assertTrue(s == s)


class TestInequalityProtocol(unittest.TestCase):
    def test_positive_unequal(self):
        self.assertTrue(ArrayStorage([4, 5, 6]) != ArrayStorage([1, 2, 3]))

    def test_negative_unequal(self):
        self.assertFalse(ArrayStorage([4, 5, 6]) != ArrayStorage([4, 5, 6]))

    def test_type_mismatch(self):
        self.assertTrue(ArrayStorage([4, 5, 6]) != [4, 5, 6])

    def test_identical(self):
        s = ArrayStorage([10, 11, 12])
        self.assertFalse(s != s)


class Event:
    def __init__(self, timest: time):
        self.timestamp = timest


class PartialMatch:
    def __init__(self, events: List[Event]):
        self.events = events
        self.last_timestamp = max(events, key=lambda x: x.timestamp).timestamp
        self.first_timestamp = min(events, key=lambda x: x.timestamp).timestamp


class TestGetGreater(unittest.TestCase):
    def setUp(self):
        t1 = time(second=1)
        t2 = time(second=5)
        t3 = time(second=17)
        t4 = time(second=33)
        self.pm1 = PartialMatch([Event(t1)])
        self.pm2 = PartialMatch([Event(t2)])
        self.pm3 = PartialMatch([Event(t3)])
        self.pm4 = PartialMatch([Event(t4)])

    def test_timestamp_not_exist(self):
        s = ArrayStorage([self.pm1, self.pm2, self.pm3, self.pm4])
        t = time(second=22)
        s_result = s.get_greater(t)
        self.assertEqual(s_result, ArrayStorage([self.pm4]))

    def test_timestamp_exist(self):
        s = ArrayStorage([self.pm1, self.pm2, self.pm3, self.pm4])
        t = time(second=17)
        s_result = s.get_greater(t)
        self.assertEqual(s_result, s[2:])


class TestGetSmaller(unittest.TestCase):
    def setUp(self):
        t1 = time(second=1)
        t2 = time(second=5)
        t3 = time(second=17)
        t4 = time(second=33)
        self.pm1 = PartialMatch([Event(t1)])
        self.pm2 = PartialMatch([Event(t2)])
        self.pm3 = PartialMatch([Event(t3)])
        self.pm4 = PartialMatch([Event(t4)])

    def test_timestamp_not_exist(self):
        s = ArrayStorage([self.pm1, self.pm2, self.pm3, self.pm4])
        t = time(second=22)
        s_result = s.get_smaller(t)
        self.assertEqual(s_result, s[:3])

    def test_timestamp_exist(self):
        s = ArrayStorage([self.pm1, self.pm2, self.pm3, self.pm4])
        t = time(second=17)
        s_result = s.get_smaller(t)
        self.assertEqual(s_result, s[:2])


if __name__ == "__main__":
    unittest.main()

    """ def test_insert(self):
        def test_set_item(self):
        def test_del_item(self):
        def test_append():
    """
