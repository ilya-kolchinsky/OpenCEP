from evaluation.Storage import SortedStorage, UnsortedStorage
from evaluation.PartialMatch import PartialMatch
from collections.abc import Sequence, Iterable, Sized, Container
from datetime import time, datetime, timedelta


"""
Event for these tests only
"""


class Event:
    def __init__(self, payload, event_type, time):
        self.payload = payload
        self.event_type = event_type
        self.timestamp = time

    def __repr__(self):
        return "((event_type: {}), (payload: {}), (timestamp: {}))".format(
            self.event_type, self.payload, self.timestamp
        )


"""
"""


def run_storage_tests():
    unsorted_storage_test = TestUnsortedStorage()
    unsorted_storage_test.run_tests()
    sorted_storage_test = TestSortedStorage()
    sorted_storage_test.run_tests()


"""
UNSORTED STORAGE
"""


class TestUnsortedStorage:
    def __init__(self):
        self.dt = datetime(2020, 1, 1)
        self.pm1 = PartialMatch([Event(1, "type", self.dt)])
        self.pm2 = PartialMatch([Event(5, "type", self.dt + timedelta(4))])
        self.pm3 = PartialMatch([Event(17, "type", self.dt + timedelta(16))])
        self.pm4 = PartialMatch([Event(33, "type", self.dt + timedelta(32))])

    def test_add(self):
        u_s = UnsortedStorage()
        my_list = [7, 7, 8, 9, 1, 7, 3]

        for i in range(len(my_list)):
            u_s.add(my_list[i])

        for i in range(len(my_list)):
            assert u_s[i] == my_list[i], "UnsortedStorage: addition wasn't by order"

    def test_get(self):
        u_s = UnsortedStorage()
        my_list = [7, 7, 8, 9, 1, 7, 3]

        for i in range(len(my_list)):
            u_s.add(my_list[i])

        assert u_s.get("nothing") == my_list, "UnsortedStorage: getting values didn't return everything"

    def test_clean_expired_partial_matches(self):
        u_s = UnsortedStorage()
        u_s.add(self.pm1)
        u_s.add(self.pm2)
        u_s.add(self.pm3)
        u_s.add(self.pm4)
        u_s.clean_expired_partial_matches(self.dt + timedelta(15))
        assert u_s.get("nothing") == [self.pm3, self.pm4], "UnsortedStorage clean_expired_partial_matches failed"

    def run_tests(self):
        self.test_add()
        self.test_get()
        self.test_clean_expired_partial_matches()


"""
SORTED STORAGE
"""


class TestSortedStorage:
    def __init__(self):
        self.dt = datetime(2020, 1, 1)
        self.pm_list = []
        for i in range(10):
            self.pm_list.append(PartialMatch([Event(i, "type", self.dt + timedelta(i * 10))]))

    def test_add(self):
        s = SortedStorage(lambda x: x.first_timestamp, "<", "left", True)
        s.add(self.pm_list[1])
        s.add(self.pm_list[9])
        s.add(self.pm_list[0])
        for i in range(2, 9):
            s.add(self.pm_list[i])

        assert len(s) == 10, "SortedStorage: incorrect size"
        for i in range(10):
            assert s[i] == self.pm_list[i], "SortedStorage: incorrect order"

    def test_get(self):
        self.test_get_equal()
        self.test_get_unequal()
        self.test_get_greater()
        self.test_get_smaller()
        self.test_get_greater_or_equal()
        self.test_get_smaller_or_equal()

    def test_get_equal(self):
        s = SortedStorage(lambda x: x.first_timestamp, "==", "left", True)
        for i in range(10):
            s.add(self.pm_list[i])
        for i in reversed(range(10)):
            s.add(self.pm_list[i])
        # 0,0,10,10,...70,70,...,90,90
        result_pms = s.get(self.dt + timedelta(70))
        # 70,70
        assert len(result_pms) == 2, "SortedStorage: get_equal returned incorrect number of pms"
        assert result_pms[0] == self.pm_list[7], "SortedStorage: get_equal returned incorrect pm[0]"
        assert result_pms[1] == self.pm_list[7], "SortedStorage: get_equal returned incorrect pm[1]"

    def test_get_unequal(self):
        s = SortedStorage(lambda x: x.first_timestamp, "!=", "left", True)
        for i in range(10):
            s.add(self.pm_list[i])
        for i in reversed(range(10)):
            s.add(self.pm_list[i])
        for i in range(10):
            s.add(self.pm_list[i])
        # 0,0,0,10,10,10,...90,90,90
        result_pms = s.get(self.dt)
        # expected 10,10,10,...,90,90,90
        assert len(result_pms) == 27, "SortedStorage: get_unequal returned incorrect number of pms"
        assert result_pms[0] == self.pm_list[1], "SortedStorage: get_unequal returned incorrect pm[0]"
        assert result_pms[3] == self.pm_list[2], "SortedStorage: get_unequal returned incorrect pm[3]"
        for i in range(27):
            assert result_pms[i] != self.pm_list[0], "SortedStorage: get_unequal returned incorrect pm[i]"

    def test_get_greater(self):
        s = SortedStorage(lambda x: x.first_timestamp, ">", "left", True)
        for i in range(10):
            s.add(self.pm_list[i])
        for i in reversed(range(7)):
            s.add(self.pm_list[i])
        for i in range(5):
            s.add(self.pm_list[i])
        # (0,0,0,1,1,1,2,2,2,3,3,3,4,4,4,5,5,6,6,7,8,9 )*10
        result_pms = s.get(self.dt + timedelta(30))
        # (4,4,4,5,5,6,6,7,8,9 )*10
        assert len(result_pms) == 10, "SortedStorage: get_greater returned incorrect number of pms"
        for i in range(10):
            assert (
                result_pms[i].first_timestamp > self.pm_list[3].first_timestamp
            ), "SortedStorage: get_greater returned incorrect pm[i]"

    def test_get_smaller(self):
        s = SortedStorage(lambda x: x.first_timestamp, "<", "left", True)
        for i in range(1):
            s.add(self.pm_list[i])
        for i in reversed(range(8)):
            s.add(self.pm_list[i])
        for i in range(4):
            s.add(self.pm_list[i])
        # (0,0,0,1,1,2,2,3,3,4,5,6,7 )*10
        result_pms = s.get(self.dt + timedelta(50))
        # (0,0,0,1,1,2,2,3,3,4 )*10
        assert len(result_pms) == 10, "SortedStorage: get_smaller returned incorrect number of pms"
        for i in range(10):
            assert (
                result_pms[i].first_timestamp < self.pm_list[5].first_timestamp
            ), "SortedStorage: get_smaller returned incorrect pm[i]"

    def test_get_greater_or_equal(self):
        s = SortedStorage(lambda x: x.first_timestamp, ">=", "left", True)
        for i in range(8):
            s.add(self.pm_list[i])
        for i in reversed(range(2)):
            s.add(self.pm_list[i])
        for i in range(10):
            s.add(self.pm_list[i])
        # (0,0,0,1,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,9 )*10
        result_pms = s.get(self.dt + timedelta(80))
        # 80,90
        assert len(result_pms) == 2, "SortedStorage: get_greater_or_equal returned incorrect number of pms"
        assert (
            result_pms[0].first_timestamp == self.pm_list[8].first_timestamp
        ), "SortedStorage: get_greater_or_equal returned incorrect pm[0]"
        assert (
            result_pms[1].first_timestamp == self.pm_list[9].first_timestamp
        ), "SortedStorage: get_greater_or_equal returned incorrect pm[1]"

    def test_get_smaller_or_equal(self):
        s = SortedStorage(lambda x: x.first_timestamp, "<=", "left", True)
        for j in range(7):
            for i in reversed(range(2)):
                s.add(self.pm_list[i])

        for j in range(5):
            for i in range(10):
                s.add(self.pm_list[i])
        # 0 -> appears 12 times
        # 1 -> appears 12 times
        # 2,3,...,9 -> appears 5 times each
        result_pms = s.get(self.dt + timedelta(20))
        # 0 -> appears 12 times
        # 1 -> appears 12 times
        # 2 -> appears 5 times
        assert len(result_pms) == 29, "SortedStorage: get_smaller_or_equal returned incorrect number of pms"
        assert (
            result_pms[0].first_timestamp == self.pm_list[0].first_timestamp
        ), "SortedStorage: get_smaller_or_equal returned incorrect pm[0]"
        assert (
            result_pms[12].first_timestamp == self.pm_list[1].first_timestamp
        ), "SortedStorage: get_smaller_or_equal returned incorrect pm[12]"
        assert (
            result_pms[28].first_timestamp == self.pm_list[2].first_timestamp
        ), "SortedStorage: get_smaller_or_equal returned incorrect pm[28]"
        for i in range(29):
            assert (
                result_pms[i].first_timestamp <= self.pm_list[2].first_timestamp
            ), "SortedStorage: get_smaller_or_equal returned incorrect pm[i]"

    def run_tests(self):
        self.test_add()
        self.test_get()


"""
from misc.Utils import get_first_index, get_last_index


# Should receive an array that's size is more than 1 and it's values are [smallerorequal,,,,,biggerorequal]
# than the value we are looking for so arrays like this:
# [1,2,3,6,34] with val > 34 shouldn't be recieved
# [34,25,60,70] with val < 34 shouldn't be recieved



class TestGetFirstIndexUtils:
    def test_one_exists(self):
        container = [1]
        index = get_first_index(container, 1, lambda x: x)
        self.assertEqual(index, 0)

    def test_one_not_exist(self):
        container = [1]
        index = get_first_index(container, 2, lambda x: x)
        self.assertEqual(index, 1)

    def test_two_not_exist_smaller(self):
        container = [1, 2]
        index = get_first_index(container, 0, lambda x: x)
        self.assertEqual(index, -1)

    def test_two_not_exist_greater(self):
        container = [1, 2]
        index = get_first_index(container, 3, lambda x: x)
        self.assertEqual(index, 2)

    def test_two_not_rexist_greater(self):
        container = [1, 2]
        index = get_first_index(container, 1, lambda x: x)
        self.assertEqual(index, 0)

    def test_two_one_exist(self):
        container = [1, 1, 2]
        index = get_first_index(container, 1, lambda x: x)
        self.assertEqual(index, 0)

    def test_t5wo_one_exist(self):
        container = [1, 2]
        index = get_first_index(container, 2, lambda x: x)
        self.assertEqual(index, 1)

    def test_anything1(self):
        container = [1, 2, 3, 4, 5, 6, 7, 8, 8, 8, 8, 9, 10]
        index = get_first_index(container, 8, lambda x: x)
        self.assertEqual(index, 7)

    def test_anything2(self):
        container = [1, 2, 3, 4, 5, 6, 8, 8, 8, 8, 9, 10]
        index = get_first_index(container, 7, lambda x: x)
        self.assertEqual(index, 5)

    def run_tests(self):
        self.test_one_exists()
        self.test_one_not_exist()
        self.test_two_not_exist_smaller()
        self.test_two_not_exist_greater()
        self.test_two_not_rexist_greater()
        self.test_two_one_exist()
        self.test_t5wo_one_exist()
        self.test_anything1()
        self.test_anything2()


class TestGetLastIndexUtils:
    def test_one_exists(self):
        container = [1]
        index = get_last_index(container, 1, lambda x: x)
        self.assertEqual(index, 0)

    def test_one_not_exist(self):
        container = [1]
        index = get_last_index(container, 2, lambda x: x)
        self.assertEqual(index, 1)

    def test_two_not_exist_smaller(self):
        container = [1, 2]
        index = get_last_index(container, 0, lambda x: x)
        self.assertEqual(index, -1)

    def test_two_not_exist_greater(self):
        container = [1, 2]
        index = get_last_index(container, 3, lambda x: x)
        self.assertEqual(index, 2)

    def test_two_one_exist(self):
        container = [1, 2]
        index = get_last_index(container, 1, lambda x: x)
        self.assertEqual(index, 0)

    def test_t5wo_one_exist(self):
        container = [1, 2]
        index = get_last_index(container, 2, lambda x: x)
        self.assertEqual(index, 1)

    def test_anything1(self):
        container = [1, 2, 3, 4, 5, 6, 7, 8, 8, 8, 8, 9, 10]
        index = get_last_index(container, 8, lambda x: x)
        self.assertEqual(index, 10)

    def test_anything2(self):
        container = [6, 8]
        index = get_last_index(container, 7, lambda x: x)
        self.assertEqual(index, 1)

    def run_tests(self):
        self.test_one_exists()
        self.test_one_not_exist()
        self.test_two_not_exist_smaller()
        self.test_two_not_exist_greater()
        self.test_two_one_exist()
        self.test_t5wo_one_exist()
        self.test_anything1()
        self.test_anything2()
"""

# class TestConstruction:

#     def test_empty(self):
#         s = SortedStorage([])
#         print(s)

#     def test_from_sequence(self):
#         s = SortedStorage([7, 8, 3, 1])
#         print(s)

#     def test_from_iterable(self):
#         def gen6842():
#             yield 6
#             yield 8
#             yield 4
#             yield 2

#         g = gen6842()
#         s = SortedStorage(g)
#         print(s)

#     def test_default_empty(self):
#         s = SortedStorage()
#         print(s)

#     def run_tests(self):
#         self.test_empty()
#         self.test_from_sequence()
#         self.test_from_iterable()
#         self.test_default_empty()

"""
Protocols
"""
# class TestContainerProtocol:
#     def __init__(self):
#         self.s = SortedStorage([6, 7, 3, 9])

#     def test_positive_contained(self):
#         self.assertTrue(6 in self.s)

#     def test_negative_contained(self):
#         self.assertFalse(2 in self.s)

#     def test_positive_not_contained(self):
#         self.assertTrue(5 not in self.s)

#     def test_negative_not_contained(self):
#         self.assertFalse(9 not in self.s)

#     def test_protocol(self):
#         self.assertTrue(issubclass(SortedStorage, Container))

#     def run_tests():
#         self.test_positive_contained()
#         self.test_negative_contained()
#         self.test_positive_not_contained()
#         self.test_negative_not_contained()
#         self.test_protocol()

# class TestSizedProtocol(unittest.TestCase):
#     def test_empty(self):
#         s = SortedStorage()
#         self.assertEqual(len(s), 0)

#     def test_one(self):
#         s = SortedStorage([42])
#         self.assertEqual(len(s), 1)

#     def test_ten(self):
#         s = SortedStorage(range(10))
#         self.assertEqual(len(s), 10)

#     def test_with_duplications(self):
#         s = SortedStorage([5, 5, 5])
#         self.assertEqual(len(s), 3)

#     def test_protocol(self):
#         self.assertTrue(issubclass(SortedStorage, Sized))


# class TestIterableProtocol(unittest.TestCase):
#     def setUp(self):
#         self.s = SortedStorage([7, 2, 1, 1, 9])

#     def test_iter(self):
#         i = iter(self.s)
#         self.assertEqual(next(i), 7)
#         self.assertEqual(next(i), 2)
#         self.assertEqual(next(i), 1)
#         self.assertEqual(next(i), 1)
#         self.assertEqual(next(i), 9)
#         self.assertRaises(StopIteration, lambda: next(i))

#     def test_for_loop(self):
#         index = 0
#         expected = [7, 2, 1, 1, 9]
#         for item in self.s:
#             self.assertEqual(item, expected[index])
#             index += 1

#     def test_protocol(self):
#         self.assertTrue(issubclass(SortedStorage, Iterable))


# class TestSequenceProtocol(unittest.TestCase):
#     def setUp(self):
#         self.s = SortedStorage([1, 4, 9, 13, 15])

#     def test_index_zero(self):
#         self.assertEqual(self.s[0], 1)

#     def test_index_four(self):
#         self.assertEqual(self.s[4], 15)

#     def test_index_one_beyond_the_end(self):
#         with self.assertRaises(IndexError):
#             self.s[5]

#     def test_index_minus_one(self):
#         self.assertEqual(self.s[-1], 15)

#     def test_index_minus_five(self):
#         self.assertEqual(self.s[-5], 1)

#     def test_index_one_before_the_beginning(self):
#         with self.assertRaises(IndexError):
#             self.s[-6]

#     def test_slice_from_start(self):
#         self.assertEqual(self.s[:3], SortedStorage([1, 4, 9]))

#     def test_slice_to_end(self):
#         self.assertEqual(self.s[3:], SortedStorage([13, 15]))

#     def test_slice_empty(self):
#         self.assertEqual(self.s[10:], SortedStorage())

#     def test_slice_arbitrary(self):
#         self.assertEqual(self.s[2:4], SortedStorage([9, 13]))

#     def test_slice_full(self):
#         self.assertEqual(self.s[:], self.s)

#     def test_reversed(self):
#         s = SortedStorage([1, 3, 5, 7])
#         r = reversed(s)
#         self.assertEqual(next(r), 7)
#         self.assertEqual(next(r), 5)
#         self.assertEqual(next(r), 3)
#         self.assertEqual(next(r), 1)
#         with self.assertRaises(StopIteration):
#             next(r)

#     def test_index_positive(self):
#         s = SortedStorage([1, 5, 8, 9])
#         self.assertEqual(s.index(8), 2)

#     def test_index_negative(self):
#         s = SortedStorage([1, 5, 8, 9])
#         with self.assertRaises(ValueError):
#             s.index(15)

#     def test_count_zero(self):
#         s = SortedStorage([1, 5, 7, 9, 7, 8, 8, 8, 8])
#         self.assertEqual(s.count(-11), 0)

#     def test_count_four(self):
#         s = SortedStorage([8, 1, 5, 7, 9, 7, 8, 8, 8, 8])
#         self.assertEqual(s.count(8), 5)

#     def test_protocol(self):
#         self.assertTrue(issubclass(SortedStorage, Sequence))

#     def test_concatenate(self):
#         s = SortedStorage([2, 8, 4, 7, 0])
#         t = SortedStorage([1, 1, 1, 1, 1, -5, 6, -5, 0, 0, 0])
#         self.assertEqual(
#             s + t, SortedStorage([2, 8, 4, 7, 0, 1, 1, 1, 1, 1, -5, 6, -5, 0, 0, 0])
#         )


# class TestEqualityProtocol(unittest.TestCase):
#     def test_positive_equal(self):
#         self.assertTrue(SortedStorage([4, 5, 6]) == SortedStorage([4, 5, 6]))

#     def test_negative_equal(self):
#         self.assertFalse(SortedStorage([4, 5, 6]) == SortedStorage([1, 2, 3]))

#     def test_type_mismatch(self):
#         self.assertFalse(SortedStorage([4, 5, 6]) == [4, 5, 6])

#     def test_identical(self):
#         s = SortedStorage([10, 11, 12])
#         self.assertTrue(s == s)


# class TestInequalityProtocol(unittest.TestCase):
#     def test_positive_unequal(self):
#         self.assertTrue(SortedStorage([4, 5, 6]) != SortedStorage([1, 2, 3]))

#     def test_negative_unequal(self):
#         self.assertFalse(SortedStorage([4, 5, 6]) != SortedStorage([4, 5, 6]))

#     def test_type_mismatch(self):
#         self.assertTrue(SortedStorage([4, 5, 6]) != [4, 5, 6])

#     def test_identical(self):
#         s = SortedStorage([10, 11, 12])
#         self.assertFalse(s != s)
