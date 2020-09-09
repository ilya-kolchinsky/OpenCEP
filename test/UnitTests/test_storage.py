from base.PatternMatch import PatternMatch
from tree.PatternMatchStorage import SortedPatternMatchStorage, UnsortedPatternMatchStorage, EquationSides
from datetime import datetime, timedelta
from base.Formula import RelopTypes


"""
Event for these tests only
"""


class Event:
    def __init__(self, payload, event_type, time):
        self.payload = payload
        self.event_type = event_type
        self.timestamp = time

    def __repr__(self):
        return "((type: {}), (payload: {}), (timestamp: {}))".format(
            self.event_type, self.payload, self.timestamp
        )


"""
"""


def run_storage_tests():
    unsorted_storage_test = TestUnsortedStorage()
    unsorted_storage_test.run_tests()
    sorted_storage_test = TestSortedStorage()
    sorted_storage_test.run_tests()
    print("PatternMatchStorage unit tests executed successfully.")


"""
UNSORTED STORAGE
"""


class TestUnsortedStorage:
    def __init__(self):
        self.dt = datetime(2020, 1, 1)
        self.pm1 = PatternMatch([Event(1, "type", self.dt)])
        self.pm2 = PatternMatch([Event(5, "type", self.dt + timedelta(4))])
        self.pm3 = PatternMatch([Event(17, "type", self.dt + timedelta(16))])
        self.pm4 = PatternMatch([Event(33, "type", self.dt + timedelta(32))])

    def test_add(self):
        u_s = UnsortedPatternMatchStorage(0)
        my_list = [7, 7, 8, 9, 1, 7, 3]

        for i in range(len(my_list)):
            u_s.add(my_list[i])

        for i in range(len(my_list)):
            assert u_s[i] == my_list[i], "UnsortedPatternMatchStorage: addition wasn't by order"

    def test_get(self):
        u_s = UnsortedPatternMatchStorage(0)
        my_list = [7, 7, 8, 9, 1, 7, 3]

        for i in range(len(my_list)):
            u_s.add(my_list[i])

        assert u_s.get("nothing") == my_list, "UnsortedPatternMatchStorage: getting values didn't return everything"

    def test_clean_expired_partial_matches(self):
        u_s = UnsortedPatternMatchStorage(0)
        u_s.add(self.pm1)
        u_s.add(self.pm2)
        u_s.add(self.pm3)
        u_s.add(self.pm4)
        u_s._clean_expired_partial_matches(self.dt + timedelta(15))
        assert u_s.get("nothing") == [self.pm3, self.pm4], "UnsortedPatternMatchStorage clean_expired_partial_matches failed"

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
            self.pm_list.append(PatternMatch([Event(i, "type", self.dt + timedelta(i * 10))]))

    def test_add(self):
        s = SortedPatternMatchStorage(lambda x: x.first_timestamp, "<", EquationSides.left, True)
        s.add(self.pm_list[1])
        s.add(self.pm_list[9])
        s.add(self.pm_list[0])
        for i in range(2, 9):
            s.add(self.pm_list[i])

        assert len(s) == 10, "SortedPatternMatchStorage: incorrect size"
        for i in range(10):
            assert s[i] == self.pm_list[i], "SortedPatternMatchStorage: incorrect order"

    def test_get(self):
        self.test_get_equal()
        self.test_get_unequal()
        self.test_get_greater()
        self.test_get_smaller()
        self.test_get_greater_or_equal()
        self.test_get_smaller_or_equal()

    def test_get_equal(self):
        s = SortedPatternMatchStorage(lambda x: x.first_timestamp, RelopTypes.Equal, EquationSides.left, True)
        for i in range(10):
            s.add(self.pm_list[i])
        for i in reversed(range(10)):
            s.add(self.pm_list[i])
        # 0,0,10,10,...70,70,...,90,90
        result_pms = s.get(self.dt + timedelta(70))
        # 70,70
        assert len(result_pms) == 2, "SortedPatternMatchStorage: get_equal returned incorrect number of pms"
        assert result_pms[0] == self.pm_list[7], "SortedPatternMatchStorage: get_equal returned incorrect pm[0]"
        assert result_pms[1] == self.pm_list[7], "SortedPatternMatchStorage: get_equal returned incorrect pm[1]"

    def test_get_unequal(self):
        s = SortedPatternMatchStorage(lambda x: x.first_timestamp, RelopTypes.NotEqual, EquationSides.left, True)
        for i in range(10):
            s.add(self.pm_list[i])
        for i in reversed(range(10)):
            s.add(self.pm_list[i])
        for i in range(10):
            s.add(self.pm_list[i])
        # 0,0,0,10,10,10,...90,90,90
        result_pms = s.get(self.dt)
        # expected 10,10,10,...,90,90,90
        assert len(result_pms) == 27, "SortedPatternMatchStorage: get_unequal returned incorrect number of pms"
        assert result_pms[0] == self.pm_list[1], "SortedPatternMatchStorage: get_unequal returned incorrect pm[0]"
        assert result_pms[3] == self.pm_list[2], "SortedPatternMatchStorage: get_unequal returned incorrect pm[3]"
        for i in range(27):
            assert result_pms[i] != self.pm_list[0], "SortedPatternMatchStorage: get_unequal returned incorrect pm[i]"

    def test_get_greater(self):
        s = SortedPatternMatchStorage(lambda x: x.first_timestamp, RelopTypes.Greater, EquationSides.left, True)
        for i in range(10):
            s.add(self.pm_list[i])
        for i in reversed(range(7)):
            s.add(self.pm_list[i])
        for i in range(5):
            s.add(self.pm_list[i])
        # (0,0,0,1,1,1,2,2,2,3,3,3,4,4,4,5,5,6,6,7,8,9 )*10
        result_pms = s.get(self.dt + timedelta(30))
        # (4,4,4,5,5,6,6,7,8,9 )*10
        assert len(result_pms) == 10, "SortedPatternMatchStorage: get_greater returned incorrect number of pms"
        for i in range(10):
            assert (
                result_pms[i].first_timestamp > self.pm_list[3].first_timestamp
            ), "SortedPatternMatchStorage: get_greater returned incorrect pm[i]"

    def test_get_smaller(self):
        s = SortedPatternMatchStorage(lambda x: x.first_timestamp, RelopTypes.Smaller, EquationSides.left, True)
        for i in range(1):
            s.add(self.pm_list[i])
        for i in reversed(range(8)):
            s.add(self.pm_list[i])
        for i in range(4):
            s.add(self.pm_list[i])
        # (0,0,0,1,1,2,2,3,3,4,5,6,7 )*10
        result_pms = s.get(self.dt + timedelta(50))
        # (0,0,0,1,1,2,2,3,3,4 )*10
        assert len(result_pms) == 10, "SortedPatternMatchStorage: get_smaller returned incorrect number of pms"
        for i in range(10):
            assert (
                result_pms[i].first_timestamp < self.pm_list[5].first_timestamp
            ), "SortedPatternMatchStorage: get_smaller returned incorrect pm[i]"

    def test_get_greater_or_equal(self):
        s = SortedPatternMatchStorage(lambda x: x.first_timestamp, RelopTypes.GreaterEqual, EquationSides.left, True)
        for i in range(8):
            s.add(self.pm_list[i])
        for i in reversed(range(2)):
            s.add(self.pm_list[i])
        for i in range(10):
            s.add(self.pm_list[i])
        # (0,0,0,1,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,9 )*10
        result_pms = s.get(self.dt + timedelta(80))
        # 80,90
        assert len(result_pms) == 2, "SortedPatternMatchStorage: get_greater_or_equal returned incorrect number of pms"
        assert (
            result_pms[0].first_timestamp == self.pm_list[8].first_timestamp
        ), "SortedPatternMatchStorage: get_greater_or_equal returned incorrect pm[0]"
        assert (
            result_pms[1].first_timestamp == self.pm_list[9].first_timestamp
        ), "SortedPatternMatchStorage: get_greater_or_equal returned incorrect pm[1]"

    def test_get_smaller_or_equal(self):
        s = SortedPatternMatchStorage(lambda x: x.first_timestamp, RelopTypes.SmallerEqual, EquationSides.left, True)
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
        assert len(result_pms) == 29, "SortedPatternMatchStorage: get_smaller_or_equal returned incorrect number of pms"
        assert (
            result_pms[0].first_timestamp == self.pm_list[0].first_timestamp
        ), "SortedPatternMatchStorage: get_smaller_or_equal returned incorrect pm[0]"
        assert (
            result_pms[12].first_timestamp == self.pm_list[1].first_timestamp
        ), "SortedPatternMatchStorage: get_smaller_or_equal returned incorrect pm[12]"
        assert (
            result_pms[28].first_timestamp == self.pm_list[2].first_timestamp
        ), "SortedPatternMatchStorage: get_smaller_or_equal returned incorrect pm[28]"
        for i in range(29):
            assert (
                result_pms[i].first_timestamp <= self.pm_list[2].first_timestamp
            ), "SortedPatternMatchStorage: get_smaller_or_equal returned incorrect pm[i]"

    def run_tests(self):
        self.test_add()
        self.test_get()
