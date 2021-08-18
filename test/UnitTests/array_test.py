import unittest
from misc.Utils import ndarray
import numpy as np


class MyTestCase(unittest.TestCase):
    def test_init(self):
        r1 = range(2 * 3 * 4 * 5)
        a = ndarray(r1)
        b = np.array(r1)
        self.assertArrayEqual(a, b)
        a = ndarray([])
        b = np.array([])
        self.assertArrayEqual(a, b)
        b = np.array([[]])
        a = ndarray([[]])
        self.assertArrayEqual(a, b)
        b = np.array([[], []])
        a = ndarray([[], []])
        self.assertArrayEqual(a, b)

    def test_reshape(self):
        r1 = range(2 * 3 * 4 * 5)
        a = ndarray(r1).reshape(2, 3, 4, 5)
        b = np.array(r1).reshape(2, 3, 4, 5)
        self.assertArrayEqual(a, b)
        a = a.reshape(-1)
        b = b.reshape(-1)
        self.assertArrayEqual(a, b)
        a = a.reshape(4, -1)
        b = b.reshape(4, -1)
        self.assertArrayEqual(a, b)
        a = a.reshape((4, 6, -1))
        b = b.reshape((4, 6, -1))
        self.assertArrayEqual(a, b)
        a = ndarray(range(10))
        b = np.array(range(10))
        self.assertArrayEqual(a.reshape(-1), b.reshape(-1))

    def test_getitem(self):
        r1 = range(2 * 3 * 4 * 5)
        a = ndarray(r1)
        b = np.array(r1)
        self.assertEqual(a[1], b[1])
        a = a.reshape(2, 3, 4, 5)
        b = b.reshape(2, 3, 4, 5)
        self.assertArrayEqual(a[1], b[1])
        self.assertArrayEqual(a[:, :, 2], b[:, :, 2])
        self.assertArrayEqual(a[0, 1, 2], b[0, 1, 2])
        indices = (slice(None), slice(0,1))
        self.assertArrayEqual(a[indices], b[indices])
        indices = (slice(None), slice(0, 1), slice(1, 3))
        self.assertArrayEqual(a[indices], b[indices])


    def assertArrayEqual(self, my_array: ndarray, numpy_array: np.array):
        self.assertEqual(my_array.shape, numpy_array.shape)
        self.assertEqual(my_array.size, numpy_array.size)
        self.assertEqual(my_array.ndim, numpy_array.ndim)
        if my_array.ndim <= 1:
            self.assertEqual(list(my_array), list(numpy_array))
        else:
            for my_layer, np_layer in zip(my_array, numpy_array):
                self.assertArrayEqual(my_layer, np_layer)


if __name__ == '__main__':
    unittest.main()
