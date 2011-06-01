try:
    import unittest2 as unittest
except ImportError:
    import unittest
import hadoopy
import time
import numpy as np

class Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_name(self):
        fn = '_tests/writetb-%f' % time.time()
        data = [('1', 1), (1.3, np.array([1,2,3])), (True, {'1': 3})]
        hadoopy.writetb(fn, data)
        np.testing.assert_equal(list(hadoopy.readtb(fn)), data)

if __name__ == '__main__':
    unittest.main()
