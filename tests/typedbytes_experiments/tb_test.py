import hadoopy
import unittest


class Test(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(Test, self).__init__(methodName)
        self.f = hadoopy.TypedBytesFile('gb_single.tb', 'r')

    def test(self):
        a = [('abcdefg', '01234'), (83, 12), (True, False), (13413, 164),
             (8589934592L, 17179869184L), (12.5, 15.25),
             (134223.123, .1232233), ('abcdefg', '01234'),
             ((1, 'a', True), (1, .25, ())), ([1, 'a', True], [1, .25, []]),
             ({'1': 3, 5: True}, {'1': 3, (3, 4): True})]
        self.assertEquals(list(self.f), a)


if __name__ == '__main__':
    unittest.main()
