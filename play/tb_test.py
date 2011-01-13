import hadoopy
import unittest


class Test(unittest.TestCase):

    def test00(self):
        self.assertEquals(hadoopy.read_tb(), ('abcdefg', '01234'))

    def test01(self):
        self.assertEquals(hadoopy.read_tb(), (83, 12))

    def test02(self):
        self.assertEquals(hadoopy.read_tb(), (True, False))

    def test03(self):
        self.assertEquals(hadoopy.read_tb(), (13413, 164))

    def test04(self):
        self.assertEquals(hadoopy.read_tb(), (8589934592L, 17179869184L))

    def test05(self):
        self.assertEquals(hadoopy.read_tb(), (12.5, 15.25))

    def test06(self):
        self.assertEquals(hadoopy.read_tb(), (134223.123, .1232233))

    def test07(self):
        self.assertEquals(hadoopy.read_tb(), ('abcdefg', '01234'))

    def test08(self):
        self.assertEquals(hadoopy.read_tb(), ((1, 'a', True), (1, .25, ())))

    def test09(self):
        self.assertEquals(hadoopy.read_tb(), ([1, 'a', True], [1, .25, []]))

    def test10(self):
        self.assertEquals(hadoopy.read_tb(), ({'1': 3, 5: True}, {'1': 3, (3, 4): True}))


if __name__ == '__main__':
    unittest.main()
