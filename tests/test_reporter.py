import unittest
import hadoopy


class TestReporter(unittest.TestCase):

    def test_status(self):
        def err(x):
            self.assertEqual('reporter:status:[test]', x)
        hadoopy.status('[test]', err=err)

    def test_counter(self):
        def err(x):
            self.assertEqual('reporter:counter:a,b,5\n', x)
        hadoopy.counter('a', 'b', 5, err=err)


if __name__ == '__main__':
    unittest.main()
