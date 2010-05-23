#!/usr/bin/env python
import unittest
import hadoopy


    
class TestFindHStream(hadoopy.Test):
    
    def __init__(self, *args, **kw):
        super(TestFindHStream, self).__init__(*args, **kw)

    def test_find(self):
        self.assertTrue(hadoopy.runner._find_hstreaming())


if __name__ == '__main__':
    unittest.main()
