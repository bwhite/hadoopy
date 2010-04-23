import unittest
from hadoopy.main import _configure_call_close

class Test(unittest.TestCase):
    
    def __init__(self, name='runTest'):
        super(Test, self).__init__(name)

    @staticmethod
    @_configure_call_close('map')
    def _call_map(func, test_input):
        return [func(key, value) for key, value in test_input]

    @staticmethod
    @_configure_call_close('reduce')
    def _call_reduce(func, test_input):
        return [func(key, value) for key, value in test_input]

    def call_map(self, func, test_input):
        self._call_map(func, test_input)

    def call_reduce(self, func, test_input):
        self._call_reduce(func, test_input)

        
