import unittest
from hadoopy.main import _configure_call_close

class Test(unittest.TestCase):
    
    def __init__(self, *args, **kw):
        super(Test, self).__init__(*args, **kw)
        self.call_map = self._call('map')
        self.call_reduce = self._call('reduce')

    def _call(self, attr):
        def call_func(func, test_input):
            @_configure_call_close(attr)
            def _in(func):
                return sum([list(func(*x)) for x in test_input], [])
            return _in(func)
        return call_func
