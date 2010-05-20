import unittest
from hadoopy.main import process_inout, _groupby_kv

class Test(unittest.TestCase):
    
    def __init__(self, *args, **kw):
        super(Test, self).__init__(*args, **kw)
        self.call_map = self._call('map')
        self.call_reduce = self._call('reduce')
        self.groupby_kv = _groupby_kv
    
    def sort_kv(self, kv):
        kv = list(kv)
        kv.sort(lambda x, y: cmp(x[0], y[0]))
        return kv

    def _call(self, attr):
        def call_func(func, test_input):
            out = []
            def out_func(out_iter):
                out.extend(out_iter)
            process_inout(func, test_input, out_func, attr)
            return out
        return call_func
