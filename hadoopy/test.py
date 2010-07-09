#!/usr/bin/env python
# (C) Copyright 2010 Brandyn A. White
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = 'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

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

    def shuffle_kv(self, kv):
        return self.groupby_kv(self.sort_kv(kv))

    def _call(self, attr):
        def call_func(func, test_input):
            out = []

            def out_func(out_iter):
                out.extend(out_iter)
            process_inout(func, test_input, out_func, attr)
            return out
        return call_func
