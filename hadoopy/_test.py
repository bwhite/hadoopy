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
import hadoopy
from operator import itemgetter
from itertools import groupby


class Test(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(Test, self).__init__(*args, **kw)

    @classmethod
    def sort_kv(cls, kv):
        """Perform a stable sort on KeyValue pair keys

        :param kv: Iterator of KeyValue pairs
        :returns: Grouped KeyValue pairs in sorted order
        """
        kv = list(kv)
        kv.sort(lambda x, y: cmp(x[0], y[0]))
        return kv

    @classmethod
    def groupby_kv(cls, kv):
        """Group sorted KeyValue pairs

        :param kv: Iterator of KeyValue pairs
        :returns: Grouped KeyValue pairs in sorted order
        """
        return ((x, (z[1] for z in y))
                for x, y in groupby(kv, itemgetter(0)))

    @classmethod
    def shuffle_kv(cls, kv):
        """Given KeyValue pairs, sort, then group

        :param kv: Iterator of KeyValue pairs
        :returns: Grouped KeyValue pairs in sorted order
        """
        return cls.groupby_kv(cls.sort_kv(kv))

    @classmethod
    def call_map(cls, func, test_input):
        """Given KeyValue pairs, sort, then group

        :param func: Mapper function or class
        :param test_input: Iterator of KeyValue pairs
        :returns: List of KeyValue pairs from the mapper
        """
        out = []

        def out_func(out_iter):
            out.extend(out_iter)
        hadoopy._main.HadoopyTask.process_inout(func, test_input,
                                                out_func, 'map')
        return out

    @classmethod
    def call_reduce(cls, func, test_input):
        """Given KeyValue pairs, sort, then group

        :param func: Reducer function or class
        :param test_input: Iterator of Grouped KeyValue pairs (e.g., from groupby_kv or shuffle_kv)
        :returns: List of KeyValue pairs from the reducer
        """
        out = []

        def out_func(out_iter):
            out.extend(out_iter)
        hadoopy._main.HadoopyTask.process_inout(func, test_input,
                                                out_func, 'reduce')
        return out
