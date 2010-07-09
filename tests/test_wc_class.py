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

__author__ =  'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import unittest
import hadoopy

class Mapper(object):
    """Emit each term with a count of 1.

    Args:
        key: unused
        value: term

    Yields:
        A tuple in the form of (key, value)
        key: term as string
        value: count as int
    """
    def configure(self):
        self.imc = {}

    def map(self, key, value):
        try:
            self.imc[value] += 1
        except KeyError:
            self.imc[value] = 1

    def close(self):
        for k, v in self.imc.iteritems():
            yield k, v

class Reducer(object):
    """Sum up counts for each term.

    Args:
        key: term as string
        values: counts as int

    Yields:
        A tuple in the form of (key, value)
        key: term as string
        value: count as int
    """
    def reduce(self, key, values):
        yield key, sum(values)
    

class TestWordcount(hadoopy.Test):
    
    def __init__(self, *args, **kw):
        super(TestWordcount, self).__init__(*args, **kw)

    def test_wc(self):
        test_in = [(None, 'a'),
                   (None, 'b'),
                   (None, 'a'),
                   (None, 'c'),
                   (None, 'a')]
        test_map_out = [('a', 3),
                        ('c', 1),
                        ('b', 1)]
        test_reduce_out = [('a', 3),
                           ('b', 1),
                           ('c', 1)]
        self.assertEqual(self.call_map(Mapper, test_in), test_map_out)
        reduce_in = self.groupby_kv(self.sort_kv(test_map_out))
        self.assertEqual(self.call_reduce(Reducer, reduce_in), test_reduce_out)


if __name__ == '__main__':
    unittest.main()
