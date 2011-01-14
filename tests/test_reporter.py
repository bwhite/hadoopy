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


class TestReporter(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(TestReporter, self).__init__(*args, **kw)

    def test_status(self):
        def err(x):
            self.assertEqual('reporter:status:[test]\n', x)
        hadoopy.status('[test]', err=err)

    def test_counter(self):
        def err(x):
            self.assertEqual('reporter:counter:a,b,5\n', x)
        hadoopy.counter('a', 'b', 5, err=err)


if __name__ == '__main__':
    unittest.main()
