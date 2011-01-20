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
import tempfile


class Test(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(Test, self).__init__(*args, **kw)

    def test_int(self):
        f = tempfile.NamedTemporaryFile()
        kvs = [(0, 0), (-123, 123), (-1, 1), (-2**32, 2**32), (-2**64, 2**64),
               (-2**128, 2**128), (-2**256, 2**256)]
        for kv in kvs:
            with hadoopy.TypedBytesFile(f.name, 'w') as fp:
                fp.write(kv)
            self.assertEquals(hadoopy.TypedBytesFile(f.name, 'r').next(), kv)

if __name__ == '__main__':
    unittest.main()
