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
import os


class Test(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(Test, self).__init__(*args, **kw)

    def test_int(self):
        read_write_fds = os.pipe()
        kvs = [(0, 0), (-123, 123), (-1, 1), (-2**32, 2**32), (-2**64, 2**64),
               (-2**128, 2**128), (-2**256, 2**256)]
        with hadoopy.TypedBytesFile(read_fd=read_write_fds[0]) as fp1:
            with hadoopy.TypedBytesFile(write_fd=read_write_fds[1]) as fp0:
                for num, kv in enumerate(kvs):
                    print(kv)
                    print('Setting')
                    fp0.write(kv)
                    fp0.flush()
                    self.assertEquals(fp1.next(), kv)
                # NOTE: If we read again here then it will block forever
            # Try to read one even when there are none left
            self.assertRaises(StopIteration, fp1.next)

if __name__ == '__main__':
    unittest.main()
